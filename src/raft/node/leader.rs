use super::super::{
    Envelope, Index, Message, ReadSequence, Request, RequestID, Response, Status,
    HEARTBEAT_INTERVAL,
};
use super::{Follower, Node, NodeID, RawNode, Role, Term, Ticks};
use crate::error::{Error, Result};

use log::{debug, info};
use std::collections::{HashMap, HashSet, VecDeque};

/// Peer replication progress.
#[derive(Clone, Debug, PartialEq)]
struct Progress {
    /// The next index to replicate to the peer.
    next: Index,
    /// The last index known to be replicated to the peer.
    last: Index,
    /// The last read sequence number confirmed by the peer.
    read_seq: ReadSequence,
}

/// A pending client write request.
#[derive(Clone, Debug, PartialEq)]
struct Write {
    /// The node which submitted the write.
    from: NodeID,
    /// The write request ID.
    id: RequestID,
}

/// A pending client read request.
#[derive(Clone, Debug, PartialEq)]
struct Read {
    /// The sequence number of this read.
    seq: ReadSequence,
    /// The node which submitted the read.
    from: NodeID,
    /// The read request ID.
    id: RequestID,
    /// The read command.
    command: Vec<u8>,
}

// A leader serves requests and replicates the log to followers.
#[derive(Clone, Debug, PartialEq)]
pub struct Leader {
    /// Peer replication progress.
    progress: HashMap<NodeID, Progress>,
    /// Keeps track of pending write requests, keyed by log index. These are
    /// added when the write is proposed and appended to the leader's log, and
    /// removed when the command is applied to the state machine, sending the
    /// command result to the waiting client.
    ///
    /// If the leader loses leadership, all pending write requests are aborted
    /// by returning Error::Abort.
    writes: HashMap<Index, Write>,
    /// Keeps track of pending read requests. To guarantee linearizability, read
    /// requests are assigned a sequence number and registered here when
    /// received, but only executed once a quorum of nodes have confirmed the
    /// current leader by responding to heartbeats with the sequence number.
    ///
    /// If we lose leadership before the command is processed, all pending read
    /// requests are aborted by returning Error::Abort.
    reads: VecDeque<Read>,
    /// The read sequence number used for the last read. Incremented for every
    /// read command, and reset when we lose leadership (thus only valid for
    /// this term).
    read_seq: ReadSequence,
    /// Number of ticks since last periodic heartbeat.
    since_heartbeat: Ticks,
}

impl Leader {
    /// Creates a new leader role.
    pub fn new(peers: HashSet<NodeID>, last_index: Index) -> Self {
        let next = last_index + 1;
        let progress =
            peers.into_iter().map(|p| (p, Progress { next, last: 0, read_seq: 0 })).collect();
        Self {
            progress,
            writes: HashMap::new(),
            reads: VecDeque::new(),
            read_seq: 0,
            since_heartbeat: 0,
        }
    }
}

impl Role for Leader {}

impl RawNode<Leader> {
    /// Asserts internal invariants.
    fn assert(&mut self) -> Result<()> {
        self.assert_node()?;

        assert_ne!(self.term, 0, "Leaders can't have term 0");
        debug_assert_eq!(Some(self.id), self.log.get_term()?.1, "Log vote does not match self");

        Ok(())
    }

    /// Transitions the leader into a follower. This can only happen if we
    /// discover a new term, so we become a leaderless follower. Subsequently
    /// stepping the received message may discover the leader, if there is one.
    pub(super) fn into_follower(mut self, term: Term) -> Result<RawNode<Follower>> {
        assert!(term >= self.term, "Term regression {} -> {}", self.term, term);
        assert!(term > self.term, "Can only become follower in later term");

        info!("Discovered new term {}", term);

        // Cancel in-flight requests.
        for write in std::mem::take(&mut self.role.writes).into_values() {
            self.send(
                write.from,
                Message::ClientResponse { id: write.id, response: Err(Error::Abort) },
            )?;
        }
        for read in std::mem::take(&mut self.role.reads).into_iter() {
            self.send(
                read.from,
                Message::ClientResponse { id: read.id, response: Err(Error::Abort) },
            )?;
        }

        self.term = term;
        self.log.set_term(term, None)?;
        Ok(self.into_role(Follower::new(None, None)))
    }

    /// Processes a message.
    pub fn step(mut self, msg: Envelope) -> Result<Node> {
        self.assert()?;
        self.assert_step(&msg);

        // Drop messages from past terms.
        if msg.term < self.term {
            debug!("Dropping message from past term ({:?})", msg);
            return Ok(self.into());
        }

        // If we receive a message for a future term, become a leaderless
        // follower in it and step the message. If the message is a Heartbeat or
        // AppendEntries from the leader, stepping it will follow the leader.
        if msg.term > self.term {
            return self.into_follower(msg.term)?.step(msg);
        }

        match msg.message {
            // There can't be two leaders in the same term.
            Message::Heartbeat { .. } | Message::Append { .. } => {
                panic!("Saw other leader {} in term {}", msg.from, msg.term);
            }

            // A follower received one of our heartbeats and confirms that we
            // are its leader. If its log is incomplete, append entries. If the
            // peer's read sequence number increased, process any pending reads.
            Message::HeartbeatResponse { last_index, last_term, read_seq } => {
                assert!(read_seq <= self.role.read_seq, "Future read sequence number");

                let progress = self.role.progress.get_mut(&msg.from).unwrap();
                if read_seq > progress.read_seq {
                    progress.read_seq = read_seq;
                    self.maybe_read()?;
                }

                if last_index < self.log.get_last_index().0
                    || !self.log.has(last_index, last_term)?
                {
                    self.send_log(msg.from)?;
                }
            }

            // A follower appended log entries we sent it. Record its progress
            // and attempt to commit new entries.
            Message::AppendResponse { reject: false, last_index, last_term } => {
                assert!(
                    last_index <= self.log.get_last_index().0,
                    "Follower accepted entries after last index"
                );
                assert!(
                    last_term <= self.log.get_last_index().1,
                    "Follower accepted entries after last term"
                );

                let progress = self.role.progress.get_mut(&msg.from).unwrap();
                if last_index > progress.last {
                    progress.last = last_index;
                    progress.next = last_index + 1;
                    self.maybe_commit_and_apply()?;
                }
            }

            // A follower rejected log entries we sent it, typically because it
            // does not have the base index in its log. Try to replicate from
            // the previous entry.
            //
            // This linear probing, as described in the Raft paper, can be very
            // slow with long divergent logs, but we keep it simple.
            //
            // TODO: make use of last_index and last_term here.
            Message::AppendResponse { reject: true, last_index: _, last_term: _ } => {
                self.role.progress.entry(msg.from).and_modify(|p| {
                    if p.next > 1 {
                        p.next -= 1
                    }
                });
                self.send_log(msg.from)?;
            }

            // A client submitted a read command. To ensure linearizability, we
            // must confirm that we are still the leader by sending a heartbeat
            // with the read's sequence number and wait for confirmation from a
            // quorum before executing the read.
            Message::ClientRequest { id, request: Request::Read(command) } => {
                self.role.read_seq += 1;
                self.role.reads.push_back(Read {
                    seq: self.role.read_seq,
                    from: msg.from,
                    id,
                    command,
                });
                if self.peers.is_empty() {
                    self.maybe_read()?;
                }
                self.heartbeat()?;
            }

            // A client submitted a write command. Propose it, and track it
            // until it's applied and the response is returned to the client.
            Message::ClientRequest { id, request: Request::Write(command) } => {
                let index = self.propose(Some(command))?;
                self.role.writes.insert(index, Write { from: msg.from, id: id.clone() });
                if self.peers.is_empty() {
                    self.maybe_commit_and_apply()?;
                }
            }

            Message::ClientRequest { id, request: Request::Status } => {
                let status = Status {
                    leader: self.id,
                    term: self.term,
                    last_index: self
                        .role
                        .progress
                        .iter()
                        .map(|(id, p)| (*id, p.last))
                        .chain(std::iter::once((self.id, self.log.get_last_index().0)))
                        .collect(),
                    commit_index: self.log.get_commit_index().0,
                    apply_index: self.state.get_applied_index(),
                    storage: self.log.status()?,
                };
                self.send(
                    msg.from,
                    Message::ClientResponse { id, response: Ok(Response::Status(status)) },
                )?;
            }

            // Votes can come in after we won the election, ignore them.
            Message::Campaign { .. } | Message::CampaignResponse { .. } => {}

            // Leaders never proxy client requests, so we don't expect to see
            // responses from other nodes.
            Message::ClientResponse { .. } => panic!("Unexpected message {:?}", msg),
        }

        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
        self.assert()?;

        self.role.since_heartbeat += 1;
        if self.role.since_heartbeat >= HEARTBEAT_INTERVAL {
            self.heartbeat()?;
            self.role.since_heartbeat = 0;
        }
        Ok(self.into())
    }

    /// Broadcasts a heartbeat to all peers.
    pub(super) fn heartbeat(&mut self) -> Result<()> {
        let (commit_index, commit_term) = self.log.get_commit_index();
        let read_seq = self.role.read_seq;
        self.broadcast(Message::Heartbeat { commit_index, commit_term, read_seq })?;
        // NB: We don't reset self.since_heartbeat here, because we want to send
        // periodic heartbeats regardless of any on-demand heartbeats.
        Ok(())
    }

    /// Proposes a command for consensus by appending it to our log and
    /// replicating it to peers. If successful, it will eventually be committed
    /// and applied to the state machine.
    pub(super) fn propose(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        let index = self.log.append(self.term, command)?;
        for peer in self.peers.clone() {
            self.send_log(peer)?;
        }
        Ok(index)
    }

    /// Commits any new log entries that have been replicated to a quorum, and
    /// applies them to the state machine.
    fn maybe_commit_and_apply(&mut self) -> Result<Index> {
        // Determine the new commit index.
        let quorum_index = self.quorum_value(
            self.role
                .progress
                .values()
                .map(|p| p.last)
                .chain(std::iter::once(self.log.get_last_index().0))
                .collect(),
        );

        // If the commit index doesn't advance, do nothing. We don't assert on
        // this, since the quorum value may regress e.g. following a restart or
        // leader change where followers are initialized with log index 0.
        let mut commit_index = self.log.get_commit_index().0;
        if quorum_index <= commit_index {
            return Ok(commit_index);
        }

        // We can only safely commit an entry from our own term (see figure 8 in
        // Raft paper).
        commit_index = match self.log.get(quorum_index)? {
            Some(entry) if entry.term == self.term => quorum_index,
            Some(_) => return Ok(commit_index),
            None => panic!("Commit index {} missing", quorum_index),
        };

        // Commit the new entries.
        self.log.commit(commit_index)?;

        // Apply entries and respond to client writers.
        Self::maybe_apply_with(&mut self.log, &mut self.state, |index, result| -> Result<()> {
            if let Some(write) = self.role.writes.remove(&index) {
                // TODO: use self.send() or something.
                self.node_tx.send(Envelope {
                    from: self.id,
                    to: write.from,
                    term: self.term,
                    message: Message::ClientResponse {
                        id: write.id,
                        response: result.map(Response::Write),
                    },
                })?;
            }
            Ok(())
        })?;

        Ok(commit_index)
    }

    /// Executes any pending read requests that are now ready after quorum
    /// confirmation of their sequence number.
    fn maybe_read(&mut self) -> Result<()> {
        if self.role.reads.is_empty() {
            return Ok(());
        }

        // Determine the maximum read sequence confirmed by quorum.
        let read_seq = self.quorum_value(
            self.role
                .progress
                .values()
                .map(|p| p.read_seq)
                .chain(std::iter::once(self.role.read_seq))
                .collect(),
        );

        // Execute the ready reads.
        while let Some(read) = self.role.reads.front() {
            if read.seq > read_seq {
                break;
            }
            let read = self.role.reads.pop_front().unwrap();
            let result = self.state.read(read.command);
            self.send(
                read.from,
                Message::ClientResponse { id: read.id, response: result.map(Response::Read) },
            )?;
        }

        Ok(())
    }

    /// Sends pending log entries to a peer.
    fn send_log(&mut self, peer: NodeID) -> Result<()> {
        let (base_index, base_term) = match self.role.progress.get(&peer) {
            Some(Progress { next, .. }) if *next > 1 => match self.log.get(next - 1)? {
                Some(entry) => (entry.index, entry.term),
                None => panic!("Missing base entry {}", next - 1),
            },
            Some(_) => (0, 0),
            None => panic!("Unknown peer {}", peer),
        };

        let entries = self.log.scan((base_index + 1)..)?.collect::<Result<Vec<_>>>()?;
        debug!("Replicating {} entries at base {} to {}", entries.len(), base_index, peer);
        self.send(peer, Message::Append { base_index, base_term, entries })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::state::tests::TestState;
    use super::super::super::{Entry, Log};
    use super::super::tests::{assert_messages, assert_node};
    use super::*;
    use crate::storage;
    use itertools::Itertools as _;
    use pretty_assertions::assert_eq;

    #[allow(clippy::type_complexity)]
    fn setup() -> Result<(RawNode<Leader>, crossbeam::channel::Receiver<Envelope>)> {
        let (node_tx, node_rx) = crossbeam::channel::unbounded();
        let peers = HashSet::from([2, 3, 4, 5]);
        let state = Box::new(TestState::new(0));
        let mut log = Log::new(storage::Memory::new(), false)?;
        log.append(1, Some(vec![0x01]))?;
        log.append(1, Some(vec![0x02]))?;
        log.append(2, Some(vec![0x03]))?;
        log.append(3, Some(vec![0x04]))?;
        log.append(3, Some(vec![0x05]))?;
        log.commit(2)?;
        log.set_term(3, Some(1))?;

        let node = RawNode {
            id: 1,
            peers: peers.clone(),
            term: 3,
            role: Leader::new(peers, log.get_last_index().0),
            log,
            state,
            node_tx,
        };
        Ok((node, node_rx))
    }

    #[test]
    // HeartbeatResponse with old log triggers replication.
    fn step_confirmleader_replicate() -> Result<()> {
        let (leader, mut node_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::HeartbeatResponse { last_index: 3, last_term: 2, read_seq: 0 },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::Append { base_index: 5, base_term: 3, entries: vec![] },
            }],
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "Saw other leader 2 in term 3")]
    // Heartbeats from other leaders in current term panics.
    fn step_heartbeat_current_term() {
        let (leader, _) = setup().unwrap();
        leader
            .step(Envelope {
                from: 2,
                to: 1,
                term: 3,
                message: Message::Heartbeat { commit_index: 5, commit_term: 3, read_seq: 7 },
            })
            .unwrap();
    }

    #[test]
    // Heartbeats from other leaders in future term converts to follower and steps.
    fn step_heartbeat_future_term() -> Result<()> {
        let (leader, mut node_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Envelope {
            from: 2,
            to: 1,
            term: 4,
            message: Message::Heartbeat { commit_index: 7, commit_term: 4, read_seq: 7 },
        })?;
        assert_node(&mut node).is_follower().term(4).leader(Some(2)).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 2,
                term: 4,
                message: Message::HeartbeatResponse { last_index: 5, last_term: 3, read_seq: 7 },
            }],
        );
        Ok(())
    }

    #[test]
    // Heartbeats from other leaders in past terms are ignored.
    fn step_heartbeat_past_term() -> Result<()> {
        let (leader, mut node_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Envelope {
            from: 2,
            to: 1,
            term: 2,
            message: Message::Heartbeat { commit_index: 3, commit_term: 2, read_seq: 7 },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2);
        assert_messages(&mut node_rx, vec![]);
        Ok(())
    }

    #[test]
    fn step_acceptentries() -> Result<()> {
        let (leader, mut node_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::AppendResponse { reject: false, last_index: 4, last_term: 3 },
        })?;
        assert_node(&mut node).committed(2);
        assert_messages(&mut node_rx, vec![]);

        node = node.step(Envelope {
            from: 3,
            to: 1,
            term: 3,
            message: Message::AppendResponse { reject: false, last_index: 5, last_term: 3 },
        })?;
        assert_node(&mut node).committed(4).applied(4);
        assert_messages(&mut node_rx, vec![]);

        node = node.step(Envelope {
            from: 4,
            to: 1,
            term: 3,
            message: Message::AppendResponse { reject: false, last_index: 5, last_term: 3 },
        })?;
        assert_node(&mut node).committed(5).applied(5);
        assert_messages(&mut node_rx, vec![]);

        assert_node(&mut node).is_leader().term(3);
        Ok(())
    }

    #[test]
    // Duplicate AcceptEntries from single node should not trigger commit.
    fn step_acceptentries_duplicate() -> Result<()> {
        let (leader, mut node_rx) = setup()?;
        let mut node: Node = leader.into();

        for _ in 0..5 {
            node = node.step(Envelope {
                from: 2,
                to: 1,
                term: 3,
                message: Message::AppendResponse { reject: false, last_index: 5, last_term: 3 },
            })?;
            assert_node(&mut node).is_leader().term(3).committed(2);
            assert_messages(&mut node_rx, vec![]);
        }
        Ok(())
    }

    #[test]
    // AcceptEntries quorum for entry in past term should not trigger commit
    fn step_acceptentries_past_term() -> Result<()> {
        let (leader, mut node_rx) = setup()?;
        let peers = leader.peers.clone();
        let mut node: Node = leader.into();

        for peer in peers.into_iter() {
            node = node.step(Envelope {
                from: peer,
                to: 1,
                term: 3,
                message: Message::AppendResponse { reject: false, last_index: 3, last_term: 3 },
            })?;
            assert_node(&mut node).is_leader().term(3).committed(2);
            assert_messages(&mut node_rx, vec![]);
        }
        Ok(())
    }

    #[test]
    #[should_panic(expected = "Follower accepted entries after last index")]
    // AcceptEntries panics if last_index is beyond leader's log.
    fn step_acceptentries_future_index() {
        let (leader, _) = setup().unwrap();
        leader
            .step(Envelope {
                from: 2,
                to: 1,
                term: 3,
                message: Message::AppendResponse { reject: false, last_index: 7, last_term: 3 },
            })
            .unwrap();
    }

    #[test]
    fn step_rejectentries() -> Result<()> {
        let (mut leader, mut node_rx) = setup()?;
        let entries = leader.log.scan(0..)?.collect::<Result<Vec<_>>>()?;
        let mut node: Node = leader.into();

        for i in 0..(entries.len() + 3) {
            node = node.step(Envelope {
                from: 2,
                to: 1,
                term: 3,
                message: Message::AppendResponse { reject: true, last_index: 0, last_term: 3 },
            })?;
            assert_node(&mut node).is_leader().term(3).committed(2);
            let index = if i >= entries.len() { 0 } else { entries.len() - i - 1 };
            let replicate = entries.get(index..).unwrap().to_vec();
            assert_messages(
                &mut node_rx,
                vec![Envelope {
                    from: 1,
                    to: 2,
                    term: 3,
                    message: Message::Append {
                        base_index: index as Index,
                        base_term: if index > 0 {
                            entries.get(index - 1).map(|e| e.term).unwrap()
                        } else {
                            0
                        },
                        entries: replicate,
                    },
                }],
            );
        }
        Ok(())
    }

    #[test]
    // Sending a client query request will pass it to the state machine and trigger heartbeats.
    fn step_clientrequest_query() -> Result<()> {
        let (leader, node_rx) = setup()?;
        let peers = leader.peers.clone();
        let mut node: Node = leader.into();
        node = node.step(Envelope {
            from: 1,
            to: 1,
            term: 3,
            message: Message::ClientRequest { id: vec![0x01], request: Request::Read(vec![0xaf]) },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2).last(5);
        for to in peers.iter().copied().sorted() {
            assert_eq!(
                node_rx.try_recv()?,
                Envelope {
                    from: 1,
                    to,
                    term: 3,
                    message: Message::Heartbeat { commit_index: 2, commit_term: 1, read_seq: 1 },
                },
            );
        }
        Ok(())
    }

    #[test]
    // Sending a mutate request should append it to log, replicate it to peers, and register notification.
    fn step_clientrequest_mutate() -> Result<()> {
        let (leader, node_rx) = setup()?;
        let peers = leader.peers.clone();
        let mut node: Node = leader.into();

        node = node.step(Envelope {
            from: 1,
            to: 1,
            term: 3,
            message: Message::ClientRequest { id: vec![0x01], request: Request::Write(vec![0xaf]) },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2).last(6).entry(Entry {
            index: 6,
            term: 3,
            command: Some(vec![0xaf]),
        });

        for peer in peers.iter().cloned() {
            assert_eq!(
                node_rx.try_recv()?,
                Envelope {
                    from: 1,
                    to: peer,
                    term: 3,
                    message: Message::Append {
                        base_index: 5,
                        base_term: 3,
                        entries: vec![Entry { index: 6, term: 3, command: Some(vec![0xaf]) },]
                    },
                }
            )
        }
        Ok(())
    }

    #[test]
    // Sending a status request should pass it on to state machine, to add status.
    fn step_clientrequest_status() -> Result<()> {
        let (leader, mut node_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Envelope {
            from: 1,
            to: 1,
            term: 3,
            message: Message::ClientRequest { id: vec![0x01], request: Request::Status },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2).last(5);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                term: 3,
                from: 1,
                to: 1,
                message: Message::ClientResponse {
                    id: vec![1],
                    response: Ok(Response::Status(Status {
                        leader: 1,
                        term: 3,
                        last_index: HashMap::from([(1, 5), (2, 0), (3, 0), (4, 0), (5, 0)]),
                        commit_index: 2,
                        apply_index: 0,
                        storage: storage::engine::Status {
                            name: "memory".to_string(),
                            keys: 7,
                            size: 72,
                            total_disk_size: 0,
                            live_disk_size: 0,
                            garbage_disk_size: 0,
                        },
                    })),
                },
            }],
        );

        Ok(())
    }

    #[test]
    fn tick() -> Result<()> {
        let (leader, mut node_rx) = setup()?;
        let peers = leader.peers.clone();
        let mut node: Node = leader.into();
        for _ in 0..5 {
            for _ in 0..HEARTBEAT_INTERVAL {
                assert_messages(&mut node_rx, vec![]);
                node = node.tick()?;
                assert_node(&mut node).is_leader().term(3).committed(2);
            }

            for to in peers.iter().copied().sorted() {
                assert_eq!(
                    node_rx.try_recv()?,
                    Envelope {
                        from: 1,
                        to,
                        term: 3,
                        message: Message::Heartbeat {
                            commit_index: 2,
                            commit_term: 1,
                            read_seq: 0
                        },
                    }
                );
            }
        }
        Ok(())
    }
}
