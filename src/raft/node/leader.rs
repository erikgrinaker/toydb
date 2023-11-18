use super::super::{Address, Event, Index, Instruction, Message, Request, Response, Status};
use super::{Follower, Node, NodeID, RoleNode, Term, HEARTBEAT_INTERVAL};
use crate::error::{Error, Result};

use ::log::{debug, info, warn};
use std::collections::HashMap;

// A leader serves requests and replicates the log to followers.
#[derive(Debug)]
pub struct Leader {
    /// Number of ticks since last heartbeat.
    heartbeat_ticks: u64,
    /// The next index to replicate to a peer.
    peer_next_index: HashMap<NodeID, Index>,
    /// The last index known to be replicated on a peer.
    peer_last_index: HashMap<NodeID, Index>,
}

impl Leader {
    /// Creates a new leader role.
    pub fn new(peers: Vec<NodeID>, last_index: Index) -> Self {
        let mut leader = Self {
            heartbeat_ticks: 0,
            peer_next_index: HashMap::new(),
            peer_last_index: HashMap::new(),
        };
        for peer in peers {
            leader.peer_next_index.insert(peer, last_index + 1);
            leader.peer_last_index.insert(peer, 0);
        }
        leader
    }
}

impl RoleNode<Leader> {
    /// Transforms the leader into a follower
    fn become_follower(mut self, term: Term, leader: NodeID) -> Result<RoleNode<Follower>> {
        info!("Discovered new leader {} for term {}, following", leader, term);
        self.term = term;
        self.log.set_term(term, None)?;
        self.state_tx.send(Instruction::Abort)?;
        self.become_role(Follower::new(Some(leader), None))
    }

    /// Appends an entry to the log and replicates it to peers.
    pub fn append(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        let index = self.log.append(self.term, command)?;
        for peer in self.peers.clone() {
            self.replicate(peer)?;
        }
        Ok(index)
    }

    /// Commits any pending log entries.
    fn commit(&mut self) -> Result<Index> {
        let mut last_indexes = vec![self.log.get_last_index().0];
        last_indexes.extend(self.role.peer_last_index.values());
        last_indexes.sort_unstable();
        last_indexes.reverse();
        let quorum_index = last_indexes[self.quorum() as usize - 1];

        // We can only safely commit up to an entry from our own term, see figure 8 in Raft paper.
        let (commit_index, _) = self.log.get_commit_index();
        if quorum_index > commit_index {
            if let Some(entry) = self.log.get(quorum_index)? {
                if entry.term == self.term {
                    self.log.commit(quorum_index)?;
                    let mut scan = self.log.scan((commit_index + 1)..=quorum_index)?;
                    while let Some(entry) = scan.next().transpose()? {
                        self.state_tx.send(Instruction::Apply { entry })?;
                    }
                }
            }
        }
        Ok(quorum_index)
    }

    /// Replicates the log to a peer.
    fn replicate(&mut self, peer: NodeID) -> Result<()> {
        let peer_next = self
            .role
            .peer_next_index
            .get(&peer)
            .cloned()
            .ok_or_else(|| Error::Internal(format!("Unknown peer {}", peer)))?;
        let base_index = if peer_next > 0 { peer_next - 1 } else { 0 };
        let base_term = match self.log.get(base_index)? {
            Some(base) => base.term,
            None if base_index == 0 => 0,
            None => return Err(Error::Internal(format!("Missing base entry {}", base_index))),
        };
        let entries = self.log.scan(peer_next..)?.collect::<Result<Vec<_>>>()?;
        debug!("Replicating {} entries at base {} to {}", entries.len(), base_index, peer);
        self.send(Address::Peer(peer), Event::ReplicateEntries { base_index, base_term, entries })?;
        Ok(())
    }

    /// Processes a message.
    pub fn step(mut self, msg: Message) -> Result<Node> {
        if let Err(err) = self.validate(&msg) {
            warn!("Ignoring invalid message: {}", err);
            return Ok(self.into());
        }
        if msg.term > self.term {
            if let Address::Peer(from) = msg.from {
                return self.become_follower(msg.term, from)?.step(msg);
            }
        }

        match msg.event {
            Event::ConfirmLeader { commit_index, has_committed } => {
                if let Address::Peer(from) = msg.from {
                    self.state_tx.send(Instruction::Vote {
                        term: msg.term,
                        index: commit_index,
                        address: msg.from,
                    })?;
                    if !has_committed {
                        self.replicate(from)?;
                    }
                }
            }

            Event::AcceptEntries { last_index } => {
                if let Address::Peer(from) = msg.from {
                    self.role.peer_last_index.insert(from, last_index);
                    self.role.peer_next_index.insert(from, last_index + 1);
                }
                self.commit()?;
            }

            Event::RejectEntries => {
                if let Address::Peer(from) = msg.from {
                    self.role.peer_next_index.entry(from).and_modify(|i| {
                        if *i > 1 {
                            *i -= 1
                        }
                    });
                    self.replicate(from)?;
                }
            }

            Event::ClientRequest { id, request: Request::Query(command) } => {
                let (commit_index, commit_term) = self.log.get_commit_index();
                self.state_tx.send(Instruction::Query {
                    id,
                    address: msg.from,
                    command,
                    term: self.term,
                    index: commit_index,
                    quorum: self.quorum(),
                })?;
                self.state_tx.send(Instruction::Vote {
                    term: self.term,
                    index: commit_index,
                    address: Address::Local,
                })?;
                if !self.peers.is_empty() {
                    self.send(Address::Broadcast, Event::Heartbeat { commit_index, commit_term })?;
                }
            }

            Event::ClientRequest { id, request: Request::Mutate(command) } => {
                let index = self.append(Some(command))?;
                self.state_tx.send(Instruction::Notify { id, address: msg.from, index })?;
                if self.peers.is_empty() {
                    self.commit()?;
                }
            }

            Event::ClientRequest { id, request: Request::Status } => {
                let engine_status = self.log.status()?;
                let mut status = Box::new(Status {
                    server: self.id,
                    leader: self.id,
                    term: self.term,
                    node_last_index: self.role.peer_last_index.clone(),
                    commit_index: self.log.get_commit_index().0,
                    apply_index: 0,
                    storage: engine_status.name.clone(),
                    storage_size: engine_status.size,
                });
                status.node_last_index.insert(self.id, self.log.get_last_index().0);
                self.state_tx.send(Instruction::Status { id, address: msg.from, status })?
            }

            Event::ClientResponse { id, mut response } => {
                if let Ok(Response::Status(ref mut status)) = response {
                    status.server = self.id;
                }
                self.send(Address::Client, Event::ClientResponse { id, response })?;
            }

            // We ignore these messages, since they are typically additional votes from the previous
            // election that we won after a quorum.
            Event::SolicitVote { .. } | Event::GrantVote => {}

            Event::Heartbeat { .. } | Event::ReplicateEntries { .. } => {
                warn!("Received unexpected message {:?}", msg)
            }
        }

        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
        if !self.peers.is_empty() {
            self.role.heartbeat_ticks += 1;
            if self.role.heartbeat_ticks >= HEARTBEAT_INTERVAL {
                self.role.heartbeat_ticks = 0;
                let (commit_index, commit_term) = self.log.get_commit_index();
                self.send(Address::Broadcast, Event::Heartbeat { commit_index, commit_term })?;
            }
        }
        Ok(self.into())
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::{Entry, Log};
    use super::super::tests::{assert_messages, assert_node};
    use super::*;
    use crate::storage;
    use pretty_assertions::assert_eq;
    use tokio::sync::mpsc;

    #[allow(clippy::type_complexity)]
    fn setup() -> Result<(
        RoleNode<Leader>,
        mpsc::UnboundedReceiver<Message>,
        mpsc::UnboundedReceiver<Instruction>,
    )> {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let peers = vec![2, 3, 4, 5];
        let mut log = Log::new(Box::new(storage::engine::Memory::new()), false)?;
        log.append(1, Some(vec![0x01]))?;
        log.append(1, Some(vec![0x02]))?;
        log.append(2, Some(vec![0x03]))?;
        log.append(3, Some(vec![0x04]))?;
        log.append(3, Some(vec![0x05]))?;
        log.commit(2)?;
        log.set_term(3, None)?;

        let node = RoleNode {
            id: 1,
            peers: peers.clone(),
            term: 3,
            role: Leader::new(peers, log.get_last_index().0),
            log,
            node_tx,
            state_tx,
            proxied_reqs: HashMap::new(),
            queued_reqs: Vec::new(),
        };
        Ok((node, node_rx, state_rx))
    }

    #[test]
    // ConfirmLeader triggers vote
    fn step_confirmleader_vote() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer(2),
            to: Address::Peer(1),
            term: 3,
            event: Event::ConfirmLeader { commit_index: 2, has_committed: true },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(
            &mut state_rx,
            vec![Instruction::Vote { term: 3, index: 2, address: Address::Peer(2) }],
        );
        Ok(())
    }

    #[test]
    // ConfirmLeader without has_committed triggers replication
    fn step_confirmleader_replicate() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer(2),
            to: Address::Peer(1),
            term: 3,
            event: Event::ConfirmLeader { commit_index: 2, has_committed: false },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer(2),
                term: 3,
                event: Event::ReplicateEntries { base_index: 5, base_term: 3, entries: vec![] },
            }],
        );
        assert_messages(
            &mut state_rx,
            vec![Instruction::Vote { term: 3, index: 2, address: Address::Peer(2) }],
        );
        Ok(())
    }

    #[test]
    // Heartbeats from other leaders in current term are ignored.
    fn step_heartbeat_current_term() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer(2),
            to: Address::Peer(1),
            term: 3,
            event: Event::Heartbeat { commit_index: 5, commit_term: 3 },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // Heartbeats from other leaders in future term converts to follower and steps.
    fn step_heartbeat_future_term() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer(2),
            to: Address::Peer(1),
            term: 4,
            event: Event::Heartbeat { commit_index: 7, commit_term: 4 },
        })?;
        assert_node(&mut node).is_follower().term(4).leader(Some(2)).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer(2),
                term: 4,
                event: Event::ConfirmLeader { commit_index: 7, has_committed: false },
            }],
        );
        assert_messages(&mut state_rx, vec![Instruction::Abort]);
        Ok(())
    }

    #[test]
    // Heartbeats from other leaders in past terms are ignored.
    fn step_heartbeat_past_term() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer(2),
            to: Address::Peer(1),
            term: 2,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    fn step_acceptentries() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer(2),
            to: Address::Peer(1),
            term: 3,
            event: Event::AcceptEntries { last_index: 4 },
        })?;
        assert_node(&mut node).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);

        node = node.step(Message {
            from: Address::Peer(3),
            to: Address::Peer(1),
            term: 3,
            event: Event::AcceptEntries { last_index: 5 },
        })?;
        assert_node(&mut node).committed(4);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(
            &mut state_rx,
            vec![
                Instruction::Apply {
                    entry: Entry { index: 3, term: 2, command: Some(vec![0x03]) },
                },
                Instruction::Apply {
                    entry: Entry { index: 4, term: 3, command: Some(vec![0x04]) },
                },
            ],
        );

        node = node.step(Message {
            from: Address::Peer(4),
            to: Address::Peer(1),
            term: 3,
            event: Event::AcceptEntries { last_index: 5 },
        })?;
        assert_node(&mut node).committed(5);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(
            &mut state_rx,
            vec![Instruction::Apply {
                entry: Entry { index: 5, term: 3, command: Some(vec![0x05]) },
            }],
        );

        assert_node(&mut node).is_leader().term(3);
        Ok(())
    }

    #[test]
    // Duplicate AcceptEntries from single node should not trigger commit.
    fn step_acceptentries_duplicate() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        for _ in 0..5 {
            node = node.step(Message {
                from: Address::Peer(2),
                to: Address::Peer(1),
                term: 3,
                event: Event::AcceptEntries { last_index: 5 },
            })?;
            assert_node(&mut node).is_leader().term(3).committed(2);
            assert_messages(&mut node_rx, vec![]);
            assert_messages(&mut state_rx, vec![]);
        }
        Ok(())
    }

    #[test]
    // AcceptEntries quorum for entry in past term should not trigger commit
    fn step_acceptentries_past_term() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let peers = leader.peers.clone();
        let mut node: Node = leader.into();

        for peer in peers.into_iter() {
            node = node.step(Message {
                from: Address::Peer(peer),
                to: Address::Peer(1),
                term: 3,
                event: Event::AcceptEntries { last_index: 3 },
            })?;
            assert_node(&mut node).is_leader().term(3).committed(2);
            assert_messages(&mut node_rx, vec![]);
            assert_messages(&mut state_rx, vec![]);
        }
        Ok(())
    }

    #[test]
    // AcceptEntries quorum for missing future entry
    fn step_acceptentries_future_index() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let peers = leader.peers.clone();
        let mut node: Node = leader.into();

        for (i, peer) in peers.into_iter().enumerate() {
            node = node.step(Message {
                from: Address::Peer(peer),
                to: Address::Peer(1),
                term: 3,
                event: Event::AcceptEntries { last_index: 7 },
            })?;
            // The local leader will cast a vote to commit 5, thus when we have votes 2x7, 1x5, 2x0
            // we will commit index 5. However, we will correctly ignore the following votes for7.
            let c = if i == 0 { 2 } else { 5 };
            assert_node(&mut node).is_leader().term(3).committed(c).last(5);
            assert_messages(&mut node_rx, vec![]);
            if i == 1 {
                assert_messages(
                    &mut state_rx,
                    vec![
                        Instruction::Apply {
                            entry: Entry { index: 3, term: 2, command: Some(vec![0x03]) },
                        },
                        Instruction::Apply {
                            entry: Entry { index: 4, term: 3, command: Some(vec![0x04]) },
                        },
                        Instruction::Apply {
                            entry: Entry { index: 5, term: 3, command: Some(vec![0x05]) },
                        },
                    ],
                );
            } else {
                assert_messages(&mut state_rx, vec![]);
            }
        }
        Ok(())
    }

    #[test]
    fn step_rejectentries() -> Result<()> {
        let (mut leader, mut node_rx, mut state_rx) = setup()?;
        let entries = leader.log.scan(0..)?.collect::<Result<Vec<_>>>()?;
        let mut node: Node = leader.into();

        for i in 0..(entries.len() + 3) {
            node = node.step(Message {
                from: Address::Peer(2),
                to: Address::Peer(1),
                term: 3,
                event: Event::RejectEntries,
            })?;
            assert_node(&mut node).is_leader().term(3).committed(2);
            let index = if i >= entries.len() { 0 } else { entries.len() - i - 1 };
            let replicate = entries.get(index..).unwrap().to_vec();
            assert_messages(
                &mut node_rx,
                vec![Message {
                    from: Address::Local,
                    to: Address::Peer(2),
                    term: 3,
                    event: Event::ReplicateEntries {
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
            assert_messages(&mut state_rx, vec![]);
        }
        Ok(())
    }

    #[test]
    // Sending a client query request will pass it to the state machine and trigger heartbeats.
    fn step_clientrequest_query() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let quorum = leader.quorum();
        let mut node: Node = leader.into();
        node = node.step(Message {
            from: Address::Client,
            to: Address::Local,
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Query(vec![0xaf]) },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2).last(5);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Broadcast,
                term: 3,
                event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
            }],
        );
        assert_messages(
            &mut state_rx,
            vec![
                Instruction::Query {
                    id: vec![0x01],
                    address: Address::Client,
                    command: vec![0xaf],
                    term: 3,
                    index: 2,
                    quorum,
                },
                Instruction::Vote { term: 3, index: 2, address: Address::Local },
            ],
        );
        Ok(())
    }

    #[test]
    // Sending a mutate request should append it to log, replicate it to peers, and register notification.
    fn step_clientrequest_mutate() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let peers = leader.peers.clone();
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Client,
            to: Address::Local,
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Mutate(vec![0xaf]) },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2).last(6).entry(Entry {
            index: 6,
            term: 3,
            command: Some(vec![0xaf]),
        });

        for peer in peers.iter().cloned() {
            assert_eq!(
                node_rx.try_recv()?,
                Message {
                    from: Address::Local,
                    to: Address::Peer(peer),
                    term: 3,
                    event: Event::ReplicateEntries {
                        base_index: 5,
                        base_term: 3,
                        entries: vec![Entry { index: 6, term: 3, command: Some(vec![0xaf]) },]
                    },
                }
            )
        }
        assert_messages(&mut node_rx, vec![]);
        assert_messages(
            &mut state_rx,
            vec![Instruction::Notify { id: vec![0x01], address: Address::Client, index: 6 }],
        );

        Ok(())
    }

    #[test]
    // Sending a status request should pass it on to state machine, to add status.
    fn step_clientrequest_status() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Client,
            to: Address::Local,
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Status },
        })?;
        assert_node(&mut node).is_leader().term(3).committed(2).last(5);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(
            &mut state_rx,
            vec![Instruction::Status {
                id: vec![0x01],
                address: Address::Client,
                status: Box::new(Status {
                    server: 1,
                    leader: 1,
                    term: 3,
                    node_last_index: vec![(1, 5), (2, 0), (3, 0), (4, 0), (5, 0)]
                        .into_iter()
                        .collect(),
                    commit_index: 2,
                    apply_index: 0,
                    storage: "memory".into(),
                    storage_size: 71,
                }),
            }],
        );

        Ok(())
    }

    #[test]
    fn tick() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();
        for _ in 0..5 {
            for _ in 0..HEARTBEAT_INTERVAL {
                assert_messages(&mut node_rx, vec![]);
                assert_messages(&mut state_rx, vec![]);
                node = node.tick()?;
                assert_node(&mut node).is_leader().term(3).committed(2);
            }

            assert_eq!(
                node_rx.try_recv()?,
                Message {
                    from: Address::Local,
                    to: Address::Broadcast,
                    term: 3,
                    event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
                }
            );
        }
        Ok(())
    }
}
