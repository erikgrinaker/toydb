use super::{
    Envelope, Index, Log, Message, ReadSequence, Request, RequestID, Response, State, Status,
};
use crate::error::{Error, Result};

use itertools::Itertools as _;
use log::{debug, info};
use rand::Rng as _;
use std::collections::{HashMap, HashSet, VecDeque};

/// A node ID.
pub type NodeID = u8;

/// A leader term.
pub type Term = u64;

/// A logical clock interval as number of ticks.
pub type Ticks = u8;

/// A Raft node, with a dynamic role. The node is driven synchronously by
/// processing inbound messages via step() or by advancing time via tick().
/// These methods consume the current node, and return a new one with a possibly
/// different role. Outbound messages are sent via the given node_tx channel.
///
/// This enum wraps the RawNode<Role> types, which implement the actual
/// node logic. It exists for ergonomic use across role transitions, i.e
/// node = node.step()?.
pub enum Node {
    Candidate(RawNode<Candidate>),
    Follower(RawNode<Follower>),
    Leader(RawNode<Leader>),
}

impl Node {
    /// Creates a new Raft node, starting as a leaderless follower, or leader if
    /// there are no peers.
    pub fn new(
        id: NodeID,
        peers: HashSet<NodeID>,
        log: Log,
        state: Box<dyn State>,
        node_tx: crossbeam::channel::Sender<Envelope>,
        heartbeat_interval: Ticks,
        election_timeout_range: std::ops::Range<Ticks>,
    ) -> Result<Self> {
        let node = RawNode::new(
            id,
            peers,
            log,
            state,
            node_tx,
            heartbeat_interval,
            election_timeout_range,
        )?;
        if node.peers.is_empty() {
            // If there are no peers, become leader immediately.
            return Ok(node.into_candidate()?.into_leader()?.into());
        }
        Ok(node.into())
    }

    /// Returns the node ID.
    pub fn id(&self) -> NodeID {
        match self {
            Node::Candidate(n) => n.id,
            Node::Follower(n) => n.id,
            Node::Leader(n) => n.id,
        }
    }

    /// Returns the node term.
    pub fn term(&self) -> Term {
        match self {
            Node::Candidate(n) => n.term,
            Node::Follower(n) => n.term,
            Node::Leader(n) => n.term,
        }
    }

    /// Processes a message from a peer.
    pub fn step(self, msg: Envelope) -> Result<Self> {
        debug!("Stepping {:?}", msg);
        match self {
            Node::Candidate(n) => n.step(msg),
            Node::Follower(n) => n.step(msg),
            Node::Leader(n) => n.step(msg),
        }
    }

    /// Moves time forward by a tick.
    pub fn tick(self) -> Result<Self> {
        match self {
            Node::Candidate(n) => n.tick(),
            Node::Follower(n) => n.tick(),
            Node::Leader(n) => n.tick(),
        }
    }
}

impl From<RawNode<Candidate>> for Node {
    fn from(n: RawNode<Candidate>) -> Self {
        Node::Candidate(n)
    }
}

impl From<RawNode<Follower>> for Node {
    fn from(n: RawNode<Follower>) -> Self {
        Node::Follower(n)
    }
}

impl From<RawNode<Leader>> for Node {
    fn from(n: RawNode<Leader>) -> Self {
        Node::Leader(n)
    }
}

/// A Raft role: leader, follower, or candidate.
pub trait Role: Clone + std::fmt::Debug + PartialEq {}

/// A Raft node with the concrete role R.
///
/// This implements the typestate pattern, where individual node states (roles)
/// are encoded as RawNode<Role>. See: http://cliffle.com/blog/rust-typestate/
pub struct RawNode<R: Role = Follower> {
    id: NodeID,
    peers: HashSet<NodeID>,
    term: Term,
    log: Log,
    state: Box<dyn State>,
    node_tx: crossbeam::channel::Sender<Envelope>,
    heartbeat_interval: Ticks,
    election_timeout_range: std::ops::Range<Ticks>,
    role: R,
}

impl<R: Role> RawNode<R> {
    /// Helper for role transitions.
    fn into_role<T: Role>(self, role: T) -> RawNode<T> {
        RawNode {
            id: self.id,
            peers: self.peers,
            term: self.term,
            log: self.log,
            state: self.state,
            node_tx: self.node_tx,
            heartbeat_interval: self.heartbeat_interval,
            election_timeout_range: self.election_timeout_range,
            role,
        }
    }

    /// Applies any pending, committed entries to the state machine. The command
    /// responses are discarded, use maybe_apply_with() instead to access them.
    fn maybe_apply(&mut self) -> Result<()> {
        Self::maybe_apply_with(&mut self.log, &mut self.state, |_, _| Ok(()))
    }

    /// Like maybe_apply(), but calls the given closure with the result of every
    /// applied command. Not a method, so that the closure can mutate the node.
    fn maybe_apply_with<F>(log: &mut Log, state: &mut Box<dyn State>, mut on_apply: F) -> Result<()>
    where
        F: FnMut(Index, Result<Vec<u8>>) -> Result<()>,
    {
        let applied_index = state.get_applied_index();
        let commit_index = log.get_commit_index().0;
        assert!(commit_index >= applied_index, "Commit index below applied index");
        if applied_index >= commit_index {
            return Ok(());
        }

        let mut scan = log.scan((applied_index + 1)..=commit_index)?;
        while let Some(entry) = scan.next().transpose()? {
            let index = entry.index;
            debug!("Applying {:?}", entry);
            match state.apply(entry) {
                Err(error @ Error::Internal(_)) => return Err(error),
                result => on_apply(index, result)?,
            }
        }
        Ok(())
    }

    /// Returns the size of the cluster.
    fn cluster_size(&self) -> u8 {
        self.peers.len() as u8 + 1
    }

    /// Returns the quorum size of the cluster.
    fn quorum_size(&self) -> u8 {
        quorum_size(self.cluster_size())
    }

    /// Returns the quorum value of the given unsorted slice, in descending
    /// order. The slice must have the same size as the cluster.
    fn quorum_value<T: Ord + Copy>(&self, values: Vec<T>) -> T {
        assert!(values.len() == self.cluster_size() as usize, "values must match cluster size");
        quorum_value(values)
    }

    /// Sends a message.
    fn send(&self, to: NodeID, message: Message) -> Result<()> {
        let msg = Envelope { from: self.id, to, term: self.term, message };
        debug!("Sending {msg:?}");
        Ok(self.node_tx.send(msg)?)
    }

    /// Broadcasts a message to all peers.
    fn broadcast(&self, message: Message) -> Result<()> {
        // Sort for test determinism.
        for id in self.peers.iter().copied().sorted() {
            self.send(id, message.clone())?;
        }
        Ok(())
    }

    /// Generates a randomized election timeout.
    fn gen_election_timeout(&self) -> Ticks {
        rand::thread_rng().gen_range(self.election_timeout_range.clone())
    }

    /// Asserts common node invariants.
    fn assert_node(&mut self) -> Result<()> {
        debug_assert_eq!(self.term, self.log.get_term()?.0, "Term does not match log");
        Ok(())
    }

    /// Asserts message invariants when stepping.
    fn assert_step(&self, msg: &Envelope) {
        // Messages must be addressed to the local node.
        assert_eq!(msg.to, self.id, "Message to other node");

        // Senders must be known.
        assert!(
            msg.from == self.id || self.peers.contains(&msg.from),
            "Unknown sender {}",
            msg.from
        );
    }
}

/// A candidate is campaigning to become a leader.
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    /// Votes received (including ourself).
    votes: HashSet<NodeID>,
    /// Ticks elapsed since election start.
    election_duration: Ticks,
    /// Election timeout, in ticks.
    election_timeout: Ticks,
}

impl Candidate {
    /// Creates a new candidate role.
    fn new(election_timeout: Ticks) -> Self {
        Self { votes: HashSet::new(), election_duration: 0, election_timeout }
    }
}

impl Role for Candidate {}

impl RawNode<Candidate> {
    /// Asserts internal invariants.
    fn assert(&mut self) -> Result<()> {
        self.assert_node()?;

        assert_ne!(self.term, 0, "Candidates can't have term 0");
        assert!(self.role.votes.contains(&self.id), "Candidate did not vote for self");
        debug_assert_eq!(Some(self.id), self.log.get_term()?.1, "Log vote does not match self");

        assert!(
            self.role.election_duration < self.role.election_timeout,
            "Election timeout passed"
        );

        Ok(())
    }

    /// Transitions the candidate to a follower. We either lost the election and
    /// follow the winner, or we discovered a new term in which case we step
    /// into it as a leaderless follower.
    fn into_follower(mut self, term: Term, leader: Option<NodeID>) -> Result<RawNode<Follower>> {
        assert!(term >= self.term, "Term regression {} -> {}", self.term, term);

        let election_timeout = self.gen_election_timeout();
        if let Some(leader) = leader {
            // We lost the election, follow the winner.
            assert_eq!(term, self.term, "Can't follow leader in different term");
            info!("Lost election, following leader {} in term {}", leader, term);
            let voted_for = Some(self.id); // by definition
            Ok(self.into_role(Follower::new(Some(leader), voted_for, election_timeout)))
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term, "Can't become leaderless follower in current term");
            info!("Discovered new term {}", term);
            self.term = term;
            self.log.set_term(term, None)?;
            Ok(self.into_role(Follower::new(None, None, election_timeout)))
        }
    }

    /// Transitions the candidate to a leader. We won the election.
    fn into_leader(self) -> Result<RawNode<Leader>> {
        info!("Won election for term {}, becoming leader", self.term);
        let peers = self.peers.clone();
        let (last_index, _) = self.log.get_last_index();
        let mut node = self.into_role(Leader::new(peers, last_index));

        // Propose an empty command when assuming leadership, to disambiguate
        // previous entries in the log. See section 8 in the Raft paper.
        //
        // We do this prior to the heartbeat, to avoid a wasted replication
        // roundtrip if the heartbeat response indicates the peer is behind.
        node.propose(None)?;
        node.heartbeat()?;

        Ok(node)
    }

    /// Processes a message.
    fn step(mut self, msg: Envelope) -> Result<Node> {
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
            return self.into_follower(msg.term, None)?.step(msg);
        }

        match msg.message {
            // Don't grant votes for other candidates who also campaign.
            Message::Campaign { .. } => {
                self.send(msg.from, Message::CampaignResponse { vote: false })?
            }

            // If we received a vote, record it. If the vote gives us quorum,
            // assume leadership.
            Message::CampaignResponse { vote: true } => {
                self.role.votes.insert(msg.from);
                if self.role.votes.len() as u8 >= self.quorum_size() {
                    return Ok(self.into_leader()?.into());
                }
            }

            // We didn't get a vote. :(
            Message::CampaignResponse { vote: false } => {}

            // If we receive a heartbeat or entries in this term, we lost the
            // election and have a new leader. Follow it and step the message.
            Message::Heartbeat { .. } | Message::Append { .. } => {
                return self.into_follower(msg.term, Some(msg.from))?.step(msg);
            }

            // Abort any inbound client requests while candidate.
            Message::ClientRequest { id, .. } => {
                self.send(msg.from, Message::ClientResponse { id, response: Err(Error::Abort) })?;
            }

            // We're not a leader in this term, nor are we forwarding requests,
            // so we shouldn't see these.
            Message::HeartbeatResponse { .. }
            | Message::AppendResponse { .. }
            | Message::ClientResponse { .. } => panic!("Received unexpected message {:?}", msg),
        }
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    fn tick(mut self) -> Result<Node> {
        self.assert()?;

        self.role.election_duration += 1;
        if self.role.election_duration >= self.role.election_timeout {
            self.campaign()?;
        }
        Ok(self.into())
    }

    /// Campaign for leadership by increasing the term, voting for ourself, and
    /// soliciting votes from all peers.
    fn campaign(&mut self) -> Result<()> {
        let term = self.term + 1;
        info!("Starting new election for term {}", term);
        self.role = Candidate::new(self.gen_election_timeout());
        self.role.votes.insert(self.id); // vote for ourself
        self.term = term;
        self.log.set_term(term, Some(self.id))?;

        let (last_index, last_term) = self.log.get_last_index();
        self.broadcast(Message::Campaign { last_index, last_term })?;
        Ok(())
    }
}

// A follower replicates state from a leader.
#[derive(Clone, Debug, PartialEq)]
pub struct Follower {
    /// The leader, or None if just initialized.
    leader: Option<NodeID>,
    /// The number of ticks since the last message from the leader.
    leader_seen: Ticks,
    /// The leader_seen timeout before triggering an election.
    election_timeout: Ticks,
    /// The node we voted for in the current term, if any.
    voted_for: Option<NodeID>,
    // Local client requests that have been forwarded to the leader. These are
    // aborted on leader/term changes.
    forwarded: HashSet<RequestID>,
}

impl Follower {
    /// Creates a new follower role.
    fn new(leader: Option<NodeID>, voted_for: Option<NodeID>, election_timeout: Ticks) -> Self {
        Self { leader, voted_for, leader_seen: 0, election_timeout, forwarded: HashSet::new() }
    }
}

impl Role for Follower {}

impl RawNode<Follower> {
    /// Creates a new node as a leaderless follower.
    fn new(
        id: NodeID,
        peers: HashSet<NodeID>,
        mut log: Log,
        state: Box<dyn State>,
        node_tx: crossbeam::channel::Sender<Envelope>,
        heartbeat_interval: Ticks,
        election_timeout_range: std::ops::Range<Ticks>,
    ) -> Result<Self> {
        let (term, voted_for) = log.get_term()?;
        let role = Follower::new(None, voted_for, 0);
        let mut node = Self {
            id,
            peers,
            term,
            log,
            state,
            node_tx,
            heartbeat_interval,
            election_timeout_range,
            role,
        };
        node.role.election_timeout = node.gen_election_timeout();
        Ok(node)
    }

    /// Asserts internal invariants.
    fn assert(&mut self) -> Result<()> {
        self.assert_node()?;

        if let Some(leader) = self.role.leader {
            assert_ne!(leader, self.id, "Can't follow self");
            assert!(self.peers.contains(&leader), "Leader not in peers");
            assert_ne!(self.term, 0, "Followers with leaders can't have term 0");
        } else {
            assert!(self.role.forwarded.is_empty(), "Leaderless follower has forwarded requests");
        }

        // NB: We allow voted_for not in peers, since this can happen when
        // removing nodes from the cluster via a cold restart. We also allow
        // voted_for self, which can happen if we lose an election.

        debug_assert_eq!(self.role.voted_for, self.log.get_term()?.1, "Vote does not match log");
        assert!(self.role.leader_seen < self.role.election_timeout, "Election timeout passed");

        Ok(())
    }

    /// Transitions the follower into a candidate, by campaigning for
    /// leadership in a new term.
    fn into_candidate(mut self) -> Result<RawNode<Candidate>> {
        // Abort any forwarded requests. These must be retried with new leader.
        self.abort_forwarded()?;

        // Apply any pending log entries, so that we're caught up if we win.
        self.maybe_apply()?;

        let election_timeout = self.gen_election_timeout();
        let mut node = self.into_role(Candidate::new(election_timeout));
        node.campaign()?;
        Ok(node)
    }

    /// Transitions the candidate into a follower, either a leaderless follower
    /// in a new term (e.g. if someone holds a new election) or following a
    /// leader in the current term once someone wins the election.
    fn into_follower(mut self, leader: Option<NodeID>, term: Term) -> Result<RawNode<Follower>> {
        assert!(term >= self.term, "Term regression {} -> {}", self.term, term);

        // Abort any forwarded requests. These must be retried with new leader.
        self.abort_forwarded()?;

        if let Some(leader) = leader {
            // We found a leader in the current term.
            assert_eq!(self.role.leader, None, "Already have leader in term");
            assert_eq!(term, self.term, "Can't follow leader in different term");
            info!("Following leader {} in term {}", leader, term);
            self.role =
                Follower::new(Some(leader), self.role.voted_for, self.role.election_timeout);
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term, "Can't become leaderless follower in current term");
            info!("Discovered new term {}", term);
            self.term = term;
            self.log.set_term(term, None)?;
            self.role = Follower::new(None, None, self.gen_election_timeout());
        }
        Ok(self)
    }

    /// Processes a message.
    fn step(mut self, msg: Envelope) -> Result<Node> {
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
            return self.into_follower(None, msg.term)?.step(msg);
        }

        // Record when we last saw a message from the leader (if any).
        if self.is_leader(msg.from) {
            self.role.leader_seen = 0
        }

        match msg.message {
            // The leader will send periodic heartbeats. If we don't have a
            // leader in this term yet, follow it. If the commit_index advances,
            // apply state transitions.
            Message::Heartbeat { commit_index, commit_term, read_seq } => {
                // Check that the heartbeat is from our leader.
                match self.role.leader {
                    Some(leader) => assert_eq!(msg.from, leader, "multiple leaders in term"),
                    None => self = self.into_follower(Some(msg.from), msg.term)?,
                }

                // Respond to the heartbeat.
                let (last_index, last_term) = self.log.get_last_index();
                self.send(
                    msg.from,
                    Message::HeartbeatResponse { last_index, last_term, read_seq },
                )?;

                // Advance commit index and apply entries.
                if self.log.has(commit_index, commit_term)?
                    && commit_index > self.log.get_commit_index().0
                {
                    self.log.commit(commit_index)?;
                    self.maybe_apply()?;
                }
            }

            // Replicate entries from the leader. If we don't have a leader in
            // this term yet, follow it.
            Message::Append { base_index, base_term, entries } => {
                // Check that the entries are from our leader.
                let from = msg.from;
                match self.role.leader {
                    Some(leader) => assert_eq!(from, leader, "multiple leaders in term"),
                    None => self = self.into_follower(Some(from), msg.term)?,
                }

                // Append the entries, if possible.
                let reject = base_index > 0 && !self.log.has(base_index, base_term)?;
                if !reject {
                    self.log.splice(entries)?;
                }
                let (last_index, last_term) = self.log.get_last_index();
                self.send(msg.from, Message::AppendResponse { reject, last_index, last_term })?;
            }

            // A candidate in this term is requesting our vote.
            Message::Campaign { last_index, last_term } => {
                // Don't vote if we already voted for someone else in this term.
                if let Some(voted_for) = self.role.voted_for {
                    if msg.from != voted_for {
                        self.send(msg.from, Message::CampaignResponse { vote: false })?;
                        return Ok(self.into());
                    }
                }

                // Don't vote if our log is newer than the candidate's log.
                let (log_index, log_term) = self.log.get_last_index();
                if log_term > last_term || log_term == last_term && log_index > last_index {
                    self.send(msg.from, Message::CampaignResponse { vote: false })?;
                    return Ok(self.into());
                }

                // Grant the vote.
                info!("Voting for {} in term {} election", msg.from, self.term);
                self.send(msg.from, Message::CampaignResponse { vote: true })?;
                self.log.set_term(self.term, Some(msg.from))?;
                self.role.voted_for = Some(msg.from);
            }

            // We may receive a vote after we lost an election and followed a
            // different leader. Ignore it.
            Message::CampaignResponse { .. } => {}

            // Forward client requests to the leader, or abort them if there is
            // none (the client must retry).
            Message::ClientRequest { ref id, .. } => {
                assert_eq!(msg.from, self.id, "Client request from other node");

                let id = id.clone();
                if let Some(leader) = self.role.leader {
                    debug!("Forwarding request to leader {}: {:?}", leader, msg);
                    self.role.forwarded.insert(id);
                    self.send(leader, msg.message)?
                } else {
                    self.send(
                        msg.from,
                        Message::ClientResponse { id, response: Err(Error::Abort) },
                    )?
                }
            }

            // Returns client responses for forwarded requests.
            Message::ClientResponse { id, response } => {
                assert!(self.is_leader(msg.from), "Client response from non-leader");

                if self.role.forwarded.remove(&id) {
                    self.send(self.id, Message::ClientResponse { id, response })?;
                }
            }

            // We're not a leader nor candidate in this term, so we shoudn't see these.
            Message::HeartbeatResponse { .. } | Message::AppendResponse { .. } => {
                panic!("Received unexpected message {msg:?}")
            }
        };
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    fn tick(mut self) -> Result<Node> {
        self.assert()?;

        self.role.leader_seen += 1;
        if self.role.leader_seen >= self.role.election_timeout {
            return Ok(self.into_candidate()?.into());
        }
        Ok(self.into())
    }

    /// Aborts all forwarded requests.
    fn abort_forwarded(&mut self) -> Result<()> {
        // Sort the IDs for test determinism.
        for id in std::mem::take(&mut self.role.forwarded).into_iter().sorted() {
            debug!("Aborting forwarded request {:x?}", id);
            self.send(self.id, Message::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    /// Checks if an address is the current leader.
    fn is_leader(&self, from: NodeID) -> bool {
        self.role.leader == Some(from)
    }
}

/// Peer replication progress.
#[derive(Clone, Debug, PartialEq)]
struct Progress {
    /// The next index to replicate to the peer.
    next: Index,
    /// The last index known to be replicated to the peer.
    ///
    /// TODO: rename to match. It needs to track the position where the
    /// follower's log matches the leader's, not its last position.
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
    fn new(peers: HashSet<NodeID>, last_index: Index) -> Self {
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
    fn into_follower(mut self, term: Term) -> Result<RawNode<Follower>> {
        assert!(term >= self.term, "Term regression {} -> {}", self.term, term);
        assert!(term > self.term, "Can only become follower in later term");

        info!("Discovered new term {}", term);

        // Cancel in-flight requests.
        for write in
            std::mem::take(&mut self.role.writes).into_values().sorted_by_key(|w| w.id.clone())
        {
            self.send(
                write.from,
                Message::ClientResponse { id: write.id, response: Err(Error::Abort) },
            )?;
        }
        for read in std::mem::take(&mut self.role.reads).into_iter().sorted_by_key(|r| r.id.clone())
        {
            self.send(
                read.from,
                Message::ClientResponse { id: read.id, response: Err(Error::Abort) },
            )?;
        }

        self.term = term;
        self.log.set_term(term, None)?;
        let election_timeout = self.gen_election_timeout();
        Ok(self.into_role(Follower::new(None, None, election_timeout)))
    }

    /// Processes a message.
    fn step(mut self, msg: Envelope) -> Result<Node> {
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
                panic!("saw other leader {} in term {}", msg.from, msg.term);
            }

            // A follower received one of our heartbeats and confirms that we
            // are its leader. If its log is incomplete, append entries. If the
            // peer's read sequence number increased, process any pending reads.
            //
            // TODO: this needs to commit and apply entries following a leader
            // change too, otherwise we can serve stale reads if the new leader
            // hadn't fully committed and applied entries yet.
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
                    "follower accepted entries after last index"
                );
                assert!(
                    last_term <= self.log.get_last_index().1,
                    "follower accepted entries after last term"
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
                        .sorted()
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

            // Don't grant other votes in this term.
            Message::Campaign { .. } => {
                self.send(msg.from, Message::CampaignResponse { vote: false })?
            }

            // Votes can come in after we won the election, ignore them.
            Message::CampaignResponse { .. } => {}

            // Leaders never proxy client requests, so we don't expect to see
            // responses from other nodes.
            Message::ClientResponse { .. } => panic!("Unexpected message {:?}", msg),
        }

        Ok(self.into())
    }

    /// Processes a logical clock tick.
    fn tick(mut self) -> Result<Node> {
        self.assert()?;

        self.role.since_heartbeat += 1;
        if self.role.since_heartbeat >= self.heartbeat_interval {
            self.heartbeat()?;
            self.role.since_heartbeat = 0;
        }
        Ok(self.into())
    }

    /// Broadcasts a heartbeat to all peers.
    fn heartbeat(&mut self) -> Result<()> {
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
    fn propose(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        let index = self.log.append(self.term, command)?;
        for peer in self.peers.iter().copied().sorted() {
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
                None => panic!("missing base entry {}", next - 1),
            },
            Some(_) => (0, 0),
            None => panic!("unknown peer {}", peer),
        };

        let entries = self.log.scan((base_index + 1)..)?.collect::<Result<Vec<_>>>()?;
        debug!("Replicating {} entries at base {} to {}", entries.len(), base_index, peer);
        self.send(peer, Message::Append { base_index, base_term, entries })?;
        Ok(())
    }
}

/// Returns the size of a quorum (strict majority), given a total size.
fn quorum_size(size: u8) -> u8 {
    size / 2 + 1
}

/// Returns the quorum (median) value of the given unsorted slice, in descending
/// order. The slice cannot be empty.
fn quorum_value<T: Ord + Copy>(mut values: Vec<T>) -> T {
    assert!(!values.is_empty(), "no values provided");
    let index = quorum_size(values.len() as u8) as usize - 1;
    *values.select_nth_unstable_by(index, |a, b: &T| a.cmp(b).reverse()).1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::bincode;
    use crate::raft::{
        Entry, Request, RequestID, Response, ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL,
    };
    use crossbeam::channel::{Receiver, Sender};
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use std::borrow::Borrow;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::error::Error;
    use std::result::Result;
    use test_each_file::test_each_path;

    #[test]
    fn quorum_size() {
        for (size, quorum) in [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4), (8, 5)] {
            assert_eq!(super::quorum_size(size), quorum);
        }
    }

    #[test]
    fn quorum_value() {
        assert_eq!(super::quorum_value(vec![1]), 1);
        assert_eq!(super::quorum_value(vec![1, 3, 2]), 2);
        assert_eq!(super::quorum_value(vec![4, 1, 3, 2]), 2);
        assert_eq!(super::quorum_value(vec![1, 1, 1, 2, 2]), 1);
        assert_eq!(super::quorum_value(vec![1, 1, 2, 2, 2]), 2);
    }

    // Run goldenscript tests in src/raft/testscripts/node.
    test_each_path! { in "src/raft/testscripts/node" as scripts => test_goldenscript }

    fn test_goldenscript(path: &std::path::Path) {
        goldenscript::run(&mut TestRunner::new(), path).expect("goldenscript failed")
    }

    /// Runs Raft goldenscript tests. For available commands, see run().
    #[derive(Default)]
    struct TestRunner {
        /// IDs of all cluster nodes, in order.
        ids: Vec<NodeID>,
        /// The cluster nodes, keyed by node ID.
        nodes: HashMap<NodeID, Node>,
        /// Outbound send queues from each node.
        nodes_rx: HashMap<NodeID, Receiver<Envelope>>,
        /// Inbound receive queues to each node, to be stepped.
        nodes_pending: HashMap<NodeID, Vec<Envelope>>,
        /// Applied log entries for each node, after TestState application.
        applied_rx: HashMap<NodeID, Receiver<Entry>>,
        /// Network partitions, sender → receivers.
        disconnected: HashMap<NodeID, HashSet<NodeID>>,
        /// In-flight client requests.
        requests: HashMap<RequestID, Request>,
        /// The request ID to use for the next client request.
        next_request_id: u8,
    }

    impl goldenscript::Runner for TestRunner {
        /// Runs a goldenscript command.
        fn run(&mut self, command: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
            let mut output = String::new();
            match command.name.as_str() {
                // campaign [ID...]
                //
                // The given nodes transition to candidates and campaign.
                "campaign" => {
                    let ids = self.parse_ids_or_all(&command.args)?;
                    self.campaign(&ids, &mut output)?;
                }

                // cluster nodes=N [leader=ID] [heartbeat_interval=N] [election_timeout=N]
                //
                // Creates a new Raft cluster.
                "cluster" => {
                    let mut args = command.consume_args();
                    let nodes = args.lookup_parse("nodes")?.unwrap_or(0);
                    let leader = args.lookup_parse("leader")?;
                    let heartbeat_interval =
                        args.lookup_parse("heartbeat_interval")?.unwrap_or(HEARTBEAT_INTERVAL);
                    let election_timeout = args
                        .lookup_parse("election_timeout")?
                        .unwrap_or(ELECTION_TIMEOUT_RANGE.start);
                    args.reject_rest()?;
                    self.cluster(nodes, leader, heartbeat_interval, election_timeout, &mut output)?;
                }

                // deliver [from=ID] [ID...]
                //
                // Delivers (steps) pending messages to the given nodes. If from
                // is given, only messages from the given node is delivered, the
                // others are left pending.
                "deliver" => {
                    let mut args = command.consume_args();
                    let from = args.lookup_parse("from")?;
                    let ids = self.parse_ids_or_all(&args.rest())?;
                    self.deliver(&ids, from, &mut output)?;
                }

                // get ID KEY
                //
                // Sends a client request to the given node to read the given
                // key from the state machine (key/value store).
                "get" => {
                    let mut args = command.consume_args();
                    let id = args.next_pos().ok_or("must specify node ID")?.parse()?;
                    let key = args.next_pos().ok_or("must specify key")?.value.clone();
                    args.reject_rest()?;
                    let request = Request::Read(TestCommand::Get { key }.encode()?);
                    self.request(id, request, &mut output)?;
                }

                // heal [ID...]
                //
                // Heals all network partitions for the given nodes.
                "heal" => {
                    let ids = self.parse_ids_or_all(&command.args)?;
                    self.heal(&ids, &mut output)?;
                }

                // heartbeat ID...
                //
                // Sends a heartbeat from the given leader nodes.
                "heartbeat" => {
                    let ids = self.parse_ids_or_all(&command.args)?;
                    self.heartbeat(&ids, &mut output)?;
                }

                // log [ID...]
                //
                // Outputs the current Raft log for the given nodes.
                "log" => {
                    let ids = self.parse_ids_or_all(&command.args)?;
                    self.log(&ids, &mut output)?;
                }

                // partition ID...
                //
                // Partitions the given nodes away from the rest of the cluster.
                // They can still communicate with each other, unless they were
                // previously partitioned.
                "partition" => {
                    let ids = self.parse_ids_or_error(&command.args)?;
                    self.partition(&ids, &mut output)?;
                }

                // put ID KEY=VALUE
                //
                // Sends a client request to the given node to write a key/value
                // pair to the state machine (key/value store).
                "put" => {
                    let mut args = command.consume_args();
                    let id = args.next_pos().ok_or("must specify node ID")?.parse()?;
                    let kv = args.next_key().ok_or("must specify key/value pair")?.clone();
                    let (key, value) = (kv.key.unwrap(), kv.value);
                    args.reject_rest()?;
                    let request = Request::Write(TestCommand::Put { key, value }.encode()?);
                    self.request(id, request, &mut output)?;
                }

                // stabilize [heartbeat=BOOL] [ID...]
                //
                // Stabilizes the given nodes by repeatedly delivering messages
                // until no more messages are pending. If heartbeat is true, also
                // emits a heartbeat from the leader and restabilizes, e.g. to
                // propagate the commit index.
                "stabilize" => {
                    let mut args = command.consume_args();
                    let heartbeat = args.lookup_parse("heartbeat")?.unwrap_or(false);
                    let ids = self.parse_ids_or_all(&args.rest())?;
                    self.stabilize(&ids, heartbeat, &mut output)?;
                }

                // state [ID...]
                //
                // Prints the current state machine contents on the given nodes.
                "state" => {
                    let ids = self.parse_ids_or_error(&command.args)?;
                    self.state(&ids, &mut output)?;
                }

                // status [request=BOOL] [ID...]
                //
                // Prints the current node status of the given nodes. If request
                // is true, sends a status client request to a single node,
                // otherwise fetches status directly from each node.
                "status" => {
                    let mut args = command.consume_args();
                    let request = args.lookup_parse("request")?.unwrap_or(false);
                    let ids = self.parse_ids_or_all(&args.rest())?;
                    if request {
                        if ids.len() != 1 {
                            return Err("request=true requires 1 node ID".into());
                        }
                        self.request(ids[0], Request::Status, &mut output)?;
                    } else {
                        self.status(&ids, &mut output)?;
                    }
                }

                // step [panic=BOOL] ID JSON
                //
                // Steps a manually generated JSON message on the given node.
                "step" => {
                    let mut args = command.consume_args();
                    let panic = args.lookup_parse("panic")?.unwrap_or(false);
                    let id = args.next_pos().ok_or("node ID not given")?.parse()?;
                    let raw = &args.next_pos().ok_or("message not given")?.value;
                    let msg = serde_json::from_str(raw)?;
                    args.reject_rest()?;
                    self.transition_catch(id, |n| n.step(msg), panic, &mut output)?;
                }

                // tick [ID...]
                //
                // Ticks the given nodes.
                "tick" => {
                    let ids = self.parse_ids_or_all(&command.args)?;
                    for id in ids {
                        self.transition(id, |n| n.tick(), &mut output)?;
                    }
                }

                name => return Err(format!("unknown command {name}").into()),
            }
            Ok(output)
        }
    }

    impl TestRunner {
        fn new() -> Self {
            Self { next_request_id: 1, ..Default::default() }
        }

        /// Makes the given nodes campaign, by transitioning to candidates.
        fn campaign(&mut self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            let campaign = |node| match node {
                Node::Candidate(mut node) => {
                    node.campaign()?;
                    Ok(node.into())
                }
                Node::Follower(node) => Ok(node.into_candidate()?.into()),
                Node::Leader(node) => {
                    Err(crate::error::Error::Internal(format!("{} is a leader", node.id)))
                }
            };
            for id in ids.iter().copied() {
                self.transition(id, campaign, output)?;
            }
            Ok(())
        }

        /// Creates a Raft cluster.
        fn cluster(
            &mut self,
            nodes: u8,
            leader: Option<NodeID>,
            heartbeat_interval: Ticks,
            election_timeout: Ticks,
            output: &mut String,
        ) -> Result<(), Box<dyn Error>> {
            if !self.ids.is_empty() {
                return Err("cluster already exists".into());
            }
            if nodes == 0 {
                return Err("cluster can't have 0 nodes".into());
            }

            self.ids = (1..=nodes).collect();

            for id in self.ids.iter().copied() {
                let (node_tx, node_rx) = crossbeam::channel::unbounded();
                let (applied_tx, applied_rx) = crossbeam::channel::unbounded();
                let peers = self.ids.iter().copied().filter(|i| *i != id).collect();
                let log = Log::new(crate::storage::Memory::new(), false)?;
                let state = Box::new(TestState::new(applied_tx));
                self.nodes.insert(
                    id,
                    Node::new(
                        id,
                        peers,
                        log,
                        state,
                        node_tx,
                        heartbeat_interval,
                        election_timeout..election_timeout + 1,
                    )?,
                );
                self.nodes_rx.insert(id, node_rx);
                self.nodes_pending.insert(id, Vec::new());
                self.applied_rx.insert(id, applied_rx);
                self.disconnected.insert(id, HashSet::new());
            }

            // Promote leader if requested. Suppress output.
            if let Some(id) = leader {
                let quiet = &mut String::new();
                let Some(Node::Follower(node)) = self.nodes.remove(&id) else {
                    return Err(format!("invalid leader {id}").into());
                };
                self.nodes.insert(id, node.into_candidate()?.into_leader()?.into());
                self.receive(id, quiet)?;
                self.stabilize(&self.ids.clone(), true, quiet)?;
            }

            // Output final cluster status.
            self.status(&self.ids, output)
        }

        /// Delivers pending inbound messages to the given nodes. If from is
        /// given, only delivers messages from the given node ID. Returns the
        /// number of delivered messages.
        fn deliver(
            &mut self,
            ids: &[NodeID],
            from: Option<NodeID>,
            output: &mut String,
        ) -> Result<u32, Box<dyn Error>> {
            // Take a snapshot of the pending queues before delivering any
            // messages. This avoids outbound messages in response to delivery
            // being delivered to higher node IDs in the same loop, which can
            // give unintuitive results.
            let mut step = Vec::new();
            for id in ids.iter().copied() {
                let Some(node_pending) = self.nodes_pending.remove(&id) else {
                    return Err(format!("unknown node {id}").into());
                };
                let (deliver, requeue) = node_pending
                    .into_iter()
                    .partition(|msg| from.is_none() || from == Some(msg.from));
                self.nodes_pending.insert(id, requeue);
                step.extend(deliver);
            }

            let delivered = step.len() as u32;
            for msg in step {
                self.transition(msg.to, |node| node.step(msg), output)?;
            }
            Ok(delivered)
        }

        /// Heals the given partitioned nodes, restoring connectivity with all
        /// other nodes.
        fn heal(&mut self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            for id in ids.iter().copied() {
                self.disconnected.insert(id, HashSet::new());
                for peers in self.disconnected.values_mut() {
                    peers.remove(&id);
                }
            }
            output.push_str(&Self::format_disconnected(&self.disconnected));
            Ok(())
        }

        /// Emits a heartbeat from the given leader nodes.
        fn heartbeat(&mut self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            for id in ids.iter().copied() {
                let node = self.nodes.get_mut(&id).ok_or(format!("unknown node {id}"))?;
                let Node::Leader(leader) = node else {
                    return Err(format!("{id} is not a leader").into());
                };
                leader.heartbeat()?;
                self.receive(id, output)?;
            }
            Ok(())
        }

        /// Outputs the current log contents for the given nodes.
        fn log(&mut self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            for id in ids {
                let node = self.nodes.get_mut(id).ok_or(format!("unknown node {id}"))?;
                let nodefmt = Self::format_node(node);
                let log = Self::borrow_log_mut(node);
                let (last_index, last_term) = log.get_last_index();
                let (commit_index, commit_term) = log.get_commit_index();
                output.push_str(&format!(
                    "{nodefmt} last={last_index}@{last_term} commit={commit_index}@{commit_term}\n",
                ));

                let mut scan = log.scan(..)?;
                while let Some(entry) = scan.next().transpose()? {
                    output.push_str(&format!("{nodefmt} entry {}\n", &Self::format_entry(&entry)));
                }
            }
            Ok(())
        }

        /// Partitions the given nodes from all other nodes in the cluster
        /// (bidirectionally). The given nodes can communicate with each other
        /// unless they were previously partitioned.
        fn partition(&mut self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            let ids: HashSet<NodeID> = HashSet::from_iter(ids.iter().copied());
            for id in ids.iter().copied() {
                for peer in self.ids.iter().copied().filter(|p| !ids.contains(p)) {
                    self.disconnected.entry(id).or_default().insert(peer);
                    self.disconnected.entry(peer).or_default().insert(id);
                }
            }
            output.push_str(&Self::format_disconnected(&self.disconnected));
            Ok(())
        }

        /// Receives outbound messages from a node, prints them, and queues them
        /// for delivery. Returns the number of received messages.
        fn receive(&mut self, id: NodeID, output: &mut String) -> Result<u32, Box<dyn Error>> {
            let rx = self.nodes_rx.get_mut(&id).ok_or(format!("unknown node {id}"))?;

            let mut count = 0;
            for msg in rx.try_iter() {
                count += 1;
                let (from, term, to) = (msg.from, msg.term, msg.to); // simplify formatting

                // If the peer is disconnected, drop the message and output it.
                if self.disconnected[&msg.from].contains(&msg.to) {
                    output.push_str(&format!(
                        "n{from}@{term} ⇥ n{to} {}\n",
                        Self::format_strikethrough(&Self::format_message(&msg.message)),
                    ));
                    continue;
                }

                // Intercept and output client responses.
                if msg.from == msg.to {
                    let Message::ClientResponse { id, response } = &msg.message else {
                        return Err(format!("invalid self-addressed message: {msg:?}").into());
                    };
                    output.push_str(&format!(
                        "n{from}@{term} → c{to} {}\n",
                        Self::format_message(&msg.message),
                    ));
                    let request = &self.requests.remove(id).ok_or("unknown request id")?;
                    output.push_str(&format!(
                        "c{to}@{term} {} ⇒ {}\n",
                        Self::format_request(request),
                        Self::format_response(response),
                    ));
                    continue;
                }

                // Output the message and queue it for delivery.
                output.push_str(&format!(
                    "n{from}@{term} → n{to} {}\n",
                    Self::format_message(&msg.message)
                ));
                self.nodes_pending.get_mut(&msg.to).ok_or(format!("unknown node {to}"))?.push(msg);
            }
            Ok(count)
        }

        /// Submits a client request via the given node.
        fn request(
            &mut self,
            id: NodeID,
            request: Request,
            output: &mut String,
        ) -> Result<(), Box<dyn Error>> {
            let node = self.nodes.get(&id).ok_or(format!("unknown node {id}"))?;
            let request_id = vec![self.next_request_id];
            self.next_request_id += 1;
            self.requests.insert(request_id.clone(), request.clone());

            let msg = Envelope {
                from: node.id(),
                to: node.id(),
                term: node.term(),
                message: Message::ClientRequest { id: request_id, request },
            };
            output.push_str(&format!(
                "c{}@{} → n{} {}\n",
                msg.from,
                msg.term,
                msg.to,
                Self::format_message(&msg.message)
            ));
            self.transition(id, |n| n.step(msg), output)
        }

        /// Stabilizes the given nodes by repeatedly delivering pending messages
        /// until no new messages are generated. If heartbeat is true, leaders
        /// then emit a heartbeat and restabilize again, e.g. to propagate the
        /// commit index.
        fn stabilize(
            &mut self,
            ids: &[NodeID],
            heartbeat: bool,
            output: &mut String,
        ) -> Result<(), Box<dyn Error>> {
            while self.deliver(ids, None, output)? > 0 {}
            if heartbeat {
                // Heartbeat the current leader (with the highest term).
                if let Some(leader) = self
                    .nodes
                    .values()
                    .sorted_by_key(|n| n.term())
                    .rev()
                    .find(|n| matches!(n, Node::Leader(_)))
                    .map(|n| n.id())
                {
                    self.heartbeat(&[leader], output)?;
                    self.stabilize(ids, false, output)?;
                }
            }
            Ok(())
        }

        /// Outputs the current state machine for the given nodes.
        fn state(&mut self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            for id in ids {
                let node = self.nodes.get_mut(id).ok_or(format!("unknown node {id}"))?;
                let state = Self::borrow_state(node);
                let applied_index = state.get_applied_index();
                let nodefmt = Self::format_node(node);
                output.push_str(&format!("{nodefmt} applied={applied_index}\n"));

                let raw = state.read(TestCommand::Scan.encode()?)?;
                let kvs: Vec<(String, String)> = bincode::deserialize(&raw)?;
                for (key, value) in kvs {
                    output.push_str(&format!("{nodefmt} state {key}={value}\n"));
                }
            }
            Ok(())
        }

        /// Outputs status for the given nodes.
        fn status(&self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            for id in ids {
                let node = self.nodes.get(id).ok_or(format!("unknown node {id}"))?;
                let (last_index, last_term) = Self::borrow_log(node).get_last_index();
                let (commit_index, commit_term) = Self::borrow_log(node).get_commit_index();
                let apply_index = Self::borrow_state(node).get_applied_index();
                output.push_str(&format!(
                    "{} last={last_index}@{last_term} commit={commit_index}@{commit_term} apply={apply_index}",
                    Self::format_node_role(node)
                ));
                if let Node::Leader(leader) = node {
                    output.push_str(&format!(
                        " progress={{{}}}",
                        leader
                            .role
                            .progress
                            .iter()
                            .sorted_by_key(|(id, _)| *id)
                            .map(|(id, pr)| format!("{id}:{}→{}", pr.last, pr.next))
                            .join(" ")
                    ))
                }
                output.push('\n');
            }
            Ok(())
        }

        /// Applies a node transition (typically a step or tick).
        fn transition<F: FnOnce(Node) -> crate::error::Result<Node>>(
            &mut self,
            id: NodeID,
            f: F,
            output: &mut String,
        ) -> Result<(), Box<dyn Error>> {
            self.transition_catch(id, f, false, output)
        }

        /// Applies a node transition, catching panics if requested.
        fn transition_catch<F: FnOnce(Node) -> crate::error::Result<Node>>(
            &mut self,
            id: NodeID,
            f: F,
            catch_unwind: bool,
            output: &mut String,
        ) -> Result<(), Box<dyn Error>> {
            let mut node = self.nodes.remove(&id).ok_or(format!("unknown node {id}"))?;
            let nodefmt = Self::format_node(&node);

            let old_noderole = Self::format_node_role(&node);
            let old_commit_index = Self::borrow_log(&node).get_commit_index().0;
            let old_last_index = Self::borrow_log(&node).get_last_index().0;

            // Apply the transition and handle panics. We mark it as
            // AssertUnwindSafe since we've removed the node from the nodes map,
            // so any future node access will fail anyway if we panic.
            node = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(node))) {
                Ok(result) => result?,
                Err(panic) if !catch_unwind => std::panic::resume_unwind(panic),
                Err(panic) => {
                    let message = panic
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| std::panic::resume_unwind(panic));
                    output.push_str(&format!("{nodefmt} panic: {message}\n"));
                    return Ok(());
                }
            };

            let nodefmt = Self::format_node(&node);
            let noderole = Self::format_node_role(&node);

            let log = Self::borrow_log_mut(&mut node);
            let (commit_index, commit_term) = log.get_commit_index();
            let appended: Vec<_> =
                log.scan(old_last_index + 1..)?.collect::<crate::error::Result<_>>()?;

            self.nodes.insert(id, node);

            // Print term/role changes.
            if old_noderole != noderole {
                output.push_str(&format!("{old_noderole} ⇨ {noderole}\n"))
            }

            // Print appended entries.
            for entry in appended {
                output.push_str(&format!("{nodefmt} append {}\n", Self::format_entry(&entry)))
            }

            // Print commit index changes.
            if old_commit_index != commit_index {
                output.push_str(&format!("{nodefmt} commit {commit_index}@{commit_term}\n"))
            }

            // Print applied entries.
            for entry in self.applied_rx[&id].try_iter() {
                output.push_str(&format!("{nodefmt} apply {}\n", Self::format_entry(&entry)))
            }

            // Receive outbound messages resulting from the transition.
            self.receive(id, output)?;

            Ok(())
        }

        /// Parses node IDs from the given argument values. Errors on key/value
        /// arguments. Can take both [Argument] and [&Argument].
        fn parse_ids<A>(&self, args: &[A]) -> Result<Vec<NodeID>, Box<dyn Error>>
        where
            A: Borrow<goldenscript::Argument>,
        {
            let mut ids = Vec::new();
            for arg in args.iter().map(|a| a.borrow()) {
                if let Some(key) = &arg.key {
                    return Err(format!("unknown argument '{key}'").into());
                }
                let id = arg.parse()?;
                if !self.nodes.contains_key(&id) {
                    return Err(format!("unknown node {id}").into());
                }
                ids.push(id)
            }
            Ok(ids)
        }

        // Parses node IDs from the given argument values, or returns all node
        // IDs if none were given.
        fn parse_ids_or_all<A>(&self, args: &[A]) -> Result<Vec<NodeID>, Box<dyn Error>>
        where
            A: Borrow<goldenscript::Argument>,
        {
            let mut ids = self.parse_ids(args)?;
            if ids.is_empty() {
                ids = self.ids.clone();
            }
            Ok(ids)
        }

        // Parses node IDs from the given argument values. Errors if no IDs were
        // given.
        fn parse_ids_or_error<A>(&self, args: &[A]) -> Result<Vec<NodeID>, Box<dyn Error>>
        where
            A: Borrow<goldenscript::Argument>,
        {
            let ids = self.parse_ids(args)?;
            if ids.is_empty() {
                return Err("node ID not given".into());
            }
            Ok(ids)
        }

        /// Formats network partitions.
        fn format_disconnected(disconnected: &HashMap<NodeID, HashSet<NodeID>>) -> String {
            // Return early if the cluster is fully connected.
            if disconnected.iter().all(|(_, peers)| peers.is_empty()) {
                return format!(
                    "{} fully connected",
                    disconnected.keys().sorted().map(|id| format!("n{id}")).join(" ")
                );
            }

            let mut output = String::new();

            // Separate symmetric and asymmetric partitions.
            let mut symmetric: HashMap<NodeID, HashSet<NodeID>> = HashMap::new();
            let mut asymmetric: HashMap<NodeID, HashSet<NodeID>> = HashMap::new();
            for (id, peers) in disconnected {
                for peer in peers {
                    if disconnected[peer].contains(id) {
                        symmetric.entry(*id).or_default().insert(*peer);
                    } else {
                        asymmetric.entry(*id).or_default().insert(*peer);
                    }
                }
            }

            // Anchor the symmetric partitions at the node with the largest number
            // of disconnects, otherwise the smallest (first) ID.
            for (id, peers) in &symmetric.clone() {
                for peer in peers {
                    // Recompute the peer set sizes for each iteration, since we
                    // modify the peer set below.
                    let len = symmetric.get(id).map(|p| p.len()).unwrap_or_default();
                    let peer_len = symmetric.get(peer).map(|p| p.len()).unwrap_or_default();
                    if len < peer_len || len == peer_len && id < peer {
                        let Some(peers) = symmetric.get_mut(id) else {
                            continue;
                        };
                        peers.remove(peer);
                        if peers.is_empty() {
                            symmetric.remove(id);
                        }
                    }
                }
            }

            // The values (HashSets) correspond to the RHS of a partition. Let's
            // group the LHS of the partition as well, from smallest to largest,
            // separately for symmetric and asymmetric partitions. The vector
            // contains (LHS, RHS, symmetric) groupings for each partition.
            let mut grouped: Vec<(HashSet<NodeID>, HashSet<NodeID>, bool)> = Vec::new();
            'outer: for (id, peers, symm) in symmetric
                .into_iter()
                .map(|(i, p)| (i, p, true))
                .chain(asymmetric.into_iter().map(|(i, p)| (i, p, false)))
                .sorted_by_key(|(id, _, symm)| (*id, !symm))
            {
                // Look for an existing LHS group with the same RHS.
                for (lhs, rhs, s) in grouped.iter_mut() {
                    if peers == *rhs && symm == *s {
                        lhs.insert(id);
                        continue 'outer;
                    }
                }
                // If we didn't find a match above, append a new LHS group.
                grouped.push((HashSet::from([id]), peers, symm))
            }

            // Display the groups.
            for (lhs, rhs, symm) in grouped {
                output.push_str(&format!(
                    "{} {} {}\n",
                    lhs.iter().sorted().map(|id| format!("n{id}")).join(" "),
                    if symm { '⇹' } else { '⇥' },
                    rhs.iter().sorted().map(|id| format!("n{id}")).join(" "),
                ))
            }

            output
        }

        /// Formats an entry.
        fn format_entry(entry: &Entry) -> String {
            let command = match entry.command.as_ref() {
                Some(raw) => TestCommand::decode(raw).expect("invalid command").to_string(),
                None => "None".to_string(),
            };
            format!("{}@{} {command}", entry.index, entry.term)
        }

        /// Formats a message.
        fn format_message(msg: &Message) -> String {
            match msg {
                Message::Campaign { last_index, last_term } => {
                    format!("Campaign last={last_index}@{last_term}")
                }
                Message::CampaignResponse { vote } => {
                    format!("CampaignResponse vote={vote}")
                }
                Message::Heartbeat { commit_index, commit_term, read_seq } => {
                    format!("Heartbeat commit={commit_index}@{commit_term} read_seq={read_seq}")
                }
                Message::HeartbeatResponse { last_index, last_term, read_seq } => {
                    format!("HeartbeatResponse last={last_index}@{last_term} read_seq={read_seq}")
                }
                Message::Append { base_index, base_term, entries } => {
                    format!(
                        "Append base={base_index}@{base_term} [{}]",
                        entries.iter().map(|e| format!("{}@{}", e.index, e.term)).join(" ")
                    )
                }
                Message::AppendResponse { reject, last_index, last_term } => {
                    format!("AppendResponse last={last_index}@{last_term} reject={reject}")
                }
                Message::ClientRequest { id, request } => {
                    format!(
                        "ClientRequest id=0x{} {}",
                        hex::encode(id),
                        match request {
                            Request::Read(v) => format!("read 0x{}", hex::encode(v)),
                            Request::Write(v) => format!("write 0x{}", hex::encode(v)),
                            Request::Status => "status".to_string(),
                        }
                    )
                }
                Message::ClientResponse { id, response } => {
                    format!(
                        "ClientResponse id=0x{} {}",
                        hex::encode(id),
                        match response {
                            Ok(Response::Read(v)) => format!("read 0x{}", hex::encode(v)),
                            Ok(Response::Write(v)) => format!("write 0x{}", hex::encode(v)),
                            Ok(Response::Status(v)) => format!("status {v:?}"),
                            Err(e) => format!("Error::{e:#?}"),
                        }
                    )
                }
            }
        }

        /// Formats a node identifier.
        fn format_node(node: &Node) -> String {
            format!("n{}@{}", node.id(), node.term())
        }

        /// Formats a node identifier with role.
        fn format_node_role(node: &Node) -> String {
            let role = match node {
                Node::Candidate(_) => "candidate".to_string(),
                Node::Follower(node) => {
                    format!(
                        "follower({})",
                        node.role.leader.map(|id| format!("n{id}")).unwrap_or_default()
                    )
                }
                Node::Leader(_) => "leader".to_string(),
            };
            format!("{} {role}", Self::format_node(node))
        }

        /// Formats a request.
        fn format_request(request: &Request) -> String {
            match request {
                Request::Read(c) | Request::Write(c) => TestCommand::decode(c).unwrap().to_string(),
                Request::Status => "status".to_string(),
            }
        }

        /// Formats a response.
        fn format_response(response: &crate::error::Result<Response>) -> String {
            match response {
                Ok(Response::Read(r) | Response::Write(r)) => {
                    TestResponse::decode(r).unwrap().to_string()
                }
                Ok(Response::Status(status)) => format!("{status:#?}"),
                Err(e) => format!("Error::{e:?} ({e})"),
            }
        }

        /// Strike-through formats the given string using a Unicode combining stroke.
        fn format_strikethrough(s: &str) -> String {
            s.chars().flat_map(|c| [c, '\u{0336}']).collect()
        }

        /// Returns a borrow for a node's log.
        fn borrow_log(node: &Node) -> &'_ Log {
            match node {
                Node::Candidate(n) => &n.log,
                Node::Follower(n) => &n.log,
                Node::Leader(n) => &n.log,
            }
        }

        /// Returns a mutable borrow for a node's log.
        fn borrow_log_mut(node: &mut Node) -> &'_ mut Log {
            match node {
                Node::Candidate(n) => &mut n.log,
                Node::Follower(n) => &mut n.log,
                Node::Leader(n) => &mut n.log,
            }
        }

        /// Returns a borrow for a node's state machine.
        fn borrow_state(node: &Node) -> &'_ dyn State {
            match node {
                Node::Candidate(n) => n.state.as_ref(),
                Node::Follower(n) => n.state.as_ref(),
                Node::Leader(n) => n.state.as_ref(),
            }
        }
    }

    /// A test state machine which stores key/value pairs. See TestCommand.
    struct TestState {
        /// The current applied index.
        applied_index: Index,
        /// The stored data.
        data: BTreeMap<String, String>,
        /// Sends applied entries, for output.
        applied_tx: Sender<Entry>,
    }

    impl TestState {
        fn new(applied_tx: Sender<Entry>) -> Self {
            Self { applied_index: 0, data: BTreeMap::new(), applied_tx }
        }
    }

    impl State for TestState {
        fn get_applied_index(&self) -> Index {
            self.applied_index
        }

        fn apply(&mut self, entry: Entry) -> crate::error::Result<Vec<u8>> {
            let response = entry
                .command
                .as_deref()
                .map(TestCommand::decode)
                .transpose()?
                .map(|c| match c {
                    TestCommand::Put { key, value } => {
                        self.data.insert(key, value);
                        TestResponse::Put(entry.index)
                    }
                    TestCommand::Get { .. } => panic!("get submitted as write command"),
                    TestCommand::Scan => panic!("scan submitted as write command"),
                })
                .map_or(Ok(Vec::new()), |r| r.encode())?;
            self.applied_index = entry.index;
            self.applied_tx.send(entry)?;
            Ok(response)
        }

        fn read(&self, command: Vec<u8>) -> crate::error::Result<Vec<u8>> {
            let response = match TestCommand::decode(&command)? {
                TestCommand::Get { key } => TestResponse::Get(self.data.get(&key).cloned()),
                TestCommand::Scan => TestResponse::Scan(self.data.clone()),
                TestCommand::Put { .. } => panic!("put submitted as read command"),
            };
            response.encode()
        }
    }

    /// A TestState command. Each command returns a corresponding TestResponse.
    #[derive(Serialize, Deserialize)]
    enum TestCommand {
        /// Fetches the value of the given key.
        Get { key: String },
        /// Stores the given key/value pair, returning the applied index.
        Put { key: String, value: String },
        /// Returns all key/value pairs.
        Scan,
    }

    impl std::fmt::Display for TestCommand {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Get { key } => write!(f, "get {key}"),
                Self::Put { key, value } => write!(f, "put {key}={value}"),
                Self::Scan => write!(f, "scan"),
            }
        }
    }

    impl TestCommand {
        fn decode(raw: &[u8]) -> crate::error::Result<Self> {
            bincode::deserialize(raw)
        }

        fn encode(&self) -> crate::error::Result<Vec<u8>> {
            bincode::serialize(self)
        }
    }

    /// A TestCommand response.
    #[derive(Serialize, Deserialize)]
    enum TestResponse {
        /// The value for the TestCommand::Get key, or None if it does not exist.
        Get(Option<String>),
        /// The applied index of a TestCommand::Put command.
        Put(Index),
        /// The scanned key/value pairs.
        Scan(BTreeMap<String, String>),
    }

    impl std::fmt::Display for TestResponse {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Get(Some(value)) => write!(f, "{value}")?,
                Self::Get(None) => write!(f, "None")?,
                Self::Put(applied_index) => write!(f, "{applied_index}")?,
                Self::Scan(scan) => {
                    write!(f, "{}", scan.iter().map(|(k, v)| format!("{k}={v}")).join(","))?
                }
            };
            Ok(())
        }
    }

    impl TestResponse {
        fn decode(raw: &[u8]) -> crate::error::Result<Self> {
            bincode::deserialize(raw)
        }

        fn encode(&self) -> crate::error::Result<Vec<u8>> {
            bincode::serialize(self)
        }
    }
}
