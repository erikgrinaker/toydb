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

/// Raft node options.
pub struct Options {
    /// The number of ticks between leader heartbeats.
    pub heartbeat_interval: Ticks,
    /// The range of randomized election timeouts for followers and candidates.
    pub election_timeout_range: std::ops::Range<Ticks>,
    /// Maximum number of entries to send in a single Append message.
    pub max_append_entries: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            heartbeat_interval: super::HEARTBEAT_INTERVAL,
            election_timeout_range: super::ELECTION_TIMEOUT_RANGE,
            max_append_entries: super::MAX_APPEND_ENTRIES,
        }
    }
}

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
        opts: Options,
    ) -> Result<Self> {
        let node = RawNode::new(id, peers, log, state, node_tx, opts)?;
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
            Node::Candidate(n) => n.term(),
            Node::Follower(n) => n.term(),
            Node::Leader(n) => n.term(),
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
pub trait Role {}

/// A Raft node with the concrete role R.
///
/// This implements the typestate pattern, where individual node states (roles)
/// are encoded as RawNode<Role>. See: http://cliffle.com/blog/rust-typestate/
pub struct RawNode<R: Role = Follower> {
    id: NodeID,
    peers: HashSet<NodeID>,
    log: Log,
    state: Box<dyn State>,
    node_tx: crossbeam::channel::Sender<Envelope>,
    opts: Options,
    role: R,
}

impl<R: Role> RawNode<R> {
    /// Helper for role transitions.
    fn into_role<T: Role>(self, role: T) -> RawNode<T> {
        RawNode {
            id: self.id,
            peers: self.peers,
            log: self.log,
            state: self.state,
            node_tx: self.node_tx,
            opts: self.opts,
            role,
        }
    }

    /// Returns the node's current term. Convenience wrapper for Log.get_term().
    fn term(&self) -> Term {
        self.log.get_term().0
    }

    /// Returns the cluster size as number of nodes.
    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// Returns the cluster quorum size (strict majority).
    fn quorum_size(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    /// Returns the quorum value of the given unsorted vector, in descending
    /// order. The slice must have the same size as the cluster.
    fn quorum_value<T: Ord + Copy>(&self, mut values: Vec<T>) -> T {
        assert_eq!(values.len(), self.cluster_size(), "vector size must match cluster size");
        *values.select_nth_unstable_by(self.quorum_size() - 1, |a, b: &T| a.cmp(b).reverse()).1
    }

    /// Sends a message.
    fn send(&self, to: NodeID, message: Message) -> Result<()> {
        Self::send_with(&self.node_tx, Envelope { from: self.id, to, term: self.term(), message })
    }

    /// Sends a message without borrowing self, to allow partial borrows.
    fn send_with(tx: &crossbeam::channel::Sender<Envelope>, msg: Envelope) -> Result<()> {
        debug!("Sending {msg:?}");
        Ok(tx.send(msg)?)
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
        rand::thread_rng().gen_range(self.opts.election_timeout_range.clone())
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
        assert_ne!(self.term(), 0, "candidates can't have term 0");
        assert!(self.role.votes.contains(&self.id), "candidate did not vote for self");
        debug_assert_eq!(Some(self.id), self.log.get_term().1, "log vote does not match self");

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
        assert!(term >= self.term(), "term regression {} → {}", self.term(), term);

        let election_timeout = self.gen_election_timeout();
        if let Some(leader) = leader {
            // We lost the election, follow the winner.
            assert_eq!(term, self.term(), "can't follow leader in different term");
            info!("Lost election, following leader {} in term {}", leader, term);
            Ok(self.into_role(Follower::new(Some(leader), election_timeout)))
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term(), "can't be leaderless follower in current term");
            info!("Discovered new term {}", term);
            self.log.set_term(term, None)?;
            Ok(self.into_role(Follower::new(None, election_timeout)))
        }
    }

    /// Transitions the candidate to a leader. We won the election.
    fn into_leader(self) -> Result<RawNode<Leader>> {
        info!("Won election for term {}, becoming leader", self.term());
        let peers = self.peers.clone();
        let (last_index, _) = self.log.get_last_index();
        let mut node = self.into_role(Leader::new(peers, last_index));

        // Propose an empty command when assuming leadership, to disambiguate
        // previous entries in the log. See section 8 in the Raft paper.
        //
        // We do this prior to the heartbeat, to avoid a wasted replication
        // roundtrip if the heartbeat response indicates the peer is behind.
        node.propose(None)?;
        node.maybe_commit_and_apply()?;
        node.heartbeat()?;

        Ok(node)
    }

    /// Processes a message.
    fn step(mut self, msg: Envelope) -> Result<Node> {
        self.assert()?;
        self.assert_step(&msg);

        // Drop messages from past terms.
        if msg.term < self.term() {
            debug!("Dropping message from past term ({:?})", msg);
            return Ok(self.into());
        }

        // If we receive a message for a future term, become a leaderless
        // follower in it and step the message. If the message is a Heartbeat or
        // Append from the leader, stepping it will follow the leader.
        if msg.term > self.term() {
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
                if self.role.votes.len() >= self.quorum_size() {
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
        let term = self.term() + 1;
        info!("Starting new election for term {term}");
        self.role = Candidate::new(self.gen_election_timeout());
        self.role.votes.insert(self.id); // vote for ourself
        self.log.set_term(term, Some(self.id))?;

        let (last_index, last_term) = self.log.get_last_index();
        self.broadcast(Message::Campaign { last_index, last_term })?;
        Ok(())
    }
}

// A follower replicates state from a leader.
pub struct Follower {
    /// The leader, or None if just initialized.
    leader: Option<NodeID>,
    /// The number of ticks since the last message from the leader.
    leader_seen: Ticks,
    /// The leader_seen timeout before triggering an election.
    election_timeout: Ticks,
    // Local client requests that have been forwarded to the leader. These are
    // aborted on leader/term changes.
    forwarded: HashSet<RequestID>,
}

impl Follower {
    /// Creates a new follower role.
    fn new(leader: Option<NodeID>, election_timeout: Ticks) -> Self {
        Self { leader, leader_seen: 0, election_timeout, forwarded: HashSet::new() }
    }
}

impl Role for Follower {}

impl RawNode<Follower> {
    /// Creates a new node as a leaderless follower.
    fn new(
        id: NodeID,
        peers: HashSet<NodeID>,
        log: Log,
        state: Box<dyn State>,
        node_tx: crossbeam::channel::Sender<Envelope>,
        opts: Options,
    ) -> Result<Self> {
        let role = Follower::new(None, 0);
        let mut node = Self { id, peers, log, state, node_tx, opts, role };
        node.role.election_timeout = node.gen_election_timeout();
        Ok(node)
    }

    /// Asserts internal invariants.
    fn assert(&mut self) -> Result<()> {
        if let Some(leader) = self.role.leader {
            assert_ne!(leader, self.id, "Can't follow self");
            assert!(self.peers.contains(&leader), "Leader not in peers");
            assert_ne!(self.term(), 0, "Followers with leaders can't have term 0");
        } else {
            assert!(self.role.forwarded.is_empty(), "Leaderless follower has forwarded requests");
        }
        assert!(self.role.leader_seen < self.role.election_timeout, "Election timeout passed");

        // NB: We allow vote not in peers, since this can happen when removing
        // nodes from the cluster via a cold restart. We also allow vote for
        // self, which can happen if we lose an election.

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

    /// Transitions the follower into a follower, either a leaderless follower
    /// in a new term (e.g. if someone holds a new election) or following a
    /// leader in the current term once someone wins the election.
    fn into_follower(mut self, leader: Option<NodeID>, term: Term) -> Result<RawNode<Follower>> {
        assert!(term >= self.term(), "term regression {} → {}", self.term(), term);

        // Abort any forwarded requests. These must be retried with new leader.
        self.abort_forwarded()?;

        if let Some(leader) = leader {
            // We found a leader in the current term.
            assert_eq!(self.role.leader, None, "Already have leader in term");
            assert_eq!(term, self.term(), "Can't follow leader in different term");
            info!("Following leader {leader} in term {term}");
            self.role = Follower::new(Some(leader), self.role.election_timeout);
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term(), "can't be leaderless follower in current term");
            info!("Discovered new term {term}");
            self.log.set_term(term, None)?;
            self.role = Follower::new(None, self.gen_election_timeout());
        }
        Ok(self)
    }

    /// Processes a message.
    fn step(mut self, msg: Envelope) -> Result<Node> {
        self.assert()?;
        self.assert_step(&msg);

        // Drop messages from past terms.
        if msg.term < self.term() {
            debug!("Dropping message from past term ({:?})", msg);
            return Ok(self.into());
        }

        // If we receive a message for a future term, become a leaderless
        // follower in it and step the message. If the message is a Heartbeat or
        // Append from the leader, stepping it will follow the leader.
        if msg.term > self.term() {
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
            Message::Heartbeat { last_index, commit_index, read_seq } => {
                assert!(commit_index <= last_index, "commit_index after last_index");

                // Check that the heartbeat is from our leader.
                match self.role.leader {
                    Some(leader) => assert_eq!(msg.from, leader, "multiple leaders in term"),
                    None => self = self.into_follower(Some(msg.from), msg.term)?,
                }

                // Attempt to match the leader's log and respond to the
                // heartbeat. last_index always has the leader's term.
                let match_index = if self.log.has(last_index, msg.term)? { last_index } else { 0 };
                self.send(msg.from, Message::HeartbeatResponse { match_index, read_seq })?;

                // Advance commit index and apply entries. We can only do this
                // if the last_index matches the leader, which implies that
                // the logs are identical up to match_index. This also implies
                // that the commit_index is present in our log.
                if match_index != 0 && commit_index > self.log.get_commit_index().0 {
                    self.log.commit(commit_index)?;
                    self.maybe_apply()?;
                }
            }

            // Append log entries from the leader to the local log.
            Message::Append { base_index, base_term, entries } => {
                if let Some(first) = entries.first() {
                    assert_eq!(base_index, first.index - 1, "base index mismatch");
                }

                // Make sure the message comes from our leader.
                match self.role.leader {
                    Some(leader) => assert_eq!(msg.from, leader, "multiple leaders in term"),
                    None => self = self.into_follower(Some(msg.from), msg.term)?,
                }

                // If the base entry is in our log, append the entries.
                let (mut reject_index, mut match_index) = (0, 0);
                if base_index == 0 || self.log.has(base_index, base_term)? {
                    match_index = entries.last().map(|e| e.index).unwrap_or(base_index);
                    self.log.splice(entries)?;
                } else {
                    // Otherwise, reject the base index. If the local log is
                    // shorter than the base index, lower the reject index to
                    // skip all the missing entries.
                    reject_index = std::cmp::min(base_index, self.log.get_last_index().0 + 1);
                }
                self.send(msg.from, Message::AppendResponse { reject_index, match_index })?;
            }

            // A candidate in this term is requesting our vote.
            Message::Campaign { last_index, last_term } => {
                // Don't vote if we already voted for someone else in this term.
                if let (_, Some(vote)) = self.log.get_term() {
                    if msg.from != vote {
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
                info!("Voting for {} in term {} election", msg.from, msg.term);
                self.log.set_term(msg.term, Some(msg.from))?;
                self.send(msg.from, Message::CampaignResponse { vote: true })?;
            }

            // We may receive a vote after we lost an election and followed a
            // different leader. Ignore it.
            Message::CampaignResponse { .. } => {}

            // Forward client requests to the leader, or abort them if there is
            // none (the client must retry).
            Message::ClientRequest { id, .. } => {
                assert_eq!(msg.from, self.id, "Client request from other node");

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

    /// Applies any pending log entries.
    fn maybe_apply(&mut self) -> Result<()> {
        let mut iter = self.log.scan_apply(self.state.get_applied_index())?;
        while let Some(entry) = iter.next().transpose()? {
            debug!("Applying {entry:?}");
            // Throw away the result, since there is no client waiting for it.
            // This includes errors -- any non-deterministic errors (e.g. IO
            // errors) must panic instead to avoid replica divergence.
            _ = self.state.apply(entry);
        }
        Ok(())
    }

    /// Checks if an address is the current leader.
    fn is_leader(&self, from: NodeID) -> bool {
        self.role.leader == Some(from)
    }
}

/// Follower replication progress.
struct Progress {
    /// The next index to replicate to the follower.
    next_index: Index,
    /// The last index where the follower's log matches the leader.
    match_index: Index,
    /// The last read sequence number confirmed by the peer.
    read_seq: ReadSequence,
}

impl Progress {
    /// Attempts to advance a follower's match index, returning true if it did.
    /// If next_index is below it, it is advanced to the following index, but
    /// is otherwise left as is to avoid regressing it unnecessarily.
    fn advance(&mut self, match_index: Index) -> bool {
        if match_index <= self.match_index {
            return false;
        }
        self.match_index = match_index;
        self.next_index = std::cmp::max(self.next_index, match_index + 1);
        true
    }

    /// Attempts to advance a follower's read_seq, returning true if it did.
    fn advance_read(&mut self, read_seq: ReadSequence) -> bool {
        if read_seq <= self.read_seq {
            return false;
        }
        self.read_seq = read_seq;
        true
    }

    /// Regresses the next index to the given index, if it's currently greater.
    /// Can't regress below match_index + 1. Returns true if next_index changes.
    fn regress_next(&mut self, next_index: Index) -> bool {
        if next_index >= self.next_index || self.next_index <= self.match_index + 1 {
            return false;
        }
        self.next_index = std::cmp::max(next_index, self.match_index + 1);
        true
    }
}

/// A pending client write request.
struct Write {
    /// The node which submitted the write.
    from: NodeID,
    /// The write request ID.
    id: RequestID,
}

/// A pending client read request.
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
pub struct Leader {
    /// Follower replication progress.
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
        let next_index = last_index + 1;
        let progress = peers
            .into_iter()
            .map(|p| (p, Progress { next_index, match_index: 0, read_seq: 0 }))
            .collect();
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
        assert_ne!(self.term(), 0, "leaders can't have term 0");
        debug_assert_eq!(Some(self.id), self.log.get_term().1, "vote does not match self");
        Ok(())
    }

    /// Transitions the leader into a follower. This can only happen if we
    /// discover a new term, so we become a leaderless follower. Subsequently
    /// stepping the received message may discover the leader, if there is one.
    fn into_follower(mut self, term: Term) -> Result<RawNode<Follower>> {
        assert!(term >= self.term(), "term regression {} → {}", self.term(), term);
        assert!(term > self.term(), "can only become follower in later term");

        info!("Discovered new term {term}");

        // Cancel in-flight requests.
        for write in std::mem::take(&mut self.role.writes).into_values().sorted_by_key(|w| w.id) {
            self.send(
                write.from,
                Message::ClientResponse { id: write.id, response: Err(Error::Abort) },
            )?;
        }
        for read in std::mem::take(&mut self.role.reads).into_iter().sorted_by_key(|r| r.id) {
            self.send(
                read.from,
                Message::ClientResponse { id: read.id, response: Err(Error::Abort) },
            )?;
        }

        self.log.set_term(term, None)?;
        let election_timeout = self.gen_election_timeout();
        Ok(self.into_role(Follower::new(None, election_timeout)))
    }

    /// Processes a message.
    fn step(mut self, msg: Envelope) -> Result<Node> {
        self.assert()?;
        self.assert_step(&msg);

        // Drop messages from past terms.
        if msg.term < self.term() {
            debug!("Dropping message from past term ({:?})", msg);
            return Ok(self.into());
        }

        // If we receive a message for a future term, become a leaderless
        // follower in it and step the message. If the message is a Heartbeat or
        // Append from the leader, stepping it will follow the leader.
        if msg.term > self.term() {
            return self.into_follower(msg.term)?.step(msg);
        }

        match msg.message {
            // There can't be two leaders in the same term.
            Message::Heartbeat { .. } | Message::Append { .. } => {
                panic!("saw other leader {} in term {}", msg.from, msg.term);
            }

            // A follower received our heartbeat and confirms our leadership.
            Message::HeartbeatResponse { match_index, read_seq } => {
                let (last_index, _) = self.log.get_last_index();
                assert!(match_index <= last_index, "future match index");
                assert!(read_seq <= self.role.read_seq, "future read sequence number");

                // If the read sequence number advances, try to execute reads.
                if self.progress(msg.from).advance_read(read_seq) {
                    self.maybe_read()?;
                }

                if match_index == 0 {
                    // If the follower didn't match our last index, an append to
                    // it must have failed (or it's catching up). Probe it to
                    // discover a matching entry and start replicating. Move
                    // next_index back to last_index since the follower just
                    // told us it doesn't have it.
                    self.progress(msg.from).regress_next(last_index);
                    self.maybe_send_append(msg.from, true)?;
                } else if self.progress(msg.from).advance(match_index) {
                    // If the follower's match index advanced, an append
                    // response got lost. Try to commit.
                    //
                    // We don't need to eagerly send any pending entries, since
                    // any proposals made after this heartbeat was sent should
                    // have been eagerly replicated in steady state. If not, the
                    // next heartbeat will trigger a probe above.
                    self.maybe_commit_and_apply()?;
                }
            }

            // A follower appended our log entries. Record its progress and
            // attempt to commit.
            Message::AppendResponse { match_index, reject_index: 0 } if match_index > 0 => {
                let (last_index, _) = self.log.get_last_index();
                assert!(match_index <= last_index, "follower matched unknown index");

                if self.progress(msg.from).advance(match_index) {
                    self.maybe_commit_and_apply()?;
                }

                // Eagerly send any further pending entries. The peer may be
                // lagging behind the leader, and we're catching it up one
                // MAX_APPEND_ENTRIES batch at a time. Or we may have received a
                // probe response at the known match index, in which case
                // advance() above would have returned false.
                self.maybe_send_append(msg.from, false)?;
            }

            // A follower rejected the log entries because the base entry in
            // reject_index did not match its log. Try a previous entry until we
            // find a common base.
            //
            // This linear probing can be slow with long divergent logs, but we
            // keep it simple.
            Message::AppendResponse { reject_index, match_index: 0 } if reject_index > 0 => {
                let (last_index, _) = self.log.get_last_index();
                assert!(reject_index <= last_index, "follower rejected unknown index");

                // If the rejected base index is at or below the match index,
                // the rejection is stale and can be ignored.
                if reject_index <= self.progress(msg.from).match_index {
                    return Ok(self.into());
                }

                // Probe below the reject index, if we haven't already moved
                // next_index below it.
                if self.progress(msg.from).regress_next(reject_index) {
                    self.maybe_send_append(msg.from, true)?;
                }
            }

            Message::AppendResponse { .. } => panic!("invalid message {msg:?}"),

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
                self.role.writes.insert(index, Write { from: msg.from, id });
                if self.peers.is_empty() {
                    self.maybe_commit_and_apply()?;
                }
            }

            Message::ClientRequest { id, request: Request::Status } => {
                let status = Status {
                    leader: self.id,
                    term: self.term(),
                    match_index: self
                        .role
                        .progress
                        .iter()
                        .map(|(id, p)| (*id, p.match_index))
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
        if self.role.since_heartbeat >= self.opts.heartbeat_interval {
            self.heartbeat()?;
            self.role.since_heartbeat = 0;
        }
        Ok(self.into())
    }

    /// Broadcasts a heartbeat to all peers.
    fn heartbeat(&mut self) -> Result<()> {
        let (last_index, last_term) = self.log.get_last_index();
        let (commit_index, _) = self.log.get_commit_index();
        let read_seq = self.role.read_seq;

        assert_eq!(last_term, self.term(), "leader has stale last_term");

        self.broadcast(Message::Heartbeat { last_index, commit_index, read_seq })?;
        // NB: We don't reset self.since_heartbeat here, because we want to send
        // periodic heartbeats regardless of any on-demand heartbeats.
        Ok(())
    }

    /// Returns a mutable borrow of a node's progress.
    fn progress(&mut self, id: NodeID) -> &mut Progress {
        self.role.progress.get_mut(&id).expect("unknown node")
    }

    /// Proposes a command for consensus by appending it to our log and
    /// replicating it to peers. If successful, it will eventually be committed
    /// and applied to the state machine.
    fn propose(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        let index = self.log.append(command)?;
        for peer in self.peers.iter().copied().sorted() {
            // Eagerly send the entry to the peer, but only if it is in steady
            // state where we've already sent the previous entries. Otherwise,
            // we're probing a lagging or divergent peer, and there's no point
            // sending this entry since it will likely be rejected.
            if index == self.progress(peer).next_index {
                self.maybe_send_append(peer, false)?;
            }
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
                .map(|p| p.match_index)
                .chain(std::iter::once(self.log.get_last_index().0))
                .collect(),
        );

        // If the commit index doesn't advance, do nothing. We don't assert on
        // this, since the quorum value may regress e.g. following a restart or
        // leader change where followers are initialized with log index 0.
        let (mut commit_index, old_commit_term) = self.log.get_commit_index();
        if quorum_index <= commit_index {
            return Ok(commit_index);
        }

        // We can only safely commit an entry from our own term (see figure 8 in
        // Raft paper).
        commit_index = match self.log.get(quorum_index)? {
            Some(entry) if entry.term == self.term() => quorum_index,
            Some(_) => return Ok(commit_index),
            None => panic!("missing commit index {quorum_index} missing"),
        };

        // Commit the new entries.
        self.log.commit(commit_index)?;

        // Apply entries and respond to client writers.
        let term = self.term();
        let mut iter = self.log.scan_apply(self.state.get_applied_index())?;
        while let Some(entry) = iter.next().transpose()? {
            debug!("Applying {entry:?}");
            let write = self.role.writes.remove(&entry.index);
            let result = self.state.apply(entry);

            if let Some(Write { id, from: to }) = write {
                let message = Message::ClientResponse { id, response: result.map(Response::Write) };
                Self::send_with(&self.node_tx, Envelope { from: self.id, term, to, message })?;
            }
        }
        drop(iter);

        // If the commit term changed, there may be pending reads waiting for us
        // to commit an entry from our own term. Execute them.
        if old_commit_term != self.term() {
            self.maybe_read()?;
        }

        Ok(commit_index)
    }

    /// Executes any pending read requests that are now ready after quorum
    /// confirmation of their sequence number.
    fn maybe_read(&mut self) -> Result<()> {
        if self.role.reads.is_empty() {
            return Ok(());
        }

        // It's only safe to read if we've committed and applied an entry from
        // our own term (the leader appends an entry when elected). Otherwise we
        // may be behind on application and serve stale reads, violating
        // linearizability.
        let (commit_index, commit_term) = self.log.get_commit_index();
        let applied_index = self.state.get_applied_index();
        if commit_term < self.term() || applied_index < commit_index {
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

    // Sends pending log entries to a peer, according to its next_index. Does
    // not send an append if the peer is already caught up. Sends an empty
    // append with the last entry as a base if all pending entries are already
    // in flight, to probe whether they've made it to the follower or not.
    fn maybe_send_append(&mut self, peer: NodeID, mut probe: bool) -> Result<()> {
        let (last_index, _) = self.log.get_last_index();
        let progress = self.role.progress.get_mut(&peer).expect("unknown node");
        assert_ne!(progress.next_index, 0, "invalid next_index");
        assert!(progress.next_index > progress.match_index, "invalid next_index <= match_index");
        assert!(progress.match_index <= last_index, "invalid match_index > last_index");
        assert!(progress.next_index <= last_index + 1, "invalid next_index > last_index + 1");

        // If the peer is already caught up, there's no point sending an append.
        if progress.match_index == last_index {
            return Ok(());
        }

        // If a probe was requested, but next_index is immediately after
        // match_index (even when 0), there is no point in probing because the
        // entry must be accepted. Send the entries instead.
        if probe && progress.next_index == progress.match_index + 1 {
            probe = false;
        }

        // If there are no pending entries and this is not a probe, there's
        // nothing more to send.
        if progress.next_index > last_index && !probe {
            return Ok(());
        }

        // Fetch the base and entries.
        let (base_index, base_term) = match progress.next_index {
            0 => panic!("next_index=0 for node {peer}"),
            1 => (0, 0),
            next => self.log.get(next - 1)?.map(|e| (e.index, e.term)).expect("missing base entry"),
        };

        let entries = if !probe {
            self.log
                .scan(progress.next_index..)?
                .take(self.opts.max_append_entries)
                .collect::<Result<_>>()?
        } else {
            Vec::new()
        };

        // Optimistically assume the entries will be accepted by the follower,
        // and bump the next_index to avoid resending them until a response.
        if let Some(last) = entries.last() {
            progress.next_index = last.index + 1;
        }

        debug!("Replicating {} entries with base {base_index} to {peer}", entries.len());
        self.send(peer, Message::Append { base_index, base_term, entries })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{bincode, Value as _};
    use crate::raft::state::test::{self as teststate, KVCommand, KVResponse};
    use crate::raft::{
        Entry, Request, RequestID, Response, ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL,
        MAX_APPEND_ENTRIES,
    };
    use crate::storage;
    use crossbeam::channel::Receiver;
    use std::borrow::Borrow;
    use std::collections::{HashMap, HashSet};
    use std::error::Error;
    use std::result::Result;
    use test_case::test_case;
    use test_each_file::test_each_path;

    /// Test helpers for RawNode.
    impl RawNode<Follower> {
        /// Creates a noop node, with a noop state machine and transport.
        fn new_noop(id: NodeID, peers: HashSet<NodeID>) -> Self {
            let log = Log::new(storage::Memory::new()).expect("log failed");
            let state = teststate::Noop::new();
            let (node_tx, _) = crossbeam::channel::unbounded();
            RawNode::new(id, peers, log, state, node_tx, Options::default()).expect("node failed")
        }
    }

    // Run goldenscript tests in src/raft/testscripts/node.
    test_each_path! { in "src/raft/testscripts/node" as scripts => test_goldenscript }

    fn test_goldenscript(path: &std::path::Path) {
        goldenscript::run(&mut TestRunner::new(), path).expect("goldenscript failed")
    }

    /// Tests RawNode.quorum_size() and cluster_size().
    #[test_case(1 => 1)]
    #[test_case(2 => 2)]
    #[test_case(3 => 2)]
    #[test_case(4 => 3)]
    #[test_case(5 => 3)]
    #[test_case(6 => 4)]
    #[test_case(7 => 4)]
    #[test_case(8 => 5)]
    fn quorum_size(size: usize) -> usize {
        let node = RawNode::new_noop(1, (2..=size as NodeID).collect());
        assert_eq!(node.cluster_size(), size);
        node.quorum_size()
    }

    /// Tests RawNode.quorum_value().
    #[test_case(vec![1] => 1)]
    #[test_case(vec![1,3,2] => 2)]
    #[test_case(vec![4,1,3,2] => 2)]
    #[test_case(vec![1,1,1,2,2] => 1)]
    #[test_case(vec![1,1,2,2,2] => 2)]
    fn quorum_value(values: Vec<i8>) -> i8 {
        let size = values.len();
        let node = RawNode::new_noop(1, (2..=size as NodeID).collect());
        assert_eq!(node.cluster_size(), size);
        node.quorum_value(values)
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
        /// Applied log entries for each node, after state machine application.
        applied_rx: HashMap<NodeID, Receiver<Entry>>,
        /// Network partitions, sender → receivers.
        disconnected: HashMap<NodeID, HashSet<NodeID>>,
        /// In-flight client requests.
        requests: HashMap<RequestID, Request>,
        /// The request ID to use for the next client request.
        next_request_id: u64,
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

                // cluster nodes=N [leader=ID] [heartbeat_interval=N] [election_timeout=N] [max_append_entries=N]
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
                    let max_append_entries =
                        args.lookup_parse("max_append_entries")?.unwrap_or(MAX_APPEND_ENTRIES);
                    args.reject_rest()?;
                    self.cluster(
                        nodes,
                        leader,
                        heartbeat_interval,
                        election_timeout,
                        max_append_entries,
                        &mut output,
                    )?;
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
                    let request = Request::Read(KVCommand::Get { key }.encode()?);
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
                    let request = Request::Write(KVCommand::Put { key, value }.encode()?);
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

                // step ID JSON
                //
                // Steps a manually generated JSON message on the given node.
                "step" => {
                    let mut args = command.consume_args();
                    let id = args.next_pos().ok_or("node ID not given")?.parse()?;
                    let raw = &args.next_pos().ok_or("message not given")?.value;
                    let msg = serde_json::from_str(raw)?;
                    args.reject_rest()?;
                    self.transition(id, |n| n.step(msg), &mut output)?;
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
                Node::Leader(node) => panic!("{} is a leader", node.id),
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
            max_append_entries: usize,
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
                let log = Log::new(storage::Memory::new())?;
                let state = teststate::Emit::new(teststate::KV::new(), applied_tx);
                let opts = Options {
                    heartbeat_interval,
                    election_timeout_range: election_timeout..election_timeout + 1,
                    max_append_entries,
                };
                self.nodes.insert(id, Node::new(id, peers, log, state, node_tx, opts)?);

                while applied_rx.try_recv().is_ok() {} // drain first apply

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
            let request_id = uuid::Uuid::from_u64_pair(0, self.next_request_id);
            self.next_request_id += 1;
            self.requests.insert(request_id, request.clone());

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

                let raw = state.read(KVCommand::Scan.encode()?)?;
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
                            .map(|(id, pr)| format!("{id}:{}→{}", pr.match_index, pr.next_index))
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
            let mut node = self.nodes.remove(&id).ok_or(format!("unknown node {id}"))?;

            let old_noderole = Self::format_node_role(&node);
            let log = Self::borrow_log_mut(&mut node);
            let old_commit_index = log.get_commit_index().0;
            let entries = log.scan(..)?.collect::<crate::error::Result<Vec<_>>>()?;

            // Apply the transition.
            node = f(node)?;

            let nodefmt = Self::format_node(&node);
            let noderole = Self::format_node_role(&node);

            let log = Self::borrow_log_mut(&mut node);
            let (commit_index, commit_term) = log.get_commit_index();
            let mut appended = log.scan(..)?.collect::<crate::error::Result<Vec<_>>>()?;
            match appended.iter().zip(entries.iter()).position(|(a, e)| a.term != e.term) {
                Some(i) => appended = appended[i..].to_vec(),
                None => appended = appended[entries.len()..].to_vec(),
            }

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
                Some(raw) => KVCommand::decode(raw).expect("invalid command").to_string(),
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
                Message::Heartbeat { last_index, commit_index, read_seq } => {
                    format!("Heartbeat last_index={last_index} commit_index={commit_index} read_seq={read_seq}")
                }
                Message::HeartbeatResponse { match_index, read_seq } => {
                    format!("HeartbeatResponse match_index={match_index} read_seq={read_seq}")
                }
                Message::Append { base_index, base_term, entries } => {
                    format!(
                        "Append base={base_index}@{base_term} [{}]",
                        entries.iter().map(|e| format!("{}@{}", e.index, e.term)).join(" ")
                    )
                }
                Message::AppendResponse { match_index, reject_index } => {
                    match (match_index, reject_index) {
                        (0, 0) => panic!("match_index and reject_index both 0"),
                        (match_index, 0) => format!("AppendResponse match_index={match_index}"),
                        (0, reject_index) => format!("AppendResponse reject_index={reject_index}"),
                        (_, _) => panic!("match_index and reject_index both non-zero"),
                    }
                }
                Message::ClientRequest { id, request } => {
                    format!(
                        "ClientRequest id=0x{} {}",
                        hex::encode(id).trim_start_matches("00"),
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
                        hex::encode(id).trim_start_matches("00"),
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
                Request::Read(c) | Request::Write(c) => KVCommand::decode(c).unwrap().to_string(),
                Request::Status => "status".to_string(),
            }
        }

        /// Formats a response.
        fn format_response(response: &crate::error::Result<Response>) -> String {
            match response {
                Ok(Response::Read(r) | Response::Write(r)) => {
                    KVResponse::decode(r).unwrap().to_string()
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
}
