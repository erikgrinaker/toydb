use std::cmp::{max, min};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Range;

use crossbeam::channel::Sender;
use itertools::Itertools as _;
use log::{debug, info};
use rand::Rng as _;

use super::log::{Index, Log};
use super::message::{Envelope, Message, ReadSequence, Request, RequestID, Response, Status};
use super::state::State;
use crate::errinput;
use crate::error::{Error, Result};

/// A node ID. Unique within a cluster. Assigned manually when started.
pub type NodeID = u8;

/// A leader term number. Increases monotonically.
pub type Term = u64;

/// A logical clock interval as number of ticks.
pub type Ticks = u8;

/// Raft node options.
#[derive(Clone, Debug, PartialEq)]
pub struct Options {
    /// The number of ticks between leader heartbeats.
    pub heartbeat_interval: Ticks,
    /// The range of randomized election timeouts for followers and candidates.
    pub election_timeout_range: Range<Ticks>,
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

/// A Raft node with a dynamic role. This implements the Raft distributed
/// consensus protocol, see the `raft` module documentation for more info.
///
/// The node is driven synchronously by processing inbound messages via `step()`
/// and by advancing time via `tick()`. These methods consume the node and
/// return a new one with a possibly different role. Outbound messages are sent
/// via the given `tx` channel, and must be delivered to peers or clients.
///
/// This enum is the public interface to the node, with a closed set of roles.
/// It wraps the `RawNode<Role>` types, which implement the actual node logic.
/// The enum allows ergonomic use across role transitions since it can represent
/// all roles, e.g.: `node = node.step()?`.
pub enum Node {
    /// A candidate campaigns for leadership.
    Candidate(RawNode<Candidate>),
    /// A follower replicates entries from a leader.
    Follower(RawNode<Follower>),
    /// A leader processes client requests and replicates entries to followers.
    Leader(RawNode<Leader>),
}

/// Helper macro which calls a closure on the inner RawNode<Role>.
macro_rules! with_rawnode {
    // Node is moved.
    ($node:expr, $closure:expr) => {{
        fn with<R: Role, T>(node: RawNode<R>, f: impl FnOnce(RawNode<R>) -> T) -> T {
            f(node)
        }
        match $node {
            Node::Candidate(node) => with(node, $closure),
            Node::Follower(node) => with(node, $closure),
            Node::Leader(node) => with(node, $closure),
        }
    }};
    // Node is borrowed (ref).
    (ref $node:expr, $closure:expr) => {{
        fn with<R: Role, T>(node: &RawNode<R>, f: impl FnOnce(&RawNode<R>) -> T) -> T {
            f(node)
        }
        match $node {
            &Node::Candidate(ref node) => with(node, $closure),
            &Node::Follower(ref node) => with(node, $closure),
            &Node::Leader(ref node) => with(node, $closure),
        }
    }};
    // Node is mutably borrowed (ref mut).
    (ref mut $node:expr, $closure:expr) => {{
        fn with<R: Role, T>(node: &mut RawNode<R>, f: impl FnOnce(&mut RawNode<R>) -> T) -> T {
            f(node)
        }
        match $node {
            &mut Node::Candidate(ref mut node) => with(node, $closure),
            &mut Node::Follower(ref mut node) => with(node, $closure),
            &mut Node::Leader(ref mut node) => with(node, $closure),
        }
    }};
}

impl Node {
    /// Creates a new Raft node. It starts as a leaderless follower, waiting to
    /// hear from a leader or otherwise transitioning to candidate and
    /// campaigning for leadership. In the case of a single-node cluster (no
    /// peers), the node immediately transitions to leader when created.
    pub fn new(
        id: NodeID,
        peers: HashSet<NodeID>,
        log: Log,
        state: Box<dyn State>,
        tx: Sender<Envelope>,
        opts: Options,
    ) -> Result<Self> {
        let node = RawNode::new(id, peers, log, state, tx, opts)?;
        // If this is a single-node cluster, become leader immediately.
        if node.cluster_size() == 1 {
            return Ok(node.into_candidate()?.into_leader()?.into());
        }
        Ok(node.into())
    }

    /// Returns the node ID.
    pub fn id(&self) -> NodeID {
        with_rawnode!(ref self, |n| n.id)
    }

    /// Returns the node term.
    pub fn term(&self) -> Term {
        with_rawnode!(ref self, |n| n.term())
    }

    /// Processes an inbound message.
    pub fn step(self, msg: Envelope) -> Result<Self> {
        with_rawnode!(self, |n| {
            assert_eq!(msg.to, n.id, "message to other node: {msg:?}");
            assert!(n.peers.contains(&msg.from) || msg.from == n.id, "unknown sender: {msg:?}");
            debug!("Stepping {msg:?}");
            n.step(msg)
        })
    }

    /// Advances time by a tick.
    pub fn tick(self) -> Result<Self> {
        with_rawnode!(self, |n| n.tick())
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

/// Marker trait for a Raft role: leader, follower, or candidate.
pub trait Role {}

/// A Raft node with role R.
///
/// This implements the typestate pattern, where individual node states (roles)
/// are encoded as RawNode<Role>. See: http://cliffle.com/blog/rust-typestate/
pub struct RawNode<R: Role> {
    /// The node ID. Must be unique in this cluster.
    id: NodeID,
    /// The IDs of the other nodes in the cluster. Does not change while
    /// running. Can change on restart, but all nodes must have the same node
    /// set to avoid multiple leaders (i.e. split brain).
    peers: HashSet<NodeID>,
    /// The Raft log, containing client commands to be executed.
    log: Log,
    /// The Raft state machine, on which client commands are executed.
    state: Box<dyn State>,
    /// Channel for sending outbound messages to other nodes.
    tx: Sender<Envelope>,
    /// Node options.
    opts: Options,
    /// Role-specific state.
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
            tx: self.tx,
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
        *values.select_nth_unstable_by(self.quorum_size() - 1, |a, b| a.cmp(b).reverse()).1
    }

    /// Generates a random election timeout.
    fn random_election_timeout(&self) -> Ticks {
        rand::thread_rng().gen_range(self.opts.election_timeout_range.clone())
    }

    /// Sends a message to the given recipient.
    fn send(&self, to: NodeID, message: Message) -> Result<()> {
        Self::send_with(&self.tx, Envelope { from: self.id, to, term: self.term(), message })
    }

    /// Sends a message without borrowing self, to allow partial borrows.
    fn send_with(tx: &Sender<Envelope>, msg: Envelope) -> Result<()> {
        debug!("Sending {msg:?}");
        Ok(tx.send(msg)?)
    }

    /// Broadcasts a message to all peers.
    fn broadcast(&self, message: Message) -> Result<()> {
        // Send in increasing ID order for test determinism.
        for id in self.peers.iter().copied().sorted() {
            self.send(id, message.clone())?;
        }
        Ok(())
    }
}

// A follower replicates log entries from a leader and forwards client requests.
// Nodes start as leaderless followers, until they either discover a leader or
// hold an election.
pub struct Follower {
    /// The leader, or None if we're a leaderless follower.
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
        tx: Sender<Envelope>,
        opts: Options,
    ) -> Result<Self> {
        if peers.contains(&id) {
            return errinput!("node ID {id} can't be in peers");
        }
        let role = Follower::new(None, 0);
        let mut node = Self { id, peers, log, state, tx, opts, role };
        node.role.election_timeout = node.random_election_timeout();

        // Apply any pending entries following restart. Unlike the Raft log,
        // state machine writes are not flushed to durable storage, so a tail of
        // writes may be lost if the OS crashes or restarts.
        node.maybe_apply()?;
        Ok(node)
    }

    /// Transitions the follower into a candidate, by campaigning for
    /// leadership in a new term.
    fn into_candidate(mut self) -> Result<RawNode<Candidate>> {
        // Abort any forwarded requests. These must be retried with new leader.
        self.abort_forwarded()?;

        // Apply any pending log entries, so that we're caught up if we win.
        self.maybe_apply()?;

        // Become candidate and campaign.
        let election_timeout = self.random_election_timeout();
        let mut node = self.into_role(Candidate::new(election_timeout));
        node.campaign()?;

        let (term, vote) = node.log.get_term();
        assert!(node.role.votes.contains(&node.id), "candidate did not vote for self");
        assert_ne!(term, 0, "candidate can't have term 0");
        assert_eq!(vote, Some(node.id), "log vote does not match self");

        Ok(node)
    }

    /// Transitions the follower into either a leaderless follower in a new term
    /// (e.g. if someone holds a new election) or a follower of a current leader.
    fn into_follower(mut self, term: Term, leader: Option<NodeID>) -> Result<RawNode<Follower>> {
        assert_ne!(term, 0, "can't become follower in term 0");

        // Abort any forwarded requests. These must be retried with new leader.
        self.abort_forwarded()?;

        if let Some(leader) = leader {
            // We found a leader in the current term.
            assert!(self.peers.contains(&leader), "leader is not a peer");
            assert_eq!(self.role.leader, None, "already have leader in term");
            assert_eq!(term, self.term(), "can't follow leader in different term");
            info!("Following leader {leader} in term {term}");
            self.role = Follower::new(Some(leader), self.role.election_timeout);
        } else {
            // We found a new term, but we don't know who the leader is yet.
            // We'll find out if we step a message from it.
            assert_ne!(term, self.term(), "can't become leaderless follower in current term");
            info!("Discovered new term {term}");
            self.log.set_term(term, None)?;
            self.role = Follower::new(None, self.random_election_timeout());
        }
        Ok(self)
    }

    /// Processes an inbound message.
    fn step(mut self, msg: Envelope) -> Result<Node> {
        // Past term: drop the message.
        if msg.term < self.term() {
            debug!("Dropping message from past term: {msg:?}");
            return Ok(self.into());
        }
        // Future term: become leaderless follower and step the message.
        if msg.term > self.term() {
            return self.into_follower(msg.term, None)?.step(msg);
        }

        // Record when we last saw a message from the leader (if any).
        if Some(msg.from) == self.role.leader {
            self.role.leader_seen = 0
        }

        match msg.message {
            // The leader sends periodic heartbeats. If we don't have a leader
            // yet, follow it. If the commit_index advances, apply commands.
            Message::Heartbeat { last_index, commit_index, read_seq } => {
                assert!(commit_index <= last_index, "commit_index after last_index");

                // Make sure the heartbeat is from our leader, or follow it.
                match self.role.leader {
                    Some(leader) => assert_eq!(msg.from, leader, "multiple leaders in term"),
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                }

                // Attempt to match the leader's log and respond to the
                // heartbeat. last_index always has the leader's term.
                let match_index = if self.log.has(last_index, msg.term)? { last_index } else { 0 };
                self.send(msg.from, Message::HeartbeatResponse { match_index, read_seq })?;

                // Advance the commit index and apply entries. We can only do
                // this if we matched the leader's last_index, which implies
                // that the logs are identical up to match_index. This also
                // implies that the commit_index is present in our log.
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

                // Make sure the append is from our leader, or follow it.
                match self.role.leader {
                    Some(leader) => assert_eq!(msg.from, leader, "multiple leaders in term"),
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                }

                // If the base entry matches our log, append the entries.
                let (mut reject_index, mut match_index) = (0, 0);
                if base_index == 0 || self.log.has(base_index, base_term)? {
                    match_index = entries.last().map(|e| e.index).unwrap_or(base_index);
                    self.log.splice(entries)?;
                } else {
                    // Otherwise, reject the base index. If the local log is
                    // shorter than the base index, lower the reject index to
                    // skip all missing entries.
                    reject_index = min(base_index, self.log.get_last_index().0 + 1);
                }
                self.send(msg.from, Message::AppendResponse { reject_index, match_index })?;
            }

            // Confirm the leader's read sequence number.
            Message::Read { seq } => {
                // Make sure the read is from our leader, or follow it.
                match self.role.leader {
                    Some(leader) => assert_eq!(msg.from, leader, "multiple leaders in term"),
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                }

                // Confirm the read.
                self.send(msg.from, Message::ReadResponse { seq })?;
            }

            // A candidate is requesting our vote. We'll only grant one.
            Message::Campaign { last_index, last_term } => {
                // Don't vote if we already voted for someone else in this term.
                // We can repeat our vote though.
                if let (_, Some(vote)) = self.log.get_term() {
                    if msg.from != vote {
                        self.send(msg.from, Message::CampaignResponse { vote: false })?;
                        return Ok(self.into());
                    }
                }

                // Don't vote if our log is newer than the candidate's log.
                // This ensures that an elected leader has all committed
                // entries, see section 5.4.1 in the Raft paper.
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

            // Forward client requests to the leader, or abort them if there is
            // none. These will not be retried, the client should use timeouts.
            // Local client requests use our node ID as the sender.
            Message::ClientRequest { id, request: _ } => {
                assert_eq!(msg.from, self.id, "client request from other node");

                if let Some(leader) = self.role.leader {
                    debug!("Forwarding request to leader {leader}: {msg:?}");
                    self.role.forwarded.insert(id);
                    self.send(leader, msg.message)?
                } else {
                    let response = Err(Error::Abort);
                    self.send(msg.from, Message::ClientResponse { id, response })?
                }
            }

            // Client responses from the leader are passed on to the client.
            Message::ClientResponse { id, response } => {
                assert_eq!(Some(msg.from), self.role.leader, "client response from non-leader");

                if self.role.forwarded.remove(&id) {
                    self.send(self.id, Message::ClientResponse { id, response })?;
                }
            }

            // We may receive a vote after we lost an election, ignore it.
            Message::CampaignResponse { .. } => {}

            // We're not leader this term, so we shouldn't see these.
            Message::HeartbeatResponse { .. }
            | Message::AppendResponse { .. }
            | Message::ReadResponse { .. } => {
                panic!("unexpected message {msg:?}")
            }
        };
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    fn tick(mut self) -> Result<Node> {
        self.role.leader_seen += 1;
        if self.role.leader_seen >= self.role.election_timeout {
            return Ok(self.into_candidate()?.into());
        }
        Ok(self.into())
    }

    /// Aborts all forwarded requests (e.g. on term/leader changes).
    fn abort_forwarded(&mut self) -> Result<()> {
        // Sort by ID for test determinism.
        for id in std::mem::take(&mut self.role.forwarded).into_iter().sorted() {
            debug!("Aborting forwarded request {id}");
            self.send(self.id, Message::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    /// Applies any pending log entries.
    fn maybe_apply(&mut self) -> Result<()> {
        let mut iter = self.log.scan_apply(self.state.get_applied_index());
        while let Some(entry) = iter.next().transpose()? {
            debug!("Applying {entry:?}");
            // Throw away the result, since only the leader responds to clients.
            // This includes errors -- any non-deterministic errors (e.g. IO
            // errors) must panic instead to avoid node divergence.
            _ = self.state.apply(entry);
        }
        Ok(())
    }
}

/// A candidate is campaigning to become a leader.
pub struct Candidate {
    /// Votes received (including our own).
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
    /// Transitions the candidate to a follower. We either lost the election and
    /// follow the winner, or we discovered a new term and step into it as a
    /// leaderless follower.
    fn into_follower(mut self, term: Term, leader: Option<NodeID>) -> Result<RawNode<Follower>> {
        let election_timeout = self.random_election_timeout();
        if let Some(leader) = leader {
            // We lost the election, follow the winner.
            assert_eq!(term, self.term(), "can't follow leader in different term");
            info!("Lost election, following leader {leader} in term {term}");
            Ok(self.into_role(Follower::new(Some(leader), election_timeout)))
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term(), "can't become leaderless follower in current term");
            info!("Discovered new term {term}");
            self.log.set_term(term, None)?;
            Ok(self.into_role(Follower::new(None, election_timeout)))
        }
    }

    /// Transitions the candidate to a leader. We won the election.
    fn into_leader(self) -> Result<RawNode<Leader>> {
        let (term, vote) = self.log.get_term();
        assert_ne!(term, 0, "leaders can't have term 0");
        assert_eq!(vote, Some(self.id), "leader did not vote for self");

        info!("Won election for term {term}, becoming leader");
        let peers = self.peers.clone();
        let (last_index, _) = self.log.get_last_index();
        let mut node = self.into_role(Leader::new(peers, last_index));

        // Propose an empty command when assuming leadership, to disambiguate
        // previous entries in the log. See section 5.4.2 in the Raft paper.
        // We do this prior to the heartbeat, to avoid a wasted replication
        // roundtrip if the heartbeat response indicates the peer is behind.
        node.propose(None)?;
        node.maybe_commit_and_apply()?;
        node.heartbeat()?;

        Ok(node)
    }

    /// Processes an inbound message.
    fn step(mut self, msg: Envelope) -> Result<Node> {
        // Past term: drop the message.
        if msg.term < self.term() {
            debug!("Dropping message from past term: {msg:?}");
            return Ok(self.into());
        }
        // Future term: become leaderless follower and step the message.
        if msg.term > self.term() {
            return self.into_follower(msg.term, None)?.step(msg);
        }

        match msg.message {
            // If we received a vote, record it. If the vote gives us quorum,
            // assume leadership.
            Message::CampaignResponse { vote: true } => {
                self.role.votes.insert(msg.from);
                if self.role.votes.len() >= self.quorum_size() {
                    return Ok(self.into_leader()?.into());
                }
            }

            // We didn't get the vote. :(
            Message::CampaignResponse { vote: false } => {}

            // Don't grant votes for other candidates.
            Message::Campaign { .. } => {
                self.send(msg.from, Message::CampaignResponse { vote: false })?
            }

            // If we hear from a leader in this term, we lost the election.
            // Follow it and step the message.
            Message::Heartbeat { .. } | Message::Append { .. } | Message::Read { .. } => {
                return self.into_follower(msg.term, Some(msg.from))?.step(msg);
            }

            // Abort client requests while campaigning. The client must retry.
            Message::ClientRequest { id, request: _ } => {
                self.send(msg.from, Message::ClientResponse { id, response: Err(Error::Abort) })?;
            }

            // We're not a leader in this term, nor are we forwarding requests,
            // so we shouldn't see these.
            Message::HeartbeatResponse { .. }
            | Message::AppendResponse { .. }
            | Message::ReadResponse { .. }
            | Message::ClientResponse { .. } => panic!("unexpected message {msg:?}"),
        }
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    fn tick(mut self) -> Result<Node> {
        self.role.election_duration += 1;
        if self.role.election_duration >= self.role.election_timeout {
            self.campaign()?;
        }
        Ok(self.into())
    }

    /// Hold a new election by increasing the term, voting for ourself, and
    /// soliciting votes from all peers.
    fn campaign(&mut self) -> Result<()> {
        let term = self.term() + 1;
        info!("Starting new election for term {term}");
        self.role = Candidate::new(self.random_election_timeout());
        self.role.votes.insert(self.id); // vote for ourself
        self.log.set_term(term, Some(self.id))?;

        let (last_index, last_term) = self.log.get_last_index();
        self.broadcast(Message::Campaign { last_index, last_term })
    }
}

// A leader serves client requests and replicates the log to followers.
// If the leader loses leadership, all client requests are aborted.
pub struct Leader {
    /// Follower replication progress.
    progress: HashMap<NodeID, Progress>,
    /// Tracks pending write requests by log index. Added when the write is
    /// proposed and appended to the leader's log, and removed when the command
    /// is applied to the state machine, returning the result to the client.
    writes: HashMap<Index, Write>,
    /// Tracks pending read requests. For linearizability, read requests are
    /// assigned a sequence number and only executed once a quorum of nodes have
    /// confirmed it. Otherwise, an old leader may serve stale reads if a new
    /// leader has been elected elsewhere.
    reads: VecDeque<Read>,
    /// The read sequence number used for the last read. Initialized to 0 in
    /// this term, and incremented for every read command.
    read_seq: ReadSequence,
    /// Number of ticks since last heartbeat.
    since_heartbeat: Ticks,
}

/// Follower replication progress (in this term).
struct Progress {
    /// The highest index where the follower's log is known to match the leader.
    /// Initialized to 0, increases monotonically.
    match_index: Index,
    /// The next index to replicate to the follower. Initialized to
    /// last_index+1, decreased when probing log mismatches. Always in
    /// the range [match_index+1, last_index+1].
    ///
    /// Entries pending transmission are in the range [next_index, last_index].
    /// Unacknowledged entries are in the range [match_index+1, next_index).
    next_index: Index,
    /// The last read sequence number confirmed by the peer. To avoid stale
    /// reads on leader changes, a read is only served once its sequence number
    /// is confirmed by a quorum.
    read_seq: ReadSequence,
}

impl Progress {
    /// Attempts to advance a follower's match index, returning true if it did.
    /// If next_index is below it, it is advanced to the following index.
    fn advance(&mut self, match_index: Index) -> bool {
        if match_index <= self.match_index {
            return false;
        }
        self.match_index = match_index;
        self.next_index = max(self.next_index, match_index + 1);
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

    /// Attempts to regress a follower's next index to the given index, returning
    /// true if it did. Won't regress below match_index + 1.
    fn regress_next(&mut self, next_index: Index) -> bool {
        if next_index >= self.next_index || self.next_index <= self.match_index + 1 {
            return false;
        }
        self.next_index = max(next_index, self.match_index + 1);
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
    /// Transitions the leader into a follower. This can only happen if we
    /// discover a new term, so we become a leaderless follower. Stepping the
    /// received message may then follow the new leader, if there is one.
    fn into_follower(mut self, term: Term) -> Result<RawNode<Follower>> {
        assert!(term > self.term(), "leader can only become follower in later term");
        info!("Discovered new term {term}");

        // Abort in-flight requests. The client must retry. Sort the requests
        // by ID for test determinism.
        for write in std::mem::take(&mut self.role.writes).into_values().sorted_by_key(|w| w.id) {
            let response = Err(Error::Abort);
            self.send(write.from, Message::ClientResponse { id: write.id, response })?;
        }
        for read in std::mem::take(&mut self.role.reads).into_iter().sorted_by_key(|r| r.id) {
            let response = Err(Error::Abort);
            self.send(read.from, Message::ClientResponse { id: read.id, response })?;
        }

        self.log.set_term(term, None)?;
        let election_timeout = self.random_election_timeout();
        Ok(self.into_role(Follower::new(None, election_timeout)))
    }

    /// Processes an inbound message.
    fn step(mut self, msg: Envelope) -> Result<Node> {
        // Past term: drop the message.
        if msg.term < self.term() {
            debug!("Dropping message from past term: {msg:?}");
            return Ok(self.into());
        }
        // Future term: become leaderless follower and step the message.
        if msg.term > self.term() {
            return self.into_follower(msg.term)?.step(msg);
        }

        match msg.message {
            // A follower received our heartbeat and confirms our leadership.
            // We may be able to execute new reads, and we may find that the
            // follower's log is lagging and requires us to catch it up.
            Message::HeartbeatResponse { match_index, read_seq } => {
                let (last_index, _) = self.log.get_last_index();
                assert!(match_index <= last_index, "future match index");
                assert!(read_seq <= self.role.read_seq, "future read sequence number");

                // If the read sequence number advances, try to execute reads.
                if self.progress(msg.from).advance_read(read_seq) {
                    self.maybe_read()?;
                }

                // If the follower didn't match our last index, an append to it
                // must have failed (or it's catching up). Probe it to discover
                // a matching entry and start replicating. Move next_index back
                // to last_index since the follower just told us it doesn't have
                // it (or a previous last_index).
                if match_index == 0 {
                    self.progress(msg.from).regress_next(last_index);
                    self.maybe_send_append(msg.from, true)?;
                }

                // If the follower's match index advances, an append response
                // got lost. Try to commit and apply.
                //
                // We don't need to eagerly send any pending entries, since any
                // proposals made after this heartbeat was sent should have been
                // eagerly replicated in steady state. If not, the next
                // heartbeat will trigger a probe above.
                if self.progress(msg.from).advance(match_index) {
                    self.maybe_commit_and_apply()?;
                }
            }

            // A follower appended our log entries (or a probe found a match).
            // Record its progress and attempt to commit and apply.
            Message::AppendResponse { match_index, reject_index: 0 } if match_index > 0 => {
                let (last_index, _) = self.log.get_last_index();
                assert!(match_index <= last_index, "future match index");

                if self.progress(msg.from).advance(match_index) {
                    self.maybe_commit_and_apply()?;
                }

                // Eagerly send any further pending entries. This may be a
                // successful probe response, or the peer may be lagging and
                // we're catching it up one MAX_APPEND_ENTRIES batch at a time.
                self.maybe_send_append(msg.from, false)?;
            }

            // A follower confirmed our read sequence number. If it advances,
            // try to execute reads.
            Message::ReadResponse { seq } => {
                if self.progress(msg.from).advance_read(seq) {
                    self.maybe_read()?;
                }
            }

            // A follower rejected an append because the base entry in
            // reject_index did not match its log. Probe the previous entry by
            // sending an empty append until we find a common base.
            //
            // This linear probing can be slow with long divergent logs, but we
            // keep it simple. See also section 5.3 in the Raft paper.
            Message::AppendResponse { reject_index, match_index: 0 } if reject_index > 0 => {
                let (last_index, _) = self.log.get_last_index();
                assert!(reject_index <= last_index, "future reject index");

                // If the rejected base index is at or below the match index,
                // the rejection is stale and can be ignored.
                if reject_index <= self.progress(msg.from).match_index {
                    return Ok(self.into());
                }

                // Probe below the reject index, if we haven't already moved
                // next_index below it. This avoids sending duplicate probes
                // (heartbeats will trigger retries if they're lost).
                if self.progress(msg.from).regress_next(reject_index) {
                    self.maybe_send_append(msg.from, true)?;
                }
            }

            // AppendResponses must set either match_index or reject_index.
            Message::AppendResponse { .. } => panic!("invalid message {msg:?}"),

            // A client submitted a write request. Propose it, and wait until
            // it's replicated and applied to the state machine before returning
            // the response to the client.
            Message::ClientRequest { id, request: Request::Write(command) } => {
                let index = self.propose(Some(command))?;
                self.role.writes.insert(index, Write { from: msg.from, id });
                if self.cluster_size() == 1 {
                    self.maybe_commit_and_apply()?;
                }
            }

            // A client submitted a read request. To ensure linearizability, we
            // must confirm that we are still the leader by sending the read's
            // sequence number and wait for quorum confirmation.
            Message::ClientRequest { id, request: Request::Read(command) } => {
                self.role.read_seq += 1;
                let read = Read { seq: self.role.read_seq, from: msg.from, id, command };
                self.role.reads.push_back(read);
                self.broadcast(Message::Read { seq: self.role.read_seq })?;
                if self.cluster_size() == 1 {
                    self.maybe_read()?;
                }
            }

            // A client submitted a status command.
            Message::ClientRequest { id, request: Request::Status } => {
                let response = self.status().map(Response::Status);
                self.send(msg.from, Message::ClientResponse { id, response })?;
            }

            // Don't grant any votes (we've already voted for ourself).
            Message::Campaign { .. } => {
                self.send(msg.from, Message::CampaignResponse { vote: false })?
            }

            // Votes can come in after we won the election, ignore them.
            Message::CampaignResponse { .. } => {}

            // There can't be another leader in this term.
            Message::Heartbeat { .. } | Message::Append { .. } | Message::Read { .. } => {
                panic!("saw other leader {} in term {}", msg.from, msg.term);
            }

            // Leaders don't proxy client requests.
            Message::ClientResponse { .. } => panic!("unexpected message {msg:?}"),
        }

        Ok(self.into())
    }

    /// Processes a logical clock tick.
    fn tick(mut self) -> Result<Node> {
        self.role.since_heartbeat += 1;
        if self.role.since_heartbeat >= self.opts.heartbeat_interval {
            self.heartbeat()?;
        }
        Ok(self.into())
    }

    /// Broadcasts a heartbeat to all peers.
    fn heartbeat(&mut self) -> Result<()> {
        let (last_index, last_term) = self.log.get_last_index();
        let (commit_index, _) = self.log.get_commit_index();
        let read_seq = self.role.read_seq;
        assert_eq!(last_term, self.term(), "leader's last_term not in current term");

        self.role.since_heartbeat = 0;
        self.broadcast(Message::Heartbeat { last_index, commit_index, read_seq })
    }

    /// Proposes a command for consensus by appending it to our log and
    /// replicating it to peers. If successful, it will eventually be committed
    /// and applied to the state machine.
    fn propose(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        let index = self.log.append(command)?;
        for peer in self.peers.iter().copied().sorted() {
            // Eagerly send the entry to the peer if it's in steady state and
            // we've sent all previous entries. Otherwise, the peer is lagging
            // and we're probing past entries for a match.
            if index == self.progress(peer).next_index {
                self.maybe_send_append(peer, false)?;
            }
        }
        Ok(index)
    }

    /// Commits new entries that have been replicated to a quorum and applies
    /// them to the state machine, returning results to clients.
    fn maybe_commit_and_apply(&mut self) -> Result<Index> {
        // Determine the new commit index by quorum.
        let (last_index, _) = self.log.get_last_index();
        let quorum_index = self.quorum_value(
            self.role.progress.values().map(|p| p.match_index).chain([last_index]).collect(),
        );

        // If the commit index doesn't advance, do nothing. We don't assert on
        // this, since the quorum value may regress e.g. following a restart or
        // leader change where followers are initialized with match index 0.
        let (old_index, old_term) = self.log.get_commit_index();
        if quorum_index <= old_index {
            return Ok(old_index);
        }

        // We can only safely commit an entry from our own term (see section
        // 5.4.2 in Raft paper).
        match self.log.get(quorum_index)? {
            Some(entry) if entry.term == self.term() => {}
            Some(_) => return Ok(old_index),
            None => panic!("commit index {quorum_index} missing"),
        }

        // Commit entries.
        self.log.commit(quorum_index)?;

        // Apply entries and respond to clients.
        let term = self.term();
        let mut iter = self.log.scan_apply(self.state.get_applied_index());
        while let Some(entry) = iter.next().transpose()? {
            debug!("Applying {entry:?}");
            let write = self.role.writes.remove(&entry.index);
            let result = self.state.apply(entry);

            if let Some(Write { id, from: to }) = write {
                let message = Message::ClientResponse { id, response: result.map(Response::Write) };
                Self::send_with(&self.tx, Envelope { from: self.id, term, to, message })?;
            }
        }
        drop(iter);

        // If the commit term changed, there may be pending reads waiting for us
        // to commit and apply an entry from our own term. Execute them.
        if old_term != self.term() {
            self.maybe_read()?;
        }

        Ok(quorum_index)
    }

    /// Executes any ready read requests (with confirmed sequence numbers).
    fn maybe_read(&mut self) -> Result<()> {
        if self.role.reads.is_empty() {
            return Ok(());
        }

        // It's only safe to read if we've committed and applied an entry from
        // our own term (the leader appends an entry when elected). Otherwise we
        // may be behind on application and serve stale reads.
        let (commit_index, commit_term) = self.log.get_commit_index();
        let applied_index = self.state.get_applied_index();
        if commit_term < self.term() || applied_index < commit_index {
            return Ok(());
        }

        // Determine the maximum read sequence confirmed by quorum.
        let quorum_read_seq = self.quorum_value(
            self.role.progress.values().map(|p| p.read_seq).chain([self.role.read_seq]).collect(),
        );

        // Execute ready reads. The VecDeque is ordered by read_seq, so we
        // can keep pulling until we hit quorum_read_seq.
        while let Some(read) = self.role.reads.front() {
            if read.seq > quorum_read_seq {
                break;
            }
            let read = self.role.reads.pop_front().unwrap();
            let response = self.state.read(read.command).map(Response::Read);
            self.send(read.from, Message::ClientResponse { id: read.id, response })?;
        }
        Ok(())
    }

    // Sends a batch of pending log entries to a follower in the
    // [next_index,last_index] range, limited by max_append_entries.
    //
    // If probe is true, an empty append probe with base_index of next_index-1
    // is sent to check if the base entry is present in the follower's log. If
    // it is, the actual entries are sent next -- otherwise, next_index is
    // decremented and another probe is sent until a match is found. See section
    // 5.3 in the Raft paper.
    //
    // The probe is skipped if the follower is up-to-date (according to
    // match_index and last_index). If the probe's base_index has already been
    // confirmed via match_index, an actual append is sent instead.
    fn maybe_send_append(&mut self, peer: NodeID, mut probe: bool) -> Result<()> {
        let (last_index, _) = self.log.get_last_index();
        let progress = self.role.progress.get_mut(&peer).expect("unknown node");
        assert_ne!(progress.next_index, 0, "invalid next_index");
        assert!(progress.next_index > progress.match_index, "invalid next_index <= match_index");
        assert!(progress.match_index <= last_index, "invalid match_index > last_index");
        assert!(progress.next_index <= last_index + 1, "invalid next_index > last_index + 1");

        // If the peer is caught up, there's no point sending an append.
        if progress.match_index == last_index {
            return Ok(());
        }

        // If a probe was requested, but the base_index has already been
        // confirmed via match_index, there is no point in probing. Just send
        // the entries instead.
        probe = probe && progress.next_index > progress.match_index + 1;

        // If there are no pending entries, and this is not a probe, there's
        // nothing more to send until we get a response from the follower.
        if progress.next_index > last_index && !probe {
            return Ok(());
        }

        // Fetch the base and entries.
        let (base_index, base_term) = match progress.next_index {
            0 => panic!("next_index=0 for node {peer}"),
            1 => (0, 0),
            next => self.log.get(next - 1)?.map(|e| (e.index, e.term)).expect("missing base entry"),
        };
        let entries = match probe {
            false => self
                .log
                .scan(progress.next_index..)
                .take(self.opts.max_append_entries)
                .try_collect()?,
            true => Vec::new(),
        };

        // Optimistically assume the entries will be accepted by the follower,
        // and bump next_index to avoid resending them until a response.
        if let Some(last) = entries.last() {
            progress.next_index = last.index + 1;
        }

        debug!("Replicating {} entries with base {base_index} to {peer}", entries.len());
        self.send(peer, Message::Append { base_index, base_term, entries })
    }

    /// Generates cluster status.
    fn status(&mut self) -> Result<Status> {
        Ok(Status {
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
            applied_index: self.state.get_applied_index(),
            storage: self.log.status()?,
        })
    }

    /// Returns a mutable borrow of a node's progress. Convenience method.
    fn progress(&mut self, id: NodeID) -> &mut Progress {
        self.role.progress.get_mut(&id).expect("unknown node")
    }
}

/// Most Raft tests are Goldenscripts under src/raft/testscripts.
#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use std::error::Error;
    use std::fmt::Write as _;
    use std::result::Result;

    use crossbeam::channel::Receiver;
    use tempfile::TempDir;
    use test_case::test_case;
    use test_each_file::test_each_path;
    use uuid::Uuid;

    use super::*;
    use crate::encoding::{Key as _, Value as _, bincode};
    use crate::raft::Entry;
    use crate::raft::state::test::{self as teststate, KVCommand, KVResponse};
    use crate::storage;
    use crate::storage::engine::test as testengine;

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

    /// Test helpers for RawNode.
    impl RawNode<Follower> {
        /// Creates a noop node, with a noop state machine and transport.
        fn new_noop(id: NodeID, peers: HashSet<NodeID>) -> Self {
            let log = Log::new(Box::new(storage::Memory::new())).expect("log failed");
            let state = teststate::Noop::new();
            let (tx, _) = crossbeam::channel::unbounded();
            RawNode::new(id, peers, log, state, tx, Options::default()).expect("node failed")
        }
    }

    /// Test helpers for Node.
    impl Node {
        fn dismantle(self) -> (Log, Box<dyn State>) {
            with_rawnode!(self, |n| (n.log, n.state))
        }

        fn get_applied_index(&self) -> Index {
            with_rawnode!(ref self, |n| n.state.get_applied_index())
        }

        fn get_commit_index(&self) -> (Index, Term) {
            with_rawnode!(ref self, |n| n.log.get_commit_index())
        }

        fn get_last_index(&self) -> (Index, Term) {
            with_rawnode!(ref self, |n| n.log.get_last_index())
        }

        fn get_term_vote(&self) -> (Term, Option<NodeID>) {
            with_rawnode!(ref self, |n| n.log.get_term())
        }

        fn options(&self) -> Options {
            with_rawnode!(ref self, |n| n.opts.clone())
        }

        fn peers(&self) -> HashSet<NodeID> {
            with_rawnode!(ref self, |n| n.peers.clone())
        }

        fn read(&self, command: Vec<u8>) -> crate::error::Result<Vec<u8>> {
            with_rawnode!(ref self, |n| n.state.read(command))
        }

        fn scan_log(&mut self) -> crate::error::Result<Vec<Entry>> {
            with_rawnode!(ref mut self, |n| n.log.scan(..).collect())
        }
    }

    /// Runs Raft goldenscript tests. See run() for available commands.
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
        /// Network partitions (sender → receivers). A symmetric (bidirectional)
        /// partition needs an entry from each side.
        disconnected: HashMap<NodeID, HashSet<NodeID>>,
        /// In-flight client requests.
        requests: HashMap<RequestID, Request>,
        /// The request ID to use for the next client request.
        next_request_id: u64,
        /// Temporary directory (deleted when dropped).
        tempdir: TempDir,
    }

    impl goldenscript::Runner for TestRunner {
        /// Runs a goldenscript command.
        fn run(&mut self, command: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
            let mut output = String::new();
            match command.name.as_str() {
                // campaign [ID...]
                // Transition the given nodes to candidates and campaign.
                "campaign" => {
                    let ids = self.parse_ids_or_all(&command.args)?;
                    self.campaign(&ids, &mut output)?;
                }

                // cluster nodes=N [leader=ID] [heartbeat_interval=N] [election_timeout=N] [max_append_entries=N]
                // Creates a new Raft cluster.
                "cluster" => {
                    let mut opts = Options::default();
                    let mut args = command.consume_args();
                    let nodes = args.lookup_parse("nodes")?.unwrap_or(0);
                    let leader = args.lookup_parse("leader")?;
                    if let Some(heartbeat_interval) = args.lookup_parse("heartbeat_interval")? {
                        opts.heartbeat_interval = heartbeat_interval;
                    };
                    if let Some(election_timeout) = args.lookup_parse("election_timeout")? {
                        opts.election_timeout_range = election_timeout..election_timeout + 1;
                    }
                    if let Some(max_append_entries) = args.lookup_parse("max_append_entries")? {
                        opts.max_append_entries = max_append_entries;
                    }
                    args.reject_rest()?;
                    self.cluster(nodes, leader, opts, &mut output)?;
                }

                // deliver [from=ID] [ID...]
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
                // Sends a client request to the given node to read the given
                // key from the state machine (key/value store).
                "get" => {
                    let mut args = command.consume_args();
                    let id = args.next_pos().ok_or("must specify node ID")?.parse()?;
                    let key = args.next_pos().ok_or("must specify key")?.value.clone();
                    args.reject_rest()?;
                    let request = Request::Read(KVCommand::Get { key }.encode());
                    self.request(id, request, &mut output)?;
                }

                // heal [ID...]
                // Heals all network partitions for the given nodes.
                "heal" => {
                    let ids = self.parse_ids_or_all(&command.args)?;
                    self.heal(&ids, &mut output)?;
                }

                // heartbeat ID...
                // Sends a heartbeat from the given leader nodes.
                "heartbeat" => {
                    let ids = self.parse_ids_or_all(&command.args)?;
                    self.heartbeat(&ids, &mut output)?;
                }

                // log [ID...]
                // Outputs the current Raft log for the given nodes.
                "log" => {
                    let ids = self.parse_ids_or_all(&command.args)?;
                    self.log(&ids, &mut output)?;
                }

                // partition ID...
                // Partitions the given nodes away from the rest of the cluster.
                // They can still communicate with each other, unless they were
                // previously partitioned.
                "partition" => {
                    let ids = self.parse_ids_or_error(&command.args)?;
                    self.partition(&ids, &mut output)?;
                }

                // put ID KEY=VALUE
                // Sends a client request to the given node to write a key/value
                // pair to the state machine (key/value store).
                "put" => {
                    let mut args = command.consume_args();
                    let id = args.next_pos().ok_or("must specify node ID")?.parse()?;
                    let kv = args.next_key().ok_or("must specify key/value pair")?.clone();
                    let (key, value) = (kv.key.unwrap(), kv.value);
                    args.reject_rest()?;
                    let request = Request::Write(KVCommand::Put { key, value }.encode());
                    self.request(id, request, &mut output)?;
                }

                // restart [commit_index=INDEX] [applied_index=INDEX] [ID...]
                // Restarts the given nodes (or all nodes). They retain their
                // log and state, unless applied_index is given (which reverts
                // the state machine to the given index, or 0 if empty).
                // commit_index may be given to regress the commit index (it
                // is not flushed to durable storage).
                "restart" => {
                    let mut args = command.consume_args();
                    let applied_index = args.lookup_parse("applied_index")?;
                    let commit_index = args.lookup_parse("commit_index")?;
                    let ids = self.parse_ids_or_all(&args.rest())?;
                    self.restart(&ids, commit_index, applied_index, &mut output)?;
                }

                // stabilize [heartbeat=BOOL] [ID...]
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
                // Prints the current state machine contents on the given nodes.
                "state" => {
                    let ids = self.parse_ids_or_all(&command.args)?;
                    self.state(&ids, &mut output)?;
                }

                // status [request=BOOL] [ID...]
                // Prints the current node status of the given nodes. If request
                // is true, sends a status client request to a single node,
                // otherwise fetches status directly from each node.
                "status" => {
                    let mut args = command.consume_args();
                    let request = args.lookup_parse("request")?.unwrap_or(false);
                    let ids = self.parse_ids_or_all(&args.rest())?;
                    if request {
                        let [id] = *ids.as_slice() else {
                            return Err("request=true requires 1 node ID".into());
                        };
                        self.request(id, Request::Status, &mut output)?;
                    } else {
                        self.status(&ids, &mut output)?;
                    }
                }

                // step ID JSON
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
            Self {
                ids: Vec::new(),
                nodes: HashMap::new(),
                nodes_rx: HashMap::new(),
                nodes_pending: HashMap::new(),
                applied_rx: HashMap::new(),
                disconnected: HashMap::new(),
                requests: HashMap::new(),
                next_request_id: 1,
                tempdir: TempDir::with_prefix("toydb").expect("tempdir failed"),
            }
        }

        /// Creates a new empty node and inserts it.
        fn add_node(
            &mut self,
            id: NodeID,
            peers: HashSet<NodeID>,
            opts: Options,
        ) -> Result<(), Box<dyn Error>> {
            // Use both a BitCask and a Memory engine, and mirror operations
            // across them, for added engine test coverage.
            let path = self.tempdir.path().join(format!("{id}.log"));
            let bitcask = storage::BitCask::new(path).expect("bitcask failed");
            let memory = storage::Memory::new();
            let engine = testengine::Mirror::new(bitcask, memory);
            let log = Log::new(Box::new(engine))?;
            let state = teststate::KV::new();
            self.add_node_with(id, peers, log, state, opts)
        }

        /// Creates a new node with the given log and state and inserts it.
        fn add_node_with(
            &mut self,
            id: NodeID,
            peers: HashSet<NodeID>,
            log: Log,
            state: Box<dyn State>,
            opts: Options,
        ) -> Result<(), Box<dyn Error>> {
            let (node_tx, node_rx) = crossbeam::channel::unbounded();
            let (applied_tx, applied_rx) = crossbeam::channel::unbounded();
            let state = teststate::Emit::new(state, applied_tx);
            self.nodes.insert(id, Node::new(id, peers, log, state, node_tx, opts)?);
            self.nodes_rx.insert(id, node_rx);
            self.nodes_pending.insert(id, Vec::new());
            self.applied_rx.insert(id, applied_rx);
            self.disconnected.insert(id, HashSet::new());
            Ok(())
        }

        /// Transitions nodes to candidates and campaign in a new term.
        fn campaign(&mut self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            let campaign = |node| match node {
                Node::Candidate(mut node) => {
                    node.campaign()?;
                    Ok(node.into())
                }
                Node::Follower(node) => Ok(node.into_candidate()?.into()),
                Node::Leader(node) => {
                    let term = node.term();
                    Ok(node.into_follower(term + 1)?.into_candidate()?.into())
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
            opts: Options,
            output: &mut String,
        ) -> Result<(), Box<dyn Error>> {
            if !self.ids.is_empty() {
                return Err("cluster already exists".into());
            }
            if nodes == 0 {
                return Err("cluster can't have 0 nodes".into());
            }

            self.ids = (1..=nodes).collect();

            for id in self.ids.clone() {
                let peers = self.ids.iter().copied().filter(|i| i != &id).collect();
                self.add_node(id, peers, opts.clone())?;
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

            // Drain any initial applied entries.
            for applied_rx in self.applied_rx.values_mut() {
                while applied_rx.try_recv().is_ok() {}
            }

            // Output final cluster status.
            self.status(&self.ids, output)
        }

        /// Delivers pending messages to the given nodes. If from is given, only
        /// delivers messages from that node. Returns the number of delivered
        /// messages.
        fn deliver(
            &mut self,
            ids: &[NodeID],
            from: Option<NodeID>,
            output: &mut String,
        ) -> Result<usize, Box<dyn Error>> {
            // Take a snapshot of the pending queues before delivering any
            // messages. This avoids outbound messages in response to delivery
            // being delivered to higher node IDs in the same loop, which can
            // give unintuitive results.
            let mut step = Vec::new();
            for id in ids.iter().copied() {
                let Some(pending) = self.nodes_pending.remove(&id) else {
                    return Err(format!("unknown node {id}").into());
                };
                let (deliver, requeue) =
                    pending.into_iter().partition(|msg| from.is_none() || from == Some(msg.from));
                self.nodes_pending.insert(id, requeue);
                step.extend(deliver);
            }

            let delivered = step.len();
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
                let Some(Node::Leader(leader)) = self.nodes.get_mut(&id) else {
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
                let (last_index, last_term) = node.get_last_index();
                let (commit_index, commit_term) = node.get_commit_index();
                let (term, vote) = node.get_term_vote();
                writeln!(
                    output,
                    "{nodefmt} term={term} last={last_index}@{last_term} commit={commit_index}@{commit_term} vote={vote:?}",
                )?;
                for entry in node.scan_log()? {
                    writeln!(output, "{nodefmt} entry {}", Self::format_entry(&entry))?;
                }
            }
            Ok(())
        }

        /// Partitions the given nodes from all other nodes in the cluster
        /// (bidirectionally). The given nodes can communicate with each other
        /// unless they were previously partitioned.
        fn partition(&mut self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            let ids = HashSet::<NodeID>::from_iter(ids.iter().copied());
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
                let msgfmt = Self::format_message(&msg.message);

                // If the peer is disconnected, drop the message and output it.
                if self.disconnected[&msg.from].contains(&msg.to) {
                    writeln!(
                        output,
                        "n{from}@{term} ⇥ n{to} {}",
                        Self::format_strikethrough(&msgfmt),
                    )?;
                    continue;
                }

                // Intercept and output client responses.
                if msg.from == msg.to {
                    let Message::ClientResponse { id, response } = &msg.message else {
                        return Err(format!("invalid self-addressed message: {msg:?}").into());
                    };
                    writeln!(output, "n{from}@{term} → c{to} {msgfmt}")?;
                    let request = &self.requests.remove(id).ok_or("unknown request id")?;
                    writeln!(
                        output,
                        "c{to}@{term} {} ⇒ {}",
                        Self::format_request(request),
                        Self::format_response(response),
                    )?;
                    continue;
                }

                // Output the message and queue it for delivery.
                writeln!(output, "n{from}@{term} → n{to} {msgfmt}")?;
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
            let request_id = Uuid::from_u64_pair(0, self.next_request_id);
            self.next_request_id += 1;
            self.requests.insert(request_id, request.clone());

            let term = self.nodes.get(&id).ok_or(format!("unknown node {id}"))?.term();
            let msg = Envelope {
                from: id,
                to: id,
                term,
                message: Message::ClientRequest { id: request_id, request },
            };
            writeln!(output, "c{id}@{term} → n{id} {}", Self::format_message(&msg.message))?;
            self.transition(id, |n| n.step(msg), output)
        }

        /// Restarts the given nodes. If commit_index or applied_index are
        /// given, the log commit index or state machine will regress.
        fn restart(
            &mut self,
            ids: &[NodeID],
            commit_index: Option<Index>,
            applied_index: Option<Index>,
            output: &mut String,
        ) -> Result<(), Box<dyn Error>> {
            for id in ids.iter().copied() {
                let node = self.nodes.remove(&id).ok_or(format!("unknown node {id}"))?;
                let peers = node.peers();
                let opts = node.options();
                let (log, mut state) = node.dismantle();
                let mut log = Log::new(log.engine)?; // reset log

                // If requested, regress the commit index.
                if let Some(commit_index) = commit_index {
                    if commit_index > log.get_commit_index().0 {
                        return Err(format!("commit_index={commit_index} beyond current").into());
                    }
                    let commit_term = match log.get(commit_index)? {
                        Some(e) => e.term,
                        None if commit_index == 0 => 0,
                        None => return Err(format!("unknown commit_index={commit_index}").into()),
                    };
                    log.engine.set(
                        &crate::raft::log::Key::CommitIndex.encode(),
                        bincode::serialize(&(commit_index, commit_term)),
                    )?;
                    // Reset the log again.
                    log = Log::new(log.engine)?;
                }

                // If requested, wipe the state machine and reapply up to the
                // requested applied index.
                if let Some(applied_index) = applied_index {
                    if applied_index > log.get_commit_index().0 {
                        return Err(format!("applied_index={applied_index} beyond commit").into());
                    }
                    state = teststate::KV::new();
                    let mut scan = log.scan(..=applied_index);
                    while let Some(entry) = scan.next().transpose()? {
                        _ = state.apply(entry); // apply errors are returned to client
                    }
                    assert_eq!(state.get_applied_index(), applied_index, "wrong applied index");
                }

                // Add node, and run a noop transition to output applied entries.
                self.add_node_with(id, peers, log, state, opts)?;
                self.transition(id, Ok, output)?;
            }
            // Output restarted node status.
            self.status(ids, output)
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
            // If requested, heartbeat the current leader (with the highest
            // term) and re-stabilize the nodes.
            if heartbeat {
                let leader = self
                    .nodes
                    .values()
                    .sorted_by_key(|n| n.term())
                    .rev()
                    .find(|n| matches!(n, Node::Leader(_)));
                if let Some(leader) = leader {
                    self.heartbeat(&[leader.id()], output)?;
                    self.stabilize(ids, false, output)?;
                }
            }
            Ok(())
        }

        /// Outputs the current state machine for the given nodes.
        fn state(&mut self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            for id in ids {
                let node = self.nodes.get_mut(id).ok_or(format!("unknown node {id}"))?;
                let nodefmt = Self::format_node(node);
                let applied_index = node.get_applied_index();
                let raw = node.read(KVCommand::Scan.encode())?;
                let KVResponse::Scan(kvs) = KVResponse::decode(&raw)? else {
                    return Err("unexpected scan response".into());
                };
                writeln!(output, "{nodefmt} applied={applied_index}")?;
                for (key, value) in kvs {
                    writeln!(output, "{nodefmt} state {key}={value}")?;
                }
            }
            Ok(())
        }

        /// Outputs status for the given nodes.
        fn status(&self, ids: &[NodeID], output: &mut String) -> Result<(), Box<dyn Error>> {
            for id in ids {
                let node = self.nodes.get(id).ok_or(format!("unknown node {id}"))?;
                let (last_index, last_term) = node.get_last_index();
                let (commit_index, commit_term) = node.get_commit_index();
                let applied_index = node.get_applied_index();
                write!(
                    output,
                    "{node} last={last_index}@{last_term} commit={commit_index}@{commit_term} applied={applied_index}",
                    node = Self::format_node_role(node)
                )?;
                if let Node::Leader(leader) = node {
                    let progress = leader
                        .role
                        .progress
                        .iter()
                        .sorted_by_key(|(id, _)| *id)
                        .map(|(id, pr)| format!("{id}:{}→{}", pr.match_index, pr.next_index))
                        .join(" ");
                    write!(output, " progress={{{progress}}}")?
                }
                output.push('\n');
            }
            Ok(())
        }

        /// Applies a node transition (typically a step or tick), and outputs
        /// relevant changes.
        fn transition(
            &mut self,
            id: NodeID,
            f: impl FnOnce(Node) -> crate::error::Result<Node>,
            output: &mut String,
        ) -> Result<(), Box<dyn Error>> {
            let mut node = self.nodes.remove(&id).ok_or(format!("unknown node {id}"))?;

            // Fetch pre-transition info.
            let old_noderole = Self::format_node_role(&node);
            let (old_commit_index, _) = node.get_commit_index();
            let mut old_entries = node.scan_log()?.into_iter();

            // Apply the transition.
            node = f(node)?;

            // Fetch post-transition info.
            let nodefmt = Self::format_node(&node);
            let noderole = Self::format_node_role(&node);
            let (commit_index, commit_term) = node.get_commit_index();

            let entries = node.scan_log()?.into_iter();
            let appended: Vec<Entry> = entries
                .skip_while(|e| Some(e.term) == old_entries.next().map(|e| e.term))
                .collect();

            self.nodes.insert(id, node);

            // Output relevant changes.
            if old_noderole != noderole {
                writeln!(output, "{old_noderole} ⇨ {noderole}")?
            }
            for entry in appended {
                writeln!(output, "{nodefmt} append {}", Self::format_entry(&entry))?
            }
            if old_commit_index != commit_index {
                writeln!(output, "{nodefmt} commit {commit_index}@{commit_term}")?;
            }
            for entry in self.applied_rx[&id].try_iter() {
                writeln!(output, "{nodefmt} apply {}", Self::format_entry(&entry))?
            }

            // Receive any outbound messages.
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
            let ids = self.parse_ids(args)?;
            if ids.is_empty() {
                return Ok(self.ids.clone());
            }
            Ok(ids)
        }

        // Parses node IDs from the given argument values, or errors if none.
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
                    "{} fully connected\n",
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
                    let len = symmetric.get(id).map(|p| p.len()).unwrap_or(0);
                    let peer_len = symmetric.get(peer).map(|p| p.len()).unwrap_or(0);
                    // If this peer set is the smallest (or we're the higher ID),
                    // remove the entry. We may no longer be in the map.
                    if len < peer_len || len == peer_len && id > peer {
                        if let Some(peers) = symmetric.get_mut(id) {
                            peers.remove(peer);
                            if peers.is_empty() {
                                symmetric.remove(id);
                            }
                        }
                    }
                }
            }

            // The values (HashSets) correspond to the RHS of a partition. Let's
            // group the LHS of the partition as well, from smallest to largest,
            // separately for symmetric and asymmetric partitions. The vector
            // contains (LHS, RHS, symmetric) groupings for each partition.
            let mut grouped: Vec<(HashSet<NodeID>, HashSet<NodeID>, bool)> = Vec::new();
            for (id, peers, symm) in symmetric
                .into_iter()
                .map(|(i, p)| (i, p, true))
                .chain(asymmetric.into_iter().map(|(i, p)| (i, p, false)))
                .sorted_by_key(|(id, _, symm)| (*id, !symm))
            {
                // Look for an existing LHS group with the same RHS, and insert
                // this node into it. Otherwise, create a new LHS group.
                match grouped.iter_mut().find(|(_, rhs, s)| peers == *rhs && symm == *s) {
                    Some((lhs, _, _)) => _ = lhs.insert(id),
                    None => grouped.push((HashSet::from([id]), peers, symm)),
                }
            }

            // Display the groups.
            for (lhs, rhs, symm) in grouped {
                let lhs = lhs.iter().sorted().map(|id| format!("n{id}")).join(" ");
                let sep = if symm { '⇹' } else { '⇥' };
                let rhs = rhs.iter().sorted().map(|id| format!("n{id}")).join(" ");
                writeln!(output, "{lhs} {sep} {rhs}").unwrap();
            }

            output
        }

        /// Formats an entry.
        fn format_entry(entry: &Entry) -> String {
            let command = match entry.command.as_ref() {
                Some(raw) => KVCommand::decode(raw).expect("invalid command").to_string(),
                None => "None".to_string(),
            };
            format!("{index}@{term} {command}", index = entry.index, term = entry.term)
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
                    format!(
                        "Heartbeat last_index={last_index} commit_index={commit_index} read_seq={read_seq}"
                    )
                }
                Message::HeartbeatResponse { match_index, read_seq } => {
                    format!("HeartbeatResponse match_index={match_index} read_seq={read_seq}")
                }
                Message::Append { base_index, base_term, entries } => {
                    let ent = entries.iter().map(|e| format!("{}@{}", e.index, e.term)).join(" ");
                    format!("Append base={base_index}@{base_term} [{ent}]")
                }
                Message::AppendResponse { match_index, reject_index } => {
                    match (match_index, reject_index) {
                        (0, 0) => panic!("match_index and reject_index both 0"),
                        (match_index, 0) => format!("AppendResponse match_index={match_index}"),
                        (0, reject_index) => format!("AppendResponse reject_index={reject_index}"),
                        (_, _) => panic!("match_index and reject_index both set"),
                    }
                }
                Message::Read { seq } => {
                    format!("Read seq={seq}")
                }
                Message::ReadResponse { seq } => {
                    format!("ReadResponse seq={seq}")
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
                            Err(error) => format!("Error::{error:#?}"),
                        }
                    )
                }
            }
        }

        /// Formats a node identifier.
        fn format_node(node: &Node) -> String {
            format!("n{id}@{term}", id = node.id(), term = node.term())
        }

        /// Formats a node identifier with role.
        fn format_node_role(node: &Node) -> String {
            let role = match node {
                Node::Candidate(_) => "candidate".to_string(),
                Node::Follower(node) => {
                    let leader = node.role.leader.map(|id| format!("n{id}")).unwrap_or_default();
                    format!("follower({leader})")
                }
                Node::Leader(_) => "leader".to_string(),
            };
            format!("{node} {role}", node = Self::format_node(node))
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
                Err(error) => format!("Error::{error:?} ({error})"),
            }
        }

        /// Strike-through formats the given string using a Unicode combining stroke.
        fn format_strikethrough(s: &str) -> String {
            s.chars().flat_map(|c| [c, '\u{0336}']).collect()
        }
    }
}
