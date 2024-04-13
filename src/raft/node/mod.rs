mod candidate;
mod follower;
mod leader;

use super::{Envelope, Index, Log, Message, State, ELECTION_TIMEOUT_RANGE};
use crate::error::{Error, Result};
use candidate::Candidate;
use follower::Follower;
use leader::Leader;

use itertools::Itertools as _;
use log::debug;
use rand::Rng as _;
use std::collections::HashSet;

/// A node ID.
pub type NodeID = u8;

/// A leader term.
pub type Term = u64;

/// A logical clock interval as number of ticks.
pub type Ticks = u8;

/// Generates a randomized election timeout.
fn rand_election_timeout() -> Ticks {
    rand::thread_rng().gen_range(ELECTION_TIMEOUT_RANGE)
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
    ) -> Result<Self> {
        let node = RawNode::new(id, peers, log, state, node_tx)?;
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
    pub use super::super::state::tests::TestState;
    use super::super::{Entry, RequestID};
    use super::*;
    use crate::storage;
    use pretty_assertions::assert_eq;
    use std::collections::HashSet;

    #[track_caller]
    pub fn assert_messages<T: std::fmt::Debug + PartialEq>(
        rx: &mut crossbeam::channel::Receiver<T>,
        msgs: Vec<T>,
    ) {
        let mut actual = Vec::new();
        while let Ok(message) = rx.try_recv() {
            actual.push(message)
        }
        assert_eq!(msgs, actual);
    }

    pub struct NodeAsserter<'a> {
        node: &'a mut Node,
    }

    impl<'a> NodeAsserter<'a> {
        pub fn new(node: &'a mut Node) -> Self {
            Self { node }
        }

        fn log(&mut self) -> &'_ mut Log {
            match self.node {
                Node::Candidate(n) => &mut n.log,
                Node::Follower(n) => &mut n.log,
                Node::Leader(n) => &mut n.log,
            }
        }

        #[track_caller]
        fn state(&mut self) -> &'_ mut Box<dyn State> {
            match self.node {
                Node::Candidate(n) => &mut n.state,
                Node::Follower(n) => &mut n.state,
                Node::Leader(n) => &mut n.state,
            }
        }

        #[track_caller]
        pub fn committed(mut self, index: Index) -> Self {
            assert_eq!(index, self.log().get_commit_index().0, "Unexpected committed index");
            self
        }

        #[track_caller]
        pub fn applied(mut self, index: Index) -> Self {
            assert_eq!(index, self.state().get_applied_index(), "Unexpected applied index");
            self
        }

        #[track_caller]
        pub fn last(mut self, index: Index) -> Self {
            assert_eq!(index, self.log().get_last_index().0, "Unexpected last index");
            self
        }

        #[track_caller]
        pub fn entry(mut self, entry: Entry) -> Self {
            assert!(entry.index <= self.log().get_last_index().0, "Index beyond last entry");
            assert_eq!(entry, self.log().get(entry.index).unwrap().unwrap());
            self
        }

        #[track_caller]
        pub fn entries(mut self, entries: Vec<Entry>) -> Self {
            assert_eq!(entries, self.log().scan(0..).unwrap().collect::<Result<Vec<_>>>().unwrap());
            self
        }

        #[allow(clippy::wrong_self_convention)]
        #[track_caller]
        pub fn is_candidate(self) -> Self {
            match self.node {
                Node::Candidate(_) => self,
                Node::Follower(_) => panic!("Expected candidate, got follower"),
                Node::Leader(_) => panic!("Expected candidate, got leader"),
            }
        }

        #[allow(clippy::wrong_self_convention)]
        #[track_caller]
        pub fn is_follower(self) -> Self {
            match self.node {
                Node::Candidate(_) => panic!("Expected follower, got candidate"),
                Node::Follower(_) => self,
                Node::Leader(_) => panic!("Expected follower, got leader"),
            }
        }

        #[allow(clippy::wrong_self_convention)]
        #[track_caller]
        pub fn is_leader(self) -> Self {
            match self.node {
                Node::Candidate(_) => panic!("Expected leader, got candidate"),
                Node::Follower(_) => panic!("Expected leader, got follower"),
                Node::Leader(_) => self,
            }
        }

        #[track_caller]
        pub fn leader(self, leader: Option<NodeID>) -> Self {
            assert_eq!(
                leader,
                match self.node {
                    Node::Candidate(_) => None,
                    Node::Follower(n) => n.role.leader,
                    Node::Leader(_) => None,
                },
                "Unexpected leader",
            );
            self
        }

        #[track_caller]
        pub fn forwarded(self, forwarded: Vec<RequestID>) -> Self {
            assert_eq!(
                forwarded.into_iter().collect::<HashSet<RequestID>>(),
                match self.node {
                    Node::Candidate(_) => HashSet::new(),
                    Node::Follower(n) => n.role.forwarded.clone(),
                    Node::Leader(_) => HashSet::new(),
                }
            );
            self
        }

        #[track_caller]
        pub fn term(mut self, term: Term) -> Self {
            assert_eq!(
                term,
                match self.node {
                    Node::Candidate(n) => n.term,
                    Node::Follower(n) => n.term,
                    Node::Leader(n) => n.term,
                },
                "Unexpected node term",
            );
            let (saved_term, saved_voted_for) = self.log().get_term().unwrap();
            assert_eq!(saved_term, term, "Incorrect term stored in log");
            assert_eq!(
                saved_voted_for,
                match self.node {
                    Node::Candidate(n) => Some(n.id),
                    Node::Follower(n) => n.role.voted_for,
                    Node::Leader(n) => Some(n.id),
                },
                "Incorrect voted_for stored in log"
            );
            self
        }

        #[track_caller]
        pub fn voted_for(mut self, voted_for: Option<NodeID>) -> Self {
            assert_eq!(
                voted_for,
                match self.node {
                    Node::Candidate(_) => None,
                    Node::Follower(n) => n.role.voted_for,
                    Node::Leader(_) => None,
                },
                "Unexpected voted_for"
            );
            let (_, saved_voted_for) = self.log().get_term().unwrap();
            assert_eq!(saved_voted_for, voted_for, "Unexpected voted_for saved in log");
            self
        }
    }

    pub fn assert_node(node: &mut Node) -> NodeAsserter {
        NodeAsserter::new(node)
    }

    fn setup_rolenode() -> Result<(RawNode<Follower>, crossbeam::channel::Receiver<Envelope>)> {
        setup_rolenode_peers(vec![2, 3])
    }

    fn setup_rolenode_peers(
        peers: Vec<NodeID>,
    ) -> Result<(RawNode<Follower>, crossbeam::channel::Receiver<Envelope>)> {
        let (node_tx, node_rx) = crossbeam::channel::unbounded();
        let node = RawNode {
            role: Follower::new(None, None),
            id: 1,
            peers: HashSet::from_iter(peers),
            term: 1,
            log: Log::new(storage::Memory::new(), false)?,
            state: Box::new(TestState::new(0)),
            node_tx,
        };
        Ok((node, node_rx))
    }

    #[test]
    fn new() -> Result<()> {
        let (node_tx, _) = crossbeam::channel::unbounded();
        let node = Node::new(
            1,
            HashSet::from([2, 3]),
            Log::new(storage::Memory::new(), false)?,
            Box::new(TestState::new(0)),
            node_tx,
        )?;
        match node {
            Node::Follower(rolenode) => {
                assert_eq!(rolenode.id, 1);
                assert_eq!(rolenode.term, 0);
                assert_eq!(rolenode.peers, HashSet::from([2, 3]));
            }
            _ => panic!("Expected node to start as follower"),
        }
        Ok(())
    }

    #[test]
    fn new_single() -> Result<()> {
        let (node_tx, _node_rx) = crossbeam::channel::unbounded();
        let node = Node::new(
            1,
            HashSet::new(),
            Log::new(storage::Memory::new(), false)?,
            Box::new(TestState::new(0)),
            node_tx,
        )?;
        match node {
            Node::Leader(rolenode) => {
                assert_eq!(rolenode.id, 1);
                assert_eq!(rolenode.term, 1);
                assert!(rolenode.peers.is_empty());
            }
            _ => panic!("Expected leader"),
        }
        Ok(())
    }

    #[test]
    fn into_role() -> Result<()> {
        let (node, _) = setup_rolenode()?;
        let role = Candidate::new();
        let new = node.into_role(role.clone());
        assert_eq!(new.id, 1);
        assert_eq!(new.term, 1);
        assert_eq!(new.peers, HashSet::from([2, 3]));
        assert_eq!(new.role, role);
        Ok(())
    }

    #[test]
    fn send() -> Result<()> {
        let (node, mut rx) = setup_rolenode()?;
        node.send(2, Message::Heartbeat { commit_index: 1, commit_term: 1, read_seq: 7 })?;
        assert_messages(
            &mut rx,
            vec![Envelope {
                from: 1,
                to: 2,
                term: 1,
                message: Message::Heartbeat { commit_index: 1, commit_term: 1, read_seq: 7 },
            }],
        );
        Ok(())
    }

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
}
