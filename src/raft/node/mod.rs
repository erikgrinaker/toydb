mod candidate;
mod follower;
mod leader;

use super::{Address, Driver, Event, Index, Instruction, Log, Message, State};
use crate::error::Result;
use candidate::Candidate;
use follower::Follower;
use leader::Leader;

use ::log::{debug, info};
use rand::Rng as _;
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

/// A node ID.
pub type NodeID = u8;

/// A leader term.
pub type Term = u64;

/// A logical clock interval as number of ticks.
pub type Ticks = u8;

/// The interval between leader heartbeats, in ticks.
const HEARTBEAT_INTERVAL: Ticks = 3;

/// The randomized election timeout range (min-max), in ticks. This is
/// randomized per node to avoid ties.
const ELECTION_TIMEOUT_RANGE: std::ops::Range<u8> = 10..20;

/// Generates a randomized election timeout.
fn rand_election_timeout() -> Ticks {
    rand::thread_rng().gen_range(ELECTION_TIMEOUT_RANGE)
}

/// Node status
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub server: NodeID,
    pub leader: NodeID,
    pub term: Term,
    pub node_last_index: HashMap<NodeID, Index>,
    pub commit_index: Index,
    pub apply_index: Index,
    pub storage: String,
    pub storage_size: u64,
}

/// The local Raft node state machine.
pub enum Node {
    Candidate(RoleNode<Candidate>),
    Follower(RoleNode<Follower>),
    Leader(RoleNode<Leader>),
}

impl Node {
    /// Creates a new Raft node, starting as a follower, or leader if no peers.
    pub async fn new(
        id: NodeID,
        peers: HashSet<NodeID>,
        mut log: Log,
        mut state: Box<dyn State>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<Self> {
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let mut driver = Driver::new(id, state_rx, node_tx.clone());
        driver.apply_log(&mut *state, &mut log)?;
        tokio::spawn(driver.drive(state));

        let (term, voted_for) = log.get_term()?;
        let mut node = RoleNode {
            id,
            peers,
            term,
            log,
            node_tx,
            state_tx,
            role: Follower::new(None, voted_for),
        };
        if node.peers.is_empty() {
            info!("No peers specified, starting as leader");
            // If we didn't vote for ourself in the persisted term, bump the
            // term and vote for ourself to ensure we have a valid leader term.
            if voted_for != Some(id) {
                node.term += 1;
                node.log.set_term(node.term, Some(id))?;
            }
            let (last_index, _) = node.log.get_last_index();
            Ok(node.become_role(Leader::new(HashSet::new(), last_index)).into())
        } else {
            Ok(node.into())
        }
    }

    /// Returns the node ID.
    pub fn id(&self) -> NodeID {
        match self {
            Node::Candidate(n) => n.id,
            Node::Follower(n) => n.id,
            Node::Leader(n) => n.id,
        }
    }

    /// Processes a message.
    pub fn step(self, msg: Message) -> Result<Self> {
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

impl From<RoleNode<Candidate>> for Node {
    fn from(rn: RoleNode<Candidate>) -> Self {
        Node::Candidate(rn)
    }
}

impl From<RoleNode<Follower>> for Node {
    fn from(rn: RoleNode<Follower>) -> Self {
        Node::Follower(rn)
    }
}

impl From<RoleNode<Leader>> for Node {
    fn from(rn: RoleNode<Leader>) -> Self {
        Node::Leader(rn)
    }
}

// A Raft node with role R
pub struct RoleNode<R> {
    id: NodeID,
    peers: HashSet<NodeID>,
    term: Term,
    log: Log,
    node_tx: mpsc::UnboundedSender<Message>,
    state_tx: mpsc::UnboundedSender<Instruction>,
    role: R,
}

impl<R> RoleNode<R> {
    /// Transforms the node into another role.
    fn become_role<T>(self, role: T) -> RoleNode<T> {
        RoleNode {
            id: self.id,
            peers: self.peers,
            term: self.term,
            log: self.log,
            node_tx: self.node_tx,
            state_tx: self.state_tx,
            role,
        }
    }

    /// Returns the quorum size of the cluster.
    fn quorum(&self) -> u64 {
        (self.peers.len() as u64 + 1) / 2 + 1
    }

    /// Sends an event
    fn send(&self, to: Address, event: Event) -> Result<()> {
        let msg = Message { term: self.term, from: Address::Node(self.id), to, event };
        debug!("Sending {:?}", msg);
        Ok(self.node_tx.send(msg)?)
    }

    /// Asserts common node invariants.
    fn assert_node(&mut self) -> Result<()> {
        debug_assert_eq!(self.term, self.log.get_term()?.0, "Term does not match log");
        Ok(())
    }

    /// Asserts message invariants when stepping.
    ///
    /// In a real production database, these should be errors instead, since
    /// external input from the network can't be trusted to uphold invariants.
    fn assert_step(&self, msg: &Message) {
        // Messages must be addressed to the local node, or a broadcast.
        match msg.to {
            Address::Broadcast => {}
            Address::Client => panic!("Message to client"),
            Address::Node(id) => assert_eq!(id, self.id, "Message to other node"),
        }

        match msg.from {
            // The broadcast address can't send anything.
            Address::Broadcast => panic!("Message from broadcast address"),
            // Clients can only send ClientRequest without a term.
            Address::Client => {
                assert_eq!(msg.term, 0, "Client message with term");
                assert!(
                    matches!(msg.event, Event::ClientRequest { .. }),
                    "Non-request message from client"
                );
            }
            // Nodes must be known, and must include their term.
            Address::Node(id) => {
                assert!(id == self.id || self.peers.contains(&id), "Unknown sender {}", id);
                // TODO: For now, accept ClientResponse without term, since the
                // state driver does not have access to it.
                assert!(
                    msg.term > 0 || matches!(msg.event, Event::ClientResponse { .. }),
                    "Message without term"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    pub use super::super::state::tests::TestState;
    use super::super::{Entry, RequestID};
    use super::follower::tests::{follower_leader, follower_voted_for};
    use super::*;
    use crate::storage;
    use pretty_assertions::assert_eq;
    use std::collections::HashSet;
    use tokio::sync::mpsc;

    pub fn assert_messages<T: std::fmt::Debug + PartialEq>(
        rx: &mut mpsc::UnboundedReceiver<T>,
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

        pub fn committed(mut self, index: Index) -> Self {
            assert_eq!(index, self.log().get_commit_index().0, "Unexpected committed index");
            self
        }

        pub fn last(mut self, index: Index) -> Self {
            assert_eq!(index, self.log().get_last_index().0, "Unexpected last index");
            self
        }

        pub fn entry(mut self, entry: Entry) -> Self {
            assert!(entry.index <= self.log().get_last_index().0, "Index beyond last entry");
            assert_eq!(entry, self.log().get(entry.index).unwrap().unwrap());
            self
        }

        pub fn entries(mut self, entries: Vec<Entry>) -> Self {
            assert_eq!(entries, self.log().scan(0..).unwrap().collect::<Result<Vec<_>>>().unwrap());
            self
        }

        #[allow(clippy::wrong_self_convention)]
        pub fn is_candidate(self) -> Self {
            match self.node {
                Node::Candidate(_) => self,
                Node::Follower(_) => panic!("Expected candidate, got follower"),
                Node::Leader(_) => panic!("Expected candidate, got leader"),
            }
        }

        #[allow(clippy::wrong_self_convention)]
        pub fn is_follower(self) -> Self {
            match self.node {
                Node::Candidate(_) => panic!("Expected follower, got candidate"),
                Node::Follower(_) => self,
                Node::Leader(_) => panic!("Expected follower, got leader"),
            }
        }

        #[allow(clippy::wrong_self_convention)]
        pub fn is_leader(self) -> Self {
            match self.node {
                Node::Candidate(_) => panic!("Expected leader, got candidate"),
                Node::Follower(_) => panic!("Expected leader, got follower"),
                Node::Leader(_) => self,
            }
        }

        pub fn leader(self, leader: Option<NodeID>) -> Self {
            assert_eq!(
                leader,
                match self.node {
                    Node::Candidate(_) => None,
                    Node::Follower(n) => follower_leader(n),
                    Node::Leader(_) => None,
                },
                "Unexpected leader",
            );
            self
        }

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
                    Node::Follower(n) => follower_voted_for(n),
                    Node::Leader(n) => Some(n.id),
                },
                "Incorrect voted_for stored in log"
            );
            self
        }

        pub fn voted_for(mut self, voted_for: Option<NodeID>) -> Self {
            assert_eq!(
                voted_for,
                match self.node {
                    Node::Candidate(_) => None,
                    Node::Follower(n) => follower_voted_for(n),
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

    fn setup_rolenode() -> Result<(RoleNode<()>, mpsc::UnboundedReceiver<Message>)> {
        setup_rolenode_peers(vec![2, 3])
    }

    fn setup_rolenode_peers(
        peers: Vec<NodeID>,
    ) -> Result<(RoleNode<()>, mpsc::UnboundedReceiver<Message>)> {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        let (state_tx, _) = mpsc::unbounded_channel();
        let node = RoleNode {
            role: (),
            id: 1,
            peers: HashSet::from_iter(peers),
            term: 1,
            log: Log::new(Box::new(storage::engine::Memory::new()), false)?,
            node_tx,
            state_tx,
        };
        Ok((node, node_rx))
    }

    #[tokio::test]
    async fn new() -> Result<()> {
        let (node_tx, _) = mpsc::unbounded_channel();
        let node = Node::new(
            1,
            HashSet::from([2, 3]),
            Log::new(Box::new(storage::engine::Memory::new()), false)?,
            Box::new(TestState::new(0)),
            node_tx,
        )
        .await?;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn new_state_apply_all() -> Result<()> {
        let (node_tx, _) = mpsc::unbounded_channel();
        let mut log = Log::new(Box::new(storage::engine::Memory::new()), false)?;
        log.append(1, Some(vec![0x01]))?;
        log.append(2, None)?;
        log.append(2, Some(vec![0x02]))?;
        log.commit(3)?;
        log.append(2, Some(vec![0x03]))?;
        let state = Box::new(TestState::new(0));

        Node::new(1, HashSet::from([2, 3]), log, state.clone(), node_tx).await?;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(state.list(), vec![vec![0x01], vec![0x02]]);
        assert_eq!(state.get_applied_index(), 3);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn new_state_apply_partial() -> Result<()> {
        let (node_tx, _) = mpsc::unbounded_channel();
        let mut log = Log::new(Box::new(storage::engine::Memory::new()), false)?;
        log.append(1, Some(vec![0x01]))?;
        log.append(2, None)?;
        log.append(2, Some(vec![0x02]))?;
        log.commit(3)?;
        log.append(2, Some(vec![0x03]))?;
        let state = Box::new(TestState::new(2));

        Node::new(1, HashSet::from([2, 3]), log, state.clone(), node_tx).await?;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(state.list(), vec![vec![0x02]]);
        assert_eq!(state.get_applied_index(), 3);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[should_panic(expected = "applied index above commit index")]
    async fn new_state_apply_missing() {
        let (node_tx, _) = mpsc::unbounded_channel();
        let mut log = Log::new(Box::new(storage::engine::Memory::new()), false).unwrap();
        log.append(1, Some(vec![0x01])).unwrap();
        log.append(2, None).unwrap();
        log.append(2, Some(vec![0x02])).unwrap();
        log.commit(3).unwrap();
        log.append(2, Some(vec![0x03])).unwrap();
        let state = Box::new(TestState::new(4));

        Node::new(1, HashSet::from([2, 3]), log, state.clone(), node_tx).await.unwrap();
    }

    #[tokio::test]
    async fn new_single() -> Result<()> {
        let (node_tx, _) = mpsc::unbounded_channel();
        let node = Node::new(
            1,
            HashSet::new(),
            Log::new(Box::new(storage::engine::Memory::new()), false)?,
            Box::new(TestState::new(0)),
            node_tx,
        )
        .await?;
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
    fn become_role() -> Result<()> {
        let (node, _) = setup_rolenode()?;
        let new = node.become_role("role");
        assert_eq!(new.id, 1);
        assert_eq!(new.term, 1);
        assert_eq!(new.peers, HashSet::from([2, 3]));
        assert_eq!(new.role, "role");
        Ok(())
    }

    #[test]
    fn quorum() -> Result<()> {
        let quorums = vec![(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4), (8, 5)];
        for (size, quorum) in quorums.into_iter() {
            let peers: Vec<NodeID> = (1..(size as u8)).collect();
            assert_eq!(peers.len(), size as usize - 1);
            let (node, _) = setup_rolenode_peers(peers)?;
            assert_eq!(node.quorum(), quorum);
        }
        Ok(())
    }

    #[test]
    fn send() -> Result<()> {
        let (node, mut rx) = setup_rolenode()?;
        node.send(Address::Node(2), Event::Heartbeat { commit_index: 1, commit_term: 1 })?;
        assert_messages(
            &mut rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 1,
                event: Event::Heartbeat { commit_index: 1, commit_term: 1 },
            }],
        );
        Ok(())
    }
}
