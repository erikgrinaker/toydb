mod candidate;
mod follower;
mod leader;

use super::{Address, Driver, Event, Instruction, Log, Message, State};
use crate::storage::log;
use crate::Error;
use candidate::Candidate;
use follower::Follower;
use leader::Leader;

use ::log::{debug, info};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc;

/// The interval between leader heartbeats, in ticks.
const HEARTBEAT_INTERVAL: u64 = 1;

/// The minimum election timeout, in ticks.
const ELECTION_TIMEOUT_MIN: u64 = 8 * HEARTBEAT_INTERVAL;

/// The maximum election timeout, in ticks.
const ELECTION_TIMEOUT_MAX: u64 = 15 * HEARTBEAT_INTERVAL;

/// Node status
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub server: String,
    pub leader: String,
    pub term: u64,
    pub node_last_index: HashMap<String, u64>,
    pub commit_index: u64,
    pub apply_index: u64,
}

/// The local Raft node state machine.
pub enum Node<L: log::Store> {
    Candidate(RoleNode<Candidate, L>),
    Follower(RoleNode<Follower, L>),
    Leader(RoleNode<Leader, L>),
}

impl<L: log::Store> Node<L> {
    /// Creates a new Raft node, starting as a follower, or leader if no peers.
    pub async fn new<S: State + Send + 'static>(
        id: &str,
        peers: Vec<String>,
        log: Log<L>,
        mut state: S,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<Self, Error> {
        let applied_index = state.applied_index();
        if applied_index > log.commit_index {
            return Err(Error::Internal(format!(
                "State machine applied index {} greater than log committed index {}",
                applied_index, log.commit_index
            )));
        }

        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let mut driver = Driver::new(state_rx, node_tx.clone());
        if log.commit_index > applied_index {
            info!("Replaying log entries {} to {}", applied_index + 1, log.commit_index);
            driver.replay(&mut state, log.scan((applied_index + 1)..=log.commit_index))?;
        };
        tokio::spawn(driver.drive(state));

        let (term, voted_for) = log.load_term()?;
        let node = RoleNode {
            id: id.to_owned(),
            peers,
            term,
            log,
            node_tx,
            state_tx,
            queued_reqs: Vec::new(),
            proxied_reqs: HashMap::new(),
            role: Follower::new(None, voted_for.as_deref()),
        };
        if node.peers.is_empty() {
            info!("No peers specified, starting as leader");
            let last_index = node.log.last_index;
            Ok(node.become_role(Leader::new(vec![], last_index))?.into())
        } else {
            Ok(node.into())
        }
    }

    /// Returns the node ID.
    pub fn id(&self) -> String {
        match self {
            Node::Candidate(n) => n.id.clone(),
            Node::Follower(n) => n.id.clone(),
            Node::Leader(n) => n.id.clone(),
        }
    }

    /// Processes a message.
    pub fn step(self, msg: Message) -> Result<Self, Error> {
        debug!("Stepping {:?}", msg);
        match self {
            Node::Candidate(n) => n.step(msg),
            Node::Follower(n) => n.step(msg),
            Node::Leader(n) => n.step(msg),
        }
    }

    /// Moves time forward by a tick.
    pub fn tick(self) -> Result<Self, Error> {
        match self {
            Node::Candidate(n) => n.tick(),
            Node::Follower(n) => n.tick(),
            Node::Leader(n) => n.tick(),
        }
    }
}

impl<L: log::Store> From<RoleNode<Candidate, L>> for Node<L> {
    fn from(rn: RoleNode<Candidate, L>) -> Self {
        Node::Candidate(rn)
    }
}

impl<L: log::Store> From<RoleNode<Follower, L>> for Node<L> {
    fn from(rn: RoleNode<Follower, L>) -> Self {
        Node::Follower(rn)
    }
}

impl<L: log::Store> From<RoleNode<Leader, L>> for Node<L> {
    fn from(rn: RoleNode<Leader, L>) -> Self {
        Node::Leader(rn)
    }
}

// A Raft node with role R
pub struct RoleNode<R, L: log::Store> {
    id: String,
    peers: Vec<String>,
    term: u64,
    log: Log<L>,
    node_tx: mpsc::UnboundedSender<Message>,
    state_tx: mpsc::UnboundedSender<Instruction>,
    /// Keeps track of queued client requests received e.g. during elections.
    queued_reqs: Vec<(Address, Event)>,
    /// Keeps track of proxied client requests, to abort on new leader election.
    proxied_reqs: HashMap<Vec<u8>, Address>,
    role: R,
}

impl<R, L: log::Store> RoleNode<R, L> {
    /// Transforms the node into another role.
    fn become_role<T>(self, role: T) -> Result<RoleNode<T, L>, Error> {
        Ok(RoleNode {
            id: self.id,
            peers: self.peers,
            term: self.term,
            log: self.log,
            node_tx: self.node_tx,
            state_tx: self.state_tx,
            queued_reqs: self.queued_reqs,
            proxied_reqs: self.proxied_reqs,
            role,
        })
    }

    /// Aborts any proxied requests.
    fn abort_proxied(&mut self) -> Result<(), Error> {
        for (id, address) in std::mem::replace(&mut self.proxied_reqs, HashMap::new()) {
            self.send(address, Event::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    /// Sends any queued requests to the given leader.
    fn forward_queued(&mut self, leader: Address) -> Result<(), Error> {
        for (from, event) in std::mem::replace(&mut self.queued_reqs, Vec::new()) {
            if let Event::ClientRequest { id, .. } = &event {
                self.proxied_reqs.insert(id.clone(), from.clone());
                self.node_tx.send(Message {
                    from: match from {
                        Address::Client => Address::Local,
                        address => address,
                    },
                    to: leader.clone(),
                    term: 0,
                    event,
                })?;
            }
        }
        Ok(())
    }

    /// Returns the quorum size of the cluster.
    fn quorum(&self) -> u64 {
        (self.peers.len() as u64 + 1) / 2 + 1
    }

    /// Sends an event
    fn send(&self, to: Address, event: Event) -> Result<(), Error> {
        let msg = Message { term: self.term, from: Address::Local, to, event };
        debug!("Sending {:?}", msg);
        Ok(self.node_tx.send(msg)?)
    }

    /// Validates a message
    fn validate(&self, msg: &Message) -> Result<(), Error> {
        match msg.from {
            Address::Peers => return Err(Error::Internal("Message from broadcast address".into())),
            Address::Local => return Err(Error::Internal("Message from local node".into())),
            Address::Client if !matches!(msg.event, Event::ClientRequest{..}) => {
                return Err(Error::Internal("Non-request message from client".into()));
            }
            _ => {}
        }

        // Allowing requests and responses form past terms is fine, since they don't rely on it
        if msg.term < self.term
            && !matches!(msg.event, Event::ClientRequest{..} | Event::ClientResponse{..})
        {
            return Err(Error::Internal(format!("Message from past term {}", msg.term)));
        }

        match &msg.to {
            Address::Peer(id) if id == &self.id => Ok(()),
            Address::Local => Ok(()),
            Address::Peers => Ok(()),
            Address::Peer(id) => {
                Err(Error::Internal(format!("Received message for other node {}", id)))
            }
            Address::Client => Err(Error::Internal("Received message for client".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    pub use super::super::state::tests::TestState;
    use super::super::Entry;
    use super::follower::tests::{follower_leader, follower_voted_for};
    use super::*;
    use pretty_assertions::assert_eq;
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

    pub struct NodeAsserter<'a, L: log::Store> {
        node: &'a Node<L>,
    }

    impl<'a, L: log::Store> NodeAsserter<'a, L> {
        pub fn new(node: &'a Node<L>) -> Self {
            Self { node }
        }

        fn log(&self) -> &'a Log<L> {
            match self.node {
                Node::Candidate(n) => &n.log,
                Node::Follower(n) => &n.log,
                Node::Leader(n) => &n.log,
            }
        }

        pub fn committed(self, index: u64) -> Self {
            assert_eq!(index, self.log().commit_index, "Unexpected committed index");
            self
        }

        pub fn last(self, index: u64) -> Self {
            assert_eq!(index, self.log().last_index, "Unexpected last index");
            self
        }

        pub fn entry(self, entry: Entry) -> Self {
            assert!(entry.index <= self.log().last_index, "Index beyond last entry");
            assert_eq!(entry, self.log().get(entry.index).unwrap().unwrap());
            self
        }

        pub fn entries(self, entries: Vec<Entry>) -> Self {
            assert_eq!(entries, self.log().scan(0..).collect::<Result<Vec<_>, Error>>().unwrap());
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

        pub fn leader(self, leader: Option<&str>) -> Self {
            assert_eq!(
                leader.map(str::to_owned),
                match self.node {
                    Node::Candidate(_) => None,
                    Node::Follower(n) => follower_leader(n),
                    Node::Leader(_) => None,
                },
                "Unexpected leader",
            );
            self
        }

        pub fn proxied(self, proxied: Vec<(Vec<u8>, Address)>) -> Self {
            assert_eq!(
                &proxied.into_iter().collect::<HashMap<Vec<u8>, Address>>(),
                match self.node {
                    Node::Candidate(n) => &n.proxied_reqs,
                    Node::Follower(n) => &n.proxied_reqs,
                    Node::Leader(n) => &n.proxied_reqs,
                }
            );
            self
        }

        pub fn queued(self, queued: Vec<(Address, Event)>) -> Self {
            assert_eq!(
                &queued,
                match self.node {
                    Node::Candidate(n) => &n.queued_reqs,
                    Node::Follower(n) => &n.queued_reqs,
                    Node::Leader(n) => &n.queued_reqs,
                }
            );
            self
        }

        pub fn term(self, term: u64) -> Self {
            assert_eq!(
                term,
                match self.node {
                    Node::Candidate(n) => n.term,
                    Node::Follower(n) => n.term,
                    Node::Leader(n) => n.term,
                },
                "Unexpected node term",
            );
            let (saved_term, saved_voted_for) = self.log().load_term().unwrap();
            assert_eq!(saved_term, term, "Incorrect term stored in log");
            assert_eq!(
                saved_voted_for,
                match self.node {
                    Node::Candidate(_) => None,
                    Node::Follower(n) => follower_voted_for(n),
                    Node::Leader(_) => None,
                },
                "Incorrect voted_for stored in log"
            );
            self
        }

        pub fn voted_for(self, voted_for: Option<&str>) -> Self {
            assert_eq!(
                voted_for.map(str::to_owned),
                match self.node {
                    Node::Candidate(_) => None,
                    Node::Follower(n) => follower_voted_for(n),
                    Node::Leader(_) => None,
                },
                "Unexpected voted_for"
            );
            let (_, saved_voted_for) = self.log().load_term().unwrap();
            assert_eq!(saved_voted_for.as_deref(), voted_for, "Unexpected voted_for saved in log");
            self
        }
    }

    pub fn assert_node<L: log::Store>(node: &Node<L>) -> NodeAsserter<L> {
        NodeAsserter::new(node)
    }

    fn setup_rolenode() -> Result<(RoleNode<(), log::Test>, mpsc::UnboundedReceiver<Message>), Error>
    {
        setup_rolenode_peers(vec!["b".into(), "c".into()])
    }

    #[allow(clippy::type_complexity)]
    fn setup_rolenode_peers(
        peers: Vec<String>,
    ) -> Result<(RoleNode<(), log::Test>, mpsc::UnboundedReceiver<Message>), Error> {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        let (state_tx, _) = mpsc::unbounded_channel();
        let node = RoleNode {
            role: (),
            id: "a".into(),
            peers,
            term: 1,
            log: Log::new(log::Test::new())?,
            node_tx,
            state_tx,
            proxied_reqs: HashMap::new(),
            queued_reqs: Vec::new(),
        };
        Ok((node, node_rx))
    }

    #[tokio::test]
    async fn new() -> Result<(), Error> {
        let (node_tx, _) = mpsc::unbounded_channel();
        let node = Node::new(
            "a",
            vec!["b".into(), "c".into()],
            Log::new(log::Test::new())?,
            TestState::new(0),
            node_tx,
        )
        .await?;
        match node {
            Node::Follower(rolenode) => {
                assert_eq!(rolenode.id, "a".to_owned());
                assert_eq!(rolenode.term, 0);
                assert_eq!(rolenode.peers, vec!["b".to_owned(), "c".to_owned()]);
            }
            _ => panic!("Expected node to start as follower"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn new_loads_term() -> Result<(), Error> {
        let (node_tx, _) = mpsc::unbounded_channel();
        let store = log::Test::new();
        Log::new(store.clone())?.save_term(3, Some("c"))?;
        let node = Node::new(
            "a",
            vec!["b".into(), "c".into()],
            Log::new(store)?,
            TestState::new(0),
            node_tx,
        )
        .await?;
        match node {
            Node::Follower(rolenode) => assert_eq!(rolenode.term, 3),
            _ => panic!("Expected node to start as follower"),
        }
        Ok(())
    }

    #[tokio::test(core_threads = 2)]
    async fn new_state_apply_all() -> Result<(), Error> {
        let (node_tx, _) = mpsc::unbounded_channel();
        let mut log = Log::new(log::Test::new())?;
        log.append(1, Some(vec![0x01]))?;
        log.append(2, None)?;
        log.append(2, Some(vec![0x02]))?;
        log.commit(3)?;
        log.append(2, Some(vec![0x03]))?;
        let state = TestState::new(0);

        Node::new("a", vec!["b".into(), "c".into()], log, state.clone(), node_tx).await?;
        tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
        assert_eq!(state.list(), vec![vec![0x01], vec![0x02]]);
        assert_eq!(state.applied_index(), 3);
        Ok(())
    }

    #[tokio::test(core_threads = 2)]
    async fn new_state_apply_partial() -> Result<(), Error> {
        let (node_tx, _) = mpsc::unbounded_channel();
        let mut log = Log::new(log::Test::new())?;
        log.append(1, Some(vec![0x01]))?;
        log.append(2, None)?;
        log.append(2, Some(vec![0x02]))?;
        log.commit(3)?;
        log.append(2, Some(vec![0x03]))?;
        let state = TestState::new(2);

        Node::new("a", vec!["b".into(), "c".into()], log, state.clone(), node_tx).await?;
        tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
        assert_eq!(state.list(), vec![vec![0x02]]);
        assert_eq!(state.applied_index(), 3);
        Ok(())
    }

    #[tokio::test(core_threads = 2)]
    async fn new_state_apply_missing() -> Result<(), Error> {
        let (node_tx, _) = mpsc::unbounded_channel();
        let mut log = Log::new(log::Test::new())?;
        log.append(1, Some(vec![0x01]))?;
        log.append(2, None)?;
        log.append(2, Some(vec![0x02]))?;
        log.commit(3)?;
        log.append(2, Some(vec![0x03]))?;
        let state = TestState::new(4);

        assert_eq!(
            Node::new("a", vec!["b".into(), "c".into()], log, state.clone(), node_tx).await.err(),
            Some(Error::Internal(
                "State machine applied index 4 greater than log committed index 3".into()
            ))
        );
        Ok(())
    }

    #[tokio::test]
    async fn new_single() -> Result<(), Error> {
        let (node_tx, _) = mpsc::unbounded_channel();
        let node =
            Node::new("a", vec![], Log::new(log::Test::new())?, TestState::new(0), node_tx).await?;
        match node {
            Node::Leader(rolenode) => {
                assert_eq!(rolenode.id, "a".to_owned());
                assert_eq!(rolenode.term, 0);
                assert!(rolenode.peers.is_empty());
            }
            _ => panic!("Expected leader"),
        }
        Ok(())
    }

    #[test]
    fn become_role() -> Result<(), Error> {
        let (node, _) = setup_rolenode()?;
        let new = node.become_role("role")?;
        assert_eq!(new.id, "a".to_owned());
        assert_eq!(new.term, 1);
        assert_eq!(new.peers, vec!["b".to_owned(), "c".to_owned()]);
        assert_eq!(new.role, "role");
        Ok(())
    }

    #[test]
    fn quorum() -> Result<(), Error> {
        let quorums = vec![(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4), (8, 5)];
        for (size, quorum) in quorums.into_iter() {
            let peers: Vec<String> =
                (0..(size as u8 - 1)).map(|i| (i as char).to_string()).collect();
            assert_eq!(peers.len(), size as usize - 1);
            let (node, _) = setup_rolenode_peers(peers)?;
            assert_eq!(node.quorum(), quorum);
        }
        Ok(())
    }

    #[test]
    fn send() -> Result<(), Error> {
        let (node, mut rx) = setup_rolenode()?;
        node.send(Address::Peer("b".into()), Event::Heartbeat { commit_index: 1, commit_term: 1 })?;
        assert_messages(
            &mut rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 1,
                event: Event::Heartbeat { commit_index: 1, commit_term: 1 },
            }],
        );
        Ok(())
    }
}
