mod candidate;
mod follower;
mod leader;

use super::{Event, Log, Message, State};
use crate::kv::storage::Storage;
use crate::Error;
use candidate::Candidate;
use follower::Follower;
use leader::Leader;

use log::{debug, info, warn};
use serde_derive::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

/// The interval between leader heartbeats, in ticks.
const HEARTBEAT_INTERVAL: u64 = 1;

/// The minimum election timeout, in ticks.
const ELECTION_TIMEOUT_MIN: u64 = 8 * HEARTBEAT_INTERVAL;

/// The maximum election timeout, in ticks.
const ELECTION_TIMEOUT_MAX: u64 = 15 * HEARTBEAT_INTERVAL;

/// Node status
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub id: String,
    pub role: String,
    pub leader: Option<String>,
    pub nodes: u64,
    pub term: u64,
    pub entries: u64,
    pub committed: u64,
    pub applied: u64,
}

/// The local Raft node state machine.
pub enum Node<L: Storage, S: State> {
    Candidate(RoleNode<Candidate, L, S>),
    Follower(RoleNode<Follower, L, S>),
    Leader(RoleNode<Leader, L, S>),
}

impl<L: Storage, S: State> Node<L, S> {
    /// Creates a new Raft node, starting as a follower, or leader if no peers.
    pub fn new(
        id: &str,
        peers: Vec<String>,
        log: Log<L>,
        state: S,
        sender: UnboundedSender<Message>,
    ) -> Result<Self, Error> {
        let (term, voted_for) = log.load_term()?;
        let node = RoleNode {
            id: id.to_owned(),
            peers,
            term,
            log,
            state,
            sender,
            role: Follower::new(None, voted_for.as_deref()),
        };
        if node.peers.is_empty() {
            info!("No peers specified, starting as leader");
            let (last_index, _) = node.log.get_last();
            Ok(node.become_role(Leader::new(vec![], last_index))?.into())
        } else {
            Ok(node.into())
        }
    }

    /// Returns node status.
    pub fn status(&self) -> Result<Status, Error> {
        let mut status = match self {
            Node::Candidate(n) => n.status(),
            Node::Follower(n) => n.status(),
            Node::Leader(n) => n.status(),
        }?;
        status.role = match self {
            Node::Candidate(_) => "candidate".into(),
            Node::Follower(_) => "follower".into(),
            Node::Leader(_) => "leader".into(),
        };
        status.leader = match self {
            Node::Candidate(_) => None,
            Node::Follower(n) => n.role.leader.clone(),
            Node::Leader(n) => Some(n.id.clone()),
        };
        Ok(status)
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

impl<L: Storage, S: State> From<RoleNode<Candidate, L, S>> for Node<L, S> {
    fn from(rn: RoleNode<Candidate, L, S>) -> Self {
        Node::Candidate(rn)
    }
}

impl<L: Storage, S: State> From<RoleNode<Follower, L, S>> for Node<L, S> {
    fn from(rn: RoleNode<Follower, L, S>) -> Self {
        Node::Follower(rn)
    }
}

impl<L: Storage, S: State> From<RoleNode<Leader, L, S>> for Node<L, S> {
    fn from(rn: RoleNode<Leader, L, S>) -> Self {
        Node::Leader(rn)
    }
}

// A Raft node with role R
pub struct RoleNode<R, L: Storage, S: State> {
    id: String,
    peers: Vec<String>,
    term: u64,
    log: Log<L>,
    state: S,
    sender: UnboundedSender<Message>,
    role: R,
}

impl<R, L: Storage, S: State> RoleNode<R, L, S> {
    /// Transforms the node into another role.
    fn become_role<T>(self, role: T) -> Result<RoleNode<T, L, S>, Error> {
        Ok(RoleNode {
            id: self.id,
            peers: self.peers,
            term: self.term,
            log: self.log,
            state: self.state,
            sender: self.sender,
            role,
        })
    }

    /// Broadcasts an event to all peers.
    fn broadcast(&self, event: Event) -> Result<(), Error> {
        for peer in self.peers.iter() {
            self.send(Some(peer), event.clone())?
        }
        Ok(())
    }

    /// Normalizes and validates a message, ensuring it is addressed
    /// to the local node and term. On any errors it emits a warning and
    /// returns false.
    fn normalize_message(&self, msg: &mut Message) -> bool {
        msg.normalize(&self.id, self.term);
        if let Err(err) = msg.validate(&self.id, self.term) {
            warn!("{}", err);
            false
        } else {
            true
        }
    }

    /// Returns the quorum size of the cluster.
    fn quorum(&self) -> u64 {
        (self.peers.len() as u64 + 1) / 2 + 1
    }

    /// Updates the current term and stores it in the log
    fn save_term(&mut self, term: u64, voted_for: Option<&str>) -> Result<(), Error> {
        self.log.save_term(term, voted_for)?;
        self.term = term;
        Ok(())
    }

    /// Sends an event to a peer, or local caller if None
    fn send(&self, to: Option<&str>, event: Event) -> Result<(), Error> {
        let msg = Message {
            term: self.term,
            from: Some(self.id.clone()),
            to: to.map(str::to_owned),
            event,
        };
        debug!("Sending {:?}", msg);
        Ok(self.sender.send(msg)?)
    }

    /// Returns the node status.
    fn status(&self) -> Result<Status, Error> {
        let (entries, _) = self.log.get_last();
        let (committed, _) = self.log.get_committed();
        let (applied, _) = self.log.get_applied();

        Ok(Status {
            id: self.id.clone(),
            role: "".into(),
            leader: None,
            nodes: self.peers.len() as u64 + 1,
            term: self.term,
            entries,
            committed,
            applied,
        })
    }
}

#[cfg(test)]
mod tests {
    pub use super::super::state::tests::TestState;
    use super::super::Entry;
    use super::follower::tests::{follower_leader, follower_voted_for};
    use super::*;
    use crate::kv;
    use tokio::sync::mpsc::UnboundedReceiver;

    pub fn assert_messages(rx: &mut UnboundedReceiver<Message>, msgs: Vec<Message>) {
        let mut actual = Vec::new();
        while let Ok(message) = rx.try_recv() {
            actual.push(message)
        }
        assert_eq!(msgs, actual);
    }

    pub struct NodeAsserter<'a, L: Storage, S: State> {
        node: &'a Node<L, S>,
    }

    impl<'a, L: Storage, S: State> NodeAsserter<'a, L, S> {
        pub fn new(node: &'a Node<L, S>) -> Self {
            Self { node }
        }

        fn log(&self) -> &'a Log<L> {
            match self.node {
                Node::Candidate(n) => &n.log,
                Node::Follower(n) => &n.log,
                Node::Leader(n) => &n.log,
            }
        }

        pub fn applied(self, index: u64) -> Self {
            let (apply_index, _) = self.log().get_applied();
            assert_eq!(index, apply_index, "Unexpected applied index");
            self
        }

        pub fn committed(self, index: u64) -> Self {
            let (commit_index, _) = self.log().get_committed();
            assert_eq!(index, commit_index, "Unexpected committed index");
            self
        }

        pub fn last(self, index: u64) -> Self {
            let (last_index, _) = self.log().get_last();
            assert_eq!(index, last_index, "Unexpected last index");
            self
        }

        pub fn entry(self, index: u64, entry: Entry) -> Self {
            let (last_index, _) = self.log().get_last();
            assert!(index <= last_index, "Index beyond last entry");
            assert_eq!(entry, self.log().get(index).unwrap().unwrap());
            self
        }

        pub fn entries(self, entries: Vec<Entry>) -> Self {
            assert_eq!(entries, self.log().range(0..).unwrap());
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

    pub fn assert_node<L: Storage, S: State>(node: &Node<L, S>) -> NodeAsserter<L, S> {
        NodeAsserter::new(node)
    }

    #[allow(clippy::type_complexity)]
    fn setup_rolenode(
    ) -> Result<(RoleNode<(), kv::storage::Test, TestState>, UnboundedReceiver<Message>), Error>
    {
        setup_rolenode_peers(vec!["b".into(), "c".into()])
    }

    #[allow(clippy::type_complexity)]
    fn setup_rolenode_peers(
        peers: Vec<String>,
    ) -> Result<(RoleNode<(), kv::storage::Test, TestState>, UnboundedReceiver<Message>), Error>
    {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let node = RoleNode {
            role: (),
            id: "a".into(),
            peers,
            term: 1,
            log: Log::new(kv::Simple::new(kv::storage::Test::new()))?,
            state: TestState::new(),
            sender,
        };
        Ok((node, receiver))
    }

    #[test]
    fn new() -> Result<(), Error> {
        let (sender, _) = tokio::sync::mpsc::unbounded_channel();
        let node = Node::new(
            "a",
            vec!["b".into(), "c".into()],
            Log::new(kv::Simple::new(kv::storage::Test::new()))?,
            TestState::new(),
            sender,
        )?;
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

    #[test]
    fn new_loads_term() -> Result<(), Error> {
        let (sender, _) = tokio::sync::mpsc::unbounded_channel();
        let storage = kv::storage::Test::new();
        Log::new(kv::Simple::new(storage.clone()))?.save_term(3, Some("c"))?;
        let node = Node::new(
            "a",
            vec!["b".into(), "c".into()],
            Log::new(kv::Simple::new(storage))?,
            TestState::new(),
            sender,
        )?;
        match node {
            Node::Follower(rolenode) => assert_eq!(rolenode.term, 3),
            _ => panic!("Expected node to start as follower"),
        }
        Ok(())
    }

    #[test]
    fn new_single() -> Result<(), Error> {
        let (sender, _) = tokio::sync::mpsc::unbounded_channel();
        let node = Node::new(
            "a",
            vec![],
            Log::new(kv::Simple::new(kv::storage::Test::new()))?,
            TestState::new(),
            sender,
        )?;
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
    fn broadcast() -> Result<(), Error> {
        let (node, mut rx) = setup_rolenode()?;
        node.broadcast(Event::Heartbeat { commit_index: 1, commit_term: 1 })?;

        for to in ["b", "c"].iter().cloned() {
            assert_eq!(
                rx.try_recv()?,
                Message {
                    from: Some("a".into()),
                    to: Some(to.into()),
                    term: 1,
                    event: Event::Heartbeat { commit_index: 1, commit_term: 1 },
                },
            )
        }
        assert_messages(&mut rx, vec![]);
        Ok(())
    }

    #[test]
    fn normalize_message() -> Result<(), Error> {
        let (node, _) = setup_rolenode()?;
        let mut msg = Message {
            from: None,
            to: None,
            term: 0,
            event: Event::QueryState { call_id: vec![], command: vec![] },
        };
        assert!(node.normalize_message(&mut msg));
        assert_eq!(
            msg,
            Message {
                from: None,
                to: Some("a".into()),
                term: 1,
                event: Event::QueryState { call_id: vec![], command: vec![] },
            }
        );

        msg.to = Some("c".into());
        assert!(!node.normalize_message(&mut msg));
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
        node.send(Some("b"), Event::Heartbeat { commit_index: 1, commit_term: 1 })?;
        assert_messages(
            &mut rx,
            vec![Message {
                from: Some("a".into()),
                to: Some("b".into()),
                term: 1,
                event: Event::Heartbeat { commit_index: 1, commit_term: 1 },
            }],
        );
        Ok(())
    }

    #[test]
    fn save_term() -> Result<(), Error> {
        let (mut node, _) = setup_rolenode()?;
        node.save_term(4, Some("b"))?;
        assert_eq!(node.log.load_term()?, (4, Some("b".into())));
        Ok(())
    }
}
