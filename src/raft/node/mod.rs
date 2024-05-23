mod candidate;
mod follower;
mod leader;

use super::{Envelope, Index, Log, Message, State};
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

    // Run goldenscript tests in src/raft/testscripts/.
    test_each_path! { in "src/raft/testscripts" as scripts => test_goldenscript }

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
                    let mut nodes = 0;
                    let mut leader = None;
                    let mut heartbeat_interval = HEARTBEAT_INTERVAL;
                    let mut election_timeout = ELECTION_TIMEOUT_RANGE.start;
                    for arg in &command.args {
                        match arg.key.as_deref() {
                            Some("election_timeout") => election_timeout = arg.parse()?,
                            Some("heartbeat_interval") => heartbeat_interval = arg.parse()?,
                            Some("leader") => leader = Some(arg.parse()?),
                            Some("nodes") => nodes = arg.parse()?,
                            _ => return Err(format!("invalid argument '{}'", arg.name()).into()),
                        }
                    }
                    self.cluster(nodes, leader, heartbeat_interval, election_timeout, &mut output)?;
                }

                // deliver [from=ID] [ID...]
                //
                // Delivers (steps) pending messages to the given nodes. If from
                // is given, only messages from the given node is delivered, the
                // others are left pending.
                "deliver" => {
                    let ids = self.parse_ids_or_all(&command.pos_args())?;
                    let mut from = None;
                    for arg in command.key_args() {
                        match arg.key.as_deref().unwrap() {
                            "from" => from = Some(arg.parse()?),
                            key => return Err(format!("invalid argument '{key}'").into()),
                        }
                    }
                    self.deliver(&ids, from, &mut output)?;
                }

                // get ID KEY
                //
                // Sends a client request to the given node to read the given
                // key from the state machine (key/value store).
                "get" => {
                    self.reject_args(&command.key_args())?;
                    let mut args = command.pos_args().into_iter();
                    let id = args.next().ok_or("must specify node ID")?.parse()?;
                    let key = args.next().ok_or("must specify key")?.value.clone();
                    self.reject_args(&args.collect_vec())?;
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
                    let mut args = command.args.iter();
                    let id = args.next().ok_or("must specify node ID")?.parse()?;
                    let kv = args.next().ok_or("must specify key/value pair")?;
                    let key = kv.key.clone().ok_or("must specify key/value pair")?;
                    let value = kv.value.clone();
                    self.reject_args(&args.collect_vec())?;
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
                    let ids = self.parse_ids_or_all(&command.pos_args())?;
                    let mut heartbeat = false;
                    for arg in command.key_args() {
                        match arg.key.as_deref().unwrap() {
                            "heartbeat" => heartbeat = arg.parse()?,
                            key => return Err(format!("invalid argument '{key}'").into()),
                        }
                    }
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
                    let ids = self.parse_ids_or_all(&command.pos_args())?;
                    let mut request = false;
                    for arg in command.key_args() {
                        match arg.key.as_deref().unwrap() {
                            "request" => request = arg.parse()?,
                            key => return Err(format!("invalid argument '{key}'").into()),
                        }
                    }
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
                    let mut pos_args = command.pos_args().into_iter();
                    let id = pos_args.next().ok_or("node ID not given")?.parse()?;
                    let raw = pos_args.next().ok_or("message not given")?.value.clone();
                    let msg = serde_json::from_str(&raw)?;
                    self.reject_args(&pos_args.collect_vec())?;
                    let mut panic = false;
                    for arg in command.key_args() {
                        match arg.key.as_deref().unwrap() {
                            "panic" => panic = arg.parse()?,
                            key => return Err(format!("unknown key '{key}'").into()),
                        }
                    }
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

        // Errors if any arguments are passed.
        fn reject_args<A>(&self, args: &[A]) -> Result<(), Box<dyn Error>>
        where
            A: Borrow<goldenscript::Argument>,
        {
            if let Some(arg) = args.first().map(|a| a.borrow()) {
                return Err(format!("unexpected argument '{}'", arg.name()).into());
            }
            Ok(())
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
