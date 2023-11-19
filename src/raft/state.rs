use super::{Address, Entry, Event, Index, Log, Message, NodeID, Response, Status, Term};
use crate::error::{Error, Result};

use log::{debug, error};
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt as _;

/// A Raft-managed state machine.
pub trait State: Send {
    /// Returns the last applied index from the state machine.
    fn get_applied_index(&self) -> Index;

    /// Applies a log entry to the state machine. If it returns Error::Internal,
    /// the Raft node halts. Any other error is considered applied and returned
    /// to the caller.
    ///
    /// The entry may contain a noop command, which is committed by Raft during
    /// leader changes. This still needs to be applied to the state machine to
    /// properly track the applied index, and returns an empty result.
    ///
    /// TODO: consider using runtime assertions instead of Error::Internal.
    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>>;

    /// Queries the state machine. All errors are propagated to the caller.
    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}

#[derive(Debug, PartialEq)]
/// A driver instruction.
pub enum Instruction {
    /// Abort all pending operations, e.g. due to leader change.
    Abort,
    /// Apply a log entry.
    Apply { entry: Entry },
    /// Notify the given address with the result of applying the entry at the given index.
    Notify { id: Vec<u8>, address: Address, index: Index },
    /// Query the state machine when the given term and index has been confirmed by vote.
    Query { id: Vec<u8>, address: Address, command: Vec<u8>, term: Term, index: Index, quorum: u64 },
    /// Extend the given server status and return it to the given address.
    Status { id: Vec<u8>, address: Address, status: Box<Status> },
    /// Votes for queries at the given term and commit index.
    Vote { term: Term, index: Index, address: Address },
}

/// A driver query.
struct Query {
    id: Vec<u8>,
    term: Term,
    address: Address,
    command: Vec<u8>,
    quorum: u64,
    votes: HashSet<Address>,
}

/// Drives a state machine, taking operations from state_rx and sending results via node_tx.
pub struct Driver {
    node_id: NodeID,
    state_rx: UnboundedReceiverStream<Instruction>,
    node_tx: mpsc::UnboundedSender<Message>,
    /// Notify clients when their mutation is applied. <index, (client, id)>
    notify: HashMap<Index, (Address, Vec<u8>)>,
    /// Execute client queries when they receive a quorum. <index, <id, query>>
    queries: BTreeMap<Index, BTreeMap<Vec<u8>, Query>>,
}

impl Driver {
    /// Creates a new state machine driver.
    pub fn new(
        node_id: NodeID,
        state_rx: mpsc::UnboundedReceiver<Instruction>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Self {
        Self {
            node_id,
            state_rx: UnboundedReceiverStream::new(state_rx),
            node_tx,
            notify: HashMap::new(),
            queries: BTreeMap::new(),
        }
    }

    /// Drives a state machine.
    pub async fn drive(mut self, mut state: Box<dyn State>) -> Result<()> {
        debug!("Starting state machine driver at applied index {}", state.get_applied_index());
        while let Some(instruction) = self.state_rx.next().await {
            if let Err(error) = self.execute(instruction, &mut *state) {
                error!("Halting state machine due to error: {}", error);
                return Err(error);
            }
        }
        debug!("Stopping state machine driver");
        Ok(())
    }

    /// Applies committed log entries to the state machine.
    pub fn apply_log(&mut self, state: &mut dyn State, log: &mut Log) -> Result<Index> {
        let applied_index = state.get_applied_index();
        let (commit_index, _) = log.get_commit_index();
        assert!(applied_index <= commit_index, "applied index above commit index");

        if applied_index < commit_index {
            let mut scan = log.scan((applied_index + 1)..=commit_index)?;
            while let Some(entry) = scan.next().transpose()? {
                self.apply(state, entry)?;
            }
        }
        Ok(state.get_applied_index())
    }

    /// Applies an entry to the state machine.
    pub fn apply(&mut self, state: &mut dyn State, entry: Entry) -> Result<Index> {
        // Apply the command.
        debug!("Applying {:?}", entry);
        match state.apply(entry) {
            Err(error @ Error::Internal(_)) => return Err(error),
            result => self.notify_applied(state.get_applied_index(), result)?,
        };
        // Try to execute any pending queries, since they may have been submitted for a
        // commit_index which hadn't been applied yet.
        self.query_execute(state)?;
        Ok(state.get_applied_index())
    }

    /// Executes a state machine instruction.
    fn execute(&mut self, i: Instruction, state: &mut dyn State) -> Result<()> {
        debug!("Executing {:?}", i);
        match i {
            Instruction::Abort => {
                self.notify_abort()?;
                self.query_abort()?;
            }

            Instruction::Apply { entry } => {
                self.apply(state, entry)?;
            }

            Instruction::Notify { id, address, index } => {
                if index > state.get_applied_index() {
                    self.notify.insert(index, (address, id));
                } else {
                    self.send(address, Event::ClientResponse { id, response: Err(Error::Abort) })?;
                }
            }

            Instruction::Query { id, address, command, index, term, quorum } => {
                self.queries.entry(index).or_default().insert(
                    id.clone(),
                    Query { id, term, address, command, quorum, votes: HashSet::new() },
                );
            }

            Instruction::Status { id, address, mut status } => {
                status.apply_index = state.get_applied_index();
                self.send(
                    address,
                    Event::ClientResponse { id, response: Ok(Response::Status(*status)) },
                )?;
            }

            Instruction::Vote { term, index, address } => {
                self.query_vote(term, index, address);
                self.query_execute(state)?;
            }
        }
        Ok(())
    }

    /// Aborts all pending notifications.
    fn notify_abort(&mut self) -> Result<()> {
        for (_, (address, id)) in std::mem::take(&mut self.notify) {
            self.send(address, Event::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    /// Notifies a client about an applied log entry, if any.
    fn notify_applied(&mut self, index: Index, result: Result<Vec<u8>>) -> Result<()> {
        if let Some((to, id)) = self.notify.remove(&index) {
            self.send(to, Event::ClientResponse { id, response: result.map(Response::Mutate) })?;
        }
        Ok(())
    }

    /// Aborts all pending queries.
    fn query_abort(&mut self) -> Result<()> {
        for (_, queries) in std::mem::take(&mut self.queries) {
            for (id, query) in queries {
                self.send(
                    query.address,
                    Event::ClientResponse { id, response: Err(Error::Abort) },
                )?;
            }
        }
        Ok(())
    }

    /// Executes any queries that are ready.
    fn query_execute(&mut self, state: &mut dyn State) -> Result<()> {
        for query in self.query_ready(state.get_applied_index()) {
            debug!("Executing query {:?}", query.command);
            let result = state.query(query.command);
            if let Err(error @ Error::Internal(_)) = result {
                return Err(error);
            }
            self.send(
                query.address,
                Event::ClientResponse { id: query.id, response: result.map(Response::Query) },
            )?
        }
        Ok(())
    }

    /// Fetches and removes any ready queries, where index <= applied_index.
    fn query_ready(&mut self, applied_index: Index) -> Vec<Query> {
        let mut ready = Vec::new();
        let mut empty = Vec::new();
        for (index, queries) in self.queries.range_mut(..=applied_index) {
            let mut ready_ids = Vec::new();
            for (id, query) in queries.iter_mut() {
                if query.votes.len() as u64 >= query.quorum {
                    ready_ids.push(id.clone());
                }
            }
            for id in ready_ids {
                if let Some(query) = queries.remove(&id) {
                    ready.push(query)
                }
            }
            if queries.is_empty() {
                empty.push(*index)
            }
        }
        for index in empty {
            self.queries.remove(&index);
        }
        ready
    }

    /// Votes for queries up to and including a given commit index for a term by an address.
    fn query_vote(&mut self, term: Term, commit_index: Index, address: Address) {
        for (_, queries) in self.queries.range_mut(..=commit_index) {
            for (_, query) in queries.iter_mut() {
                if term >= query.term {
                    query.votes.insert(address.clone());
                }
            }
        }
    }

    /// Sends a message.
    fn send(&self, to: Address, event: Event) -> Result<()> {
        // TODO: This needs to use the correct term.
        let msg = Message { from: Address::Node(self.node_id), to, term: 0, event };
        debug!("Sending {:?}", msg);
        Ok(self.node_tx.send(msg)?)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug)]
    pub struct TestState {
        commands: Arc<Mutex<Vec<Vec<u8>>>>,
        applied_index: Arc<Mutex<Index>>,
    }

    impl TestState {
        pub fn new(applied_index: Index) -> Self {
            Self {
                commands: Arc::new(Mutex::new(Vec::new())),
                applied_index: Arc::new(Mutex::new(applied_index)),
            }
        }

        pub fn list(&self) -> Vec<Vec<u8>> {
            self.commands.lock().unwrap().clone()
        }
    }

    impl State for TestState {
        fn get_applied_index(&self) -> Index {
            *self.applied_index.lock().unwrap()
        }

        // Appends the entry to the internal command list.
        fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
            if let Some(command) = &entry.command {
                self.commands.lock()?.push(command.clone());
            }
            *self.applied_index.lock()? = entry.index;
            Ok(entry.command.unwrap_or_default())
        }

        // Appends the command to the internal commands list.
        fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
            self.commands.lock()?.push(command.clone());
            Ok(command)
        }
    }

    async fn setup() -> Result<(
        Box<TestState>,
        mpsc::UnboundedSender<Instruction>,
        mpsc::UnboundedReceiver<Message>,
    )> {
        let state = Box::new(TestState::new(0));
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        tokio::spawn(Driver::new(1, state_rx, node_tx).drive(state.clone()));
        Ok((state, state_tx, node_rx))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_abort() -> Result<()> {
        let (state, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Notify {
            id: vec![0x01],
            index: 1,
            address: Address::Node(1),
        })?;
        state_tx.send(Instruction::Query {
            id: vec![0x02],
            address: Address::Client,
            command: vec![0xf0],
            term: 1,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Vote { term: 1, index: 1, address: Address::Node(1) })?;
        state_tx.send(Instruction::Abort)?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![
                Message {
                    from: Address::Node(1),
                    to: Address::Node(1),
                    term: 0,
                    event: Event::ClientResponse { id: vec![0x01], response: Err(Error::Abort) }
                },
                Message {
                    from: Address::Node(1),
                    to: Address::Client,
                    term: 0,
                    event: Event::ClientResponse { id: vec![0x02], response: Err(Error::Abort) }
                }
            ]
        );
        assert_eq!(state.list(), Vec::<Vec<u8>>::new());
        assert_eq!(state.get_applied_index(), 0);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_apply() -> Result<()> {
        let (state, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Notify {
            id: vec![0x01],
            index: 2,
            address: Address::Client,
        })?;
        state_tx.send(Instruction::Apply { entry: Entry { index: 1, term: 1, command: None } })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 2, term: 1, command: Some(vec![0xaf]) },
        })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![Message {
                from: Address::Node(1),
                to: Address::Client,
                term: 0,
                event: Event::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::Mutate(vec![0xaf]))
                }
            }]
        );
        assert_eq!(state.list(), vec![vec![0xaf]]);
        assert_eq!(state.get_applied_index(), 2);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_query() -> Result<()> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            term: 2,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 2, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { term: 2, index: 1, address: Address::Node(1) })?;
        state_tx.send(Instruction::Vote { term: 2, index: 1, address: Address::Node(2) })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![Message {
                from: Address::Node(1),
                to: Address::Client,
                term: 0,
                event: Event::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::Query(vec![0xf0]))
                }
            }]
        );

        Ok(())
    }

    // A query for an index submitted in a given term cannot be satisfied by votes below that term.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_query_noterm() -> Result<()> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            term: 2,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 1, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { term: 2, index: 1, address: Address::Node(1) })?;
        state_tx.send(Instruction::Vote { term: 1, index: 1, address: Address::Node(1) })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(node_rx.collect::<Vec<_>>().await, vec![]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_query_noquorum() -> Result<()> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            term: 1,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 1, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { term: 1, index: 1, address: Address::Node(1) })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(node_rx.collect::<Vec<_>>().await, vec![]);

        Ok(())
    }
}
