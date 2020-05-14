use super::{Address, Entry, Event, Message, Response, Scan, Status};
use crate::Error;

use log::{debug, error};
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::stream::StreamExt as _;
use tokio::sync::mpsc;

/// A Raft-managed state machine.
pub trait State {
    /// Returns the last applied index from the state machine, used when initializing the driver.
    fn applied_index(&self) -> u64;

    /// Mutates the state machine. If the state machine returns Error::Internal, the Raft node
    /// halts. For any other error, the state is applied and the error propagated to the caller.
    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>, Error>;

    /// Queries the state machine. All errors are propagated to the caller.
    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error>;
}

#[derive(Debug, PartialEq)]
/// A driver instruction.
pub enum Instruction {
    /// Abort all pending operations, e.g. due to leader change.
    Abort,
    /// Apply a log entry.
    Apply { entry: Entry },
    /// Notify the given address with the result of applying the entry at the given index.
    Notify { id: Vec<u8>, address: Address, index: u64 },
    /// Query the state machine when the given index has been confirmed by vote.
    Query { id: Vec<u8>, address: Address, command: Vec<u8>, index: u64, quorum: u64 },
    /// Extend the given server status and return it to the given address.
    Status { id: Vec<u8>, address: Address, status: Status },
    /// Votes for queries at the given commit index.
    Vote { index: u64, address: Address },
}

/// A driver query.
struct Query {
    id: Vec<u8>,
    address: Address,
    command: Vec<u8>,
    quorum: u64,
    votes: HashSet<Address>,
}

/// Drives a state machine, taking operations from state_rx and sending results via node_tx.
pub struct Driver {
    state_rx: mpsc::UnboundedReceiver<Instruction>,
    node_tx: mpsc::UnboundedSender<Message>,
    applied_index: u64,
    /// Notify clients when their mutation is applied. <index, (client, id)>
    notify: HashMap<u64, (Address, Vec<u8>)>,
    /// Execute client queries when they receive a quorum. <index, <id, query>>
    queries: BTreeMap<u64, BTreeMap<Vec<u8>, Query>>,
}

impl Driver {
    /// Creates a new state machine driver.
    pub fn new(
        state_rx: mpsc::UnboundedReceiver<Instruction>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Self {
        Self {
            state_rx,
            node_tx,
            applied_index: 0,
            notify: HashMap::new(),
            queries: BTreeMap::new(),
        }
    }

    /// Drives a state machine.
    pub async fn drive<S: State>(mut self, mut state: S) -> Result<(), Error> {
        debug!("Starting state machine driver");
        while let Some(instruction) = self.state_rx.next().await {
            if let Err(error) = self.execute(instruction, &mut state).await {
                error!("Halting state machine due to error: {}", error);
                return Err(error);
            }
        }
        debug!("Stopping state machine driver");
        Ok(())
    }

    /// Synchronously (re)plays a set of log entries, for initial sync.
    pub fn replay<'a, S: State>(&mut self, state: &mut S, mut scan: Scan<'a>) -> Result<(), Error> {
        while let Some(entry) = scan.next().transpose()? {
            debug!("Replaying {:?}", entry);
            if let Some(command) = entry.command {
                match state.mutate(entry.index, command) {
                    Err(error @ Error::Internal(_)) => return Err(error),
                    _ => self.applied_index = entry.index,
                }
            }
        }
        Ok(())
    }

    /// Executes a state machine instruction.
    pub async fn execute<S: State>(&mut self, i: Instruction, state: &mut S) -> Result<(), Error> {
        debug!("Executing {:?}", i);
        match i {
            Instruction::Abort => {
                self.notify_abort()?;
                self.query_abort()?;
            }

            Instruction::Apply { entry: Entry { index, command, .. } } => {
                if let Some(command) = command {
                    debug!("Applying state machine command {}: {:?}", index, command);
                    match tokio::task::block_in_place(|| state.mutate(index, command)) {
                        Err(error @ Error::Internal(_)) => return Err(error),
                        result => self.notify_applied(index, result)?,
                    };
                }
                // We have to track applied_index here, separately from the state machine, because
                // no-op log entries are significant for whether a query should be executed.
                self.applied_index = index;
                // Try to execute any pending queries, since they may have been submitted for a
                // commit_index which hadn't been applied yet.
                self.query_execute(state)?;
            }

            Instruction::Notify { id, address, index } => {
                if index > state.applied_index() {
                    self.notify.insert(index, (address, id));
                } else {
                    self.send(address, Event::ClientResponse { id, response: Err(Error::Abort) })?;
                }
            }

            Instruction::Query { id, address, command, index, quorum } => {
                self.queries.entry(index).or_default().insert(
                    id.clone(),
                    Query { id, address, command, quorum, votes: HashSet::new() },
                );
            }

            Instruction::Status { id, address, mut status } => {
                status.apply_index = state.applied_index();
                self.send(
                    address,
                    Event::ClientResponse { id, response: Ok(Response::Status(status)) },
                )?;
            }

            Instruction::Vote { index, address } => {
                self.query_vote(index, address);
                self.query_execute(state)?;
            }
        }
        Ok(())
    }

    /// Aborts all pending notifications.
    fn notify_abort(&mut self) -> Result<(), Error> {
        for (_, (address, id)) in std::mem::replace(&mut self.notify, HashMap::new()) {
            self.send(address, Event::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    /// Notifies a client about an applied log entry, if any.
    fn notify_applied(&mut self, index: u64, result: Result<Vec<u8>, Error>) -> Result<(), Error> {
        if let Some((to, id)) = self.notify.remove(&index) {
            self.send(to, Event::ClientResponse { id, response: result.map(Response::State) })?;
        }
        Ok(())
    }

    /// Aborts all pending queries.
    fn query_abort(&mut self) -> Result<(), Error> {
        for (_, queries) in std::mem::replace(&mut self.queries, BTreeMap::new()) {
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
    fn query_execute<S: State>(&mut self, state: &mut S) -> Result<(), Error> {
        for query in self.query_ready(self.applied_index) {
            debug!("Executing query {:?}", query.command);
            let result = state.query(query.command);
            if let Err(error @ Error::Internal(_)) = result {
                return Err(error);
            }
            self.send(
                query.address,
                Event::ClientResponse { id: query.id, response: result.map(Response::State) },
            )?
        }
        Ok(())
    }

    /// Fetches and removes any ready queries, where index <= applied_index.
    fn query_ready(&mut self, applied_index: u64) -> Vec<Query> {
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

    /// Votes for queries up to and including a given commit index by an address.
    fn query_vote(&mut self, commit_index: u64, address: Address) {
        for (_, queries) in self.queries.range_mut(..=commit_index) {
            for (_, query) in queries.iter_mut() {
                query.votes.insert(address.clone());
            }
        }
    }

    /// Sends a message.
    fn send(&self, to: Address, event: Event) -> Result<(), Error> {
        let msg = Message { from: Address::Local, to, term: 0, event };
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
        applied_index: Arc<Mutex<u64>>,
    }

    impl TestState {
        pub fn new(applied_index: u64) -> Self {
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
        fn applied_index(&self) -> u64 {
            *self.applied_index.lock().unwrap()
        }

        // Appends the command to the internal commands list.
        fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>, Error> {
            self.commands.lock()?.push(command.clone());
            *self.applied_index.lock()? = index;
            Ok(command)
        }

        // Appends the command to the internal commands list.
        fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
            self.commands.lock()?.push(command.clone());
            Ok(command)
        }
    }

    async fn setup() -> Result<
        (TestState, mpsc::UnboundedSender<Instruction>, mpsc::UnboundedReceiver<Message>),
        Error,
    > {
        let state = TestState::new(0);
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        tokio::spawn(Driver::new(state_rx, node_tx).drive(state.clone()));
        Ok((state, state_tx, node_rx))
    }

    #[tokio::test(core_threads = 2)]
    async fn driver_abort() -> Result<(), Error> {
        let (state, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Notify {
            id: vec![0x01],
            index: 1,
            address: Address::Peer("a".into()),
        })?;
        state_tx.send(Instruction::Query {
            id: vec![0x02],
            address: Address::Client,
            command: vec![0xf0],
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Vote { index: 1, address: Address::Local })?;
        state_tx.send(Instruction::Abort)?;
        std::mem::drop(state_tx);

        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![
                Message {
                    from: Address::Local,
                    to: Address::Peer("a".into()),
                    term: 0,
                    event: Event::ClientResponse { id: vec![0x01], response: Err(Error::Abort) }
                },
                Message {
                    from: Address::Local,
                    to: Address::Client,
                    term: 0,
                    event: Event::ClientResponse { id: vec![0x02], response: Err(Error::Abort) }
                }
            ]
        );
        assert_eq!(state.list(), Vec::<Vec<u8>>::new());
        assert_eq!(state.applied_index(), 0);

        Ok(())
    }

    #[tokio::test(core_threads = 2)]
    async fn driver_apply() -> Result<(), Error> {
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
        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![Message {
                from: Address::Local,
                to: Address::Client,
                term: 0,
                event: Event::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::State(vec![0xaf]))
                }
            }]
        );
        assert_eq!(state.list(), vec![vec![0xaf]]);
        assert_eq!(state.applied_index(), 2);

        Ok(())
    }

    #[tokio::test(core_threads = 2)]
    async fn driver_query() -> Result<(), Error> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 1, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { index: 1, address: Address::Local })?;
        state_tx.send(Instruction::Vote { index: 1, address: Address::Peer("a".into()) })?;
        std::mem::drop(state_tx);

        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![Message {
                from: Address::Local,
                to: Address::Client,
                term: 0,
                event: Event::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::State(vec![0xf0]))
                }
            }]
        );

        Ok(())
    }

    #[tokio::test(core_threads = 2)]
    async fn driver_query_noquorum() -> Result<(), Error> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 1, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { index: 1, address: Address::Local })?;
        std::mem::drop(state_tx);

        assert_eq!(node_rx.collect::<Vec<_>>().await, vec![]);

        Ok(())
    }
}
