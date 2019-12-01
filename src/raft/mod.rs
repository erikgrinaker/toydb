mod log;
mod node;
mod transport;

pub use self::log::Entry;
pub use self::transport::{Event, Message, Transport};

use crate::kv;
use crate::Error;
use crossbeam_channel::{Receiver, Sender};
use node::Node;
use std::collections::HashMap;
use uuid::Uuid;

/// The duration of a Raft tick, which is the unit of time for e.g.
/// heartbeat intervals and election timeouts.
const TICK: std::time::Duration = std::time::Duration::from_millis(100);

/// A Raft-managed state machine.
pub trait State: 'static + Sync + Send + std::fmt::Debug {
    /// Mutates the state machine.
    fn mutate(&mut self, command: Vec<u8>) -> Result<Vec<u8>, Error>;

    /// Queries the state machine.
    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error>;
}

/// A Raft state machine representing an entire Raft cluster.
#[derive(Clone)]
pub struct Raft {
    call_tx: Sender<(Event, Sender<Event>)>,
    join_rx: Receiver<Result<(), Error>>,
}

impl Raft {
    /// Starts a new Raft state machine in a separate thread.
    pub fn start<S, L, T>(
        id: &str,
        peers: Vec<String>,
        state: S,
        store: kv::Simple<L>,
        transport: T,
    ) -> Result<Raft, Error>
    where
        S: State,
        L: kv::storage::Storage,
        T: Transport,
    {
        let ticker = crossbeam_channel::tick(TICK);
        let inbound_rx = transport.receiver();
        let (outbound_tx, outbound_rx) = crossbeam_channel::unbounded();
        let (call_tx, call_rx) = crossbeam_channel::unbounded::<(Event, Sender<Event>)>();
        let (join_tx, join_rx) = crossbeam_channel::unbounded();
        let mut response_txs: HashMap<Vec<u8>, Sender<Event>> = HashMap::new();
        let mut node = Node::new(id, peers, store, state, outbound_tx)?;

        std::thread::spawn(move || {
            // Ugly workaround to use ?, while waiting for try_blocks:
            // https://doc.rust-lang.org/unstable-book/language-features/try-blocks.html
            let result = (move || loop {
                select! {
                    // Handle ticks
                    recv(ticker) -> _ => node = node.tick()?,

                    // Handle local method calls
                    recv(call_rx) -> recv => {
                        let (event, response_tx) = recv?;
                        if let Some(call_id) = event.call_id() {
                            response_txs.insert(call_id, response_tx);
                            node = node.step(Message{from: None, to: None, term: 0, event})?;
                        } else {
                            response_tx.send(Event::RespondError{
                                call_id: vec![],
                                error: format!("Call ID not found for event {:?}", event),
                            })?;
                        }
                    },

                    // Handle inbound messages from peers
                    recv(inbound_rx) -> recv => node = node.step(recv?)?,

                    // Handle outbound messages from node, either to peers or a local method caller
                    recv(outbound_rx) -> recv => {
                        let msg = recv?;
                        if msg.to.is_some() {
                            transport.send(msg)?
                        } else if let Some(call_id) = msg.event.call_id() {
                            if let Some(response_tx) = response_txs.get(&call_id) {
                                response_tx.send(msg.event)?;
                            }
                        }
                    },
                }
            })();
            join_tx.send(result).unwrap()
        });

        Ok(Raft { call_tx, join_rx })
    }

    /// Waits for the Raft node to complete
    pub fn join(&self) -> Result<(), Error> {
        self.join_rx.recv()?
    }

    /// Runs a synchronous client call on the Raft cluster
    fn call(&self, event: Event) -> Result<Event, Error> {
        let (response_tx, response_rx) = crossbeam_channel::unbounded();
        self.call_tx.send((event, response_tx))?;
        match response_rx.recv()? {
            Event::RespondError { error, .. } => Err(Error::Network(error)),
            e => Ok(e),
        }
    }

    /// Generates a call ID
    fn call_id() -> Vec<u8> {
        Uuid::new_v4().as_bytes().to_vec()
    }

    /// Mutates the Raft state machine.
    pub fn mutate(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match self.call(Event::MutateState { call_id: Self::call_id(), command })? {
            Event::RespondState { response, .. } => Ok(response),
            event => Err(Error::Internal(format!("Unexpected Raft mutate response {:?}", event))),
        }
    }

    /// Queries the Raft state machine.
    pub fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match self.call(Event::QueryState { call_id: Self::call_id(), command })? {
            Event::RespondState { response, .. } => Ok(response),
            event => Err(Error::Internal(format!("Unexpected Raft read response {:?}", event))),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crossbeam_channel::Receiver;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug)]
    pub struct TestState {
        commands: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl TestState {
        pub fn new() -> Self {
            Self { commands: Arc::new(Mutex::new(Vec::new())) }
        }

        pub fn list(&self) -> Vec<Vec<u8>> {
            self.commands.lock().unwrap().clone()
        }
    }

    impl State for TestState {
        // Appends the command to the internal commands list, and
        // returns the command prefixed with a 0xff byte.
        fn mutate(&mut self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
            if command.len() != 1 {
                return Err(Error::Value("Mutation payload must be 1 byte".into()));
            }
            self.commands.lock()?.push(command.clone());
            Ok(vec![0xff, command[0]])
        }

        // Reads the command in the internal commands list at the index
        // given by the query command (1-based). Returns the stored command prefixed by
        // 0xbb, or 0xbb 0x00 if not found.
        fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
            if command.len() != 1 {
                return Err(Error::Value("Query payload must be 1 byte".into()));
            }
            let index = command[0] as usize;
            Ok(vec![0xbb, self.commands.lock()?.get(index - 1).map(|c| c[0]).unwrap_or(0x00)])
        }
    }

    pub fn assert_messages(rx: &Receiver<Message>, msgs: Vec<Message>) {
        let mut actual = Vec::new();
        while !rx.is_empty() {
            actual.push(rx.recv().unwrap());
        }
        assert_eq!(msgs, actual);
    }
}
