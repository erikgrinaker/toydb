use crate::Error;

/// A Raft-managed state machine.
pub trait State: 'static + Sync + Send + std::fmt::Debug {
    /// Reads from the state machine.
    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>, Error>;

    /// Mutates the state machine.
    fn mutate(&mut self, command: Vec<u8>) -> Result<Vec<u8>, Error>;
}
