use super::{Entry, Index};
use crate::error::Result;

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

    /// Reads from the state machine. All errors are propagated to the caller.
    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}

#[cfg(test)]
pub mod tests {
    use super::*;
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
        fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
            self.commands.lock()?.push(command.clone());
            Ok(command)
        }
    }
}
