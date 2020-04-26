use crate::Error;

/// A Raft-managed state machine.
pub trait State {
    /// Mutates the state machine. If the state machine returns Error::Internal, the Raft node
    /// halts. For any other error, the state is applied and the error propagated to the caller.
    fn mutate(&mut self, command: Vec<u8>) -> Result<Vec<u8>, Error>;

    /// Queries the state machine. All errors are propagated to the caller.
    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error>;
}

#[cfg(test)]
pub mod tests {
    use super::*;
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
        // Appends the command to the internal commands list, and returns the command prefixed with
        // a 0xff byte. Returns Error::Internal if the payload is != 1 byte, and Error::Value if
        // the value is 0xff.
        fn mutate(&mut self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
            if command.len() != 1 {
                return Err(Error::Internal("Command must be 1 byte".into()));
            }
            if command[0] == 0xff {
                return Err(Error::Value("Command cannot be 0xff".into()));
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
}
