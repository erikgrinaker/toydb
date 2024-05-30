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
    /// TODO: add an IO error kind instead to signify non-deterministic
    /// application failure which should not consider the command applied.
    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>>;

    /// Reads from the state machine. All errors are propagated to the caller.
    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}
