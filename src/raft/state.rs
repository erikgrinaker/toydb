use super::{Entry, Index};
use crate::error::Result;

/// A Raft-managed state machine. Raft itself does not care what the state
/// machine is, nor what the commands and results do -- it will simply apply
/// arbitrary binary commands sequentially from the Raft log, returning an
/// arbitrary binary result to the client.
///
/// Since commands are applied identically across all replicas, they must be
/// deterministic and yield the same state and result across all replicas too.
/// Otherwise, the replicas will diverge, and different replicas will produce
/// different results.
///
/// Write commands ([`Request::Write`]) are replicated and applied on all
/// replicas via [`State::apply`]. The state machine must keep track of the last
/// applied index and return it via [`State::get_applied_index`]. Read commands
/// ([`Request::Read`]) are only executed on a single replica via
/// [`State::read`] and must not make any state changes.
pub trait State: Send {
    /// Returns the last applied index from the state machine.
    ///
    /// This must correspond to the current state of the state machine, since it
    /// determines which command to apply next. In particular, a node crash may
    /// result in partial command application or data loss, which must be
    /// handled appropriately.
    fn get_applied_index(&self) -> Index;

    /// Applies a log entry to the state machine, returning a client result.
    /// Errors are considered applied and propagated back to the client.
    ///
    /// This is executed on all replicas, so the result must be deterministic:
    /// it must yield the same state and result on all replicas, even if the
    /// command is reapplied following a node crash.
    ///
    /// Any non-deterministic apply error (e.g. an IO error) must panic and
    /// crash the node -- if it instead returns an error to the client, the
    /// command is considered applied and replica states will diverge. The state
    /// machine is responsible for panicing when appropriate.
    ///
    /// The entry may contain a noop command, which is committed by Raft during
    /// leader changes. This still needs to be applied to the state machine to
    /// properly update the applied index, and should return an empty result.
    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>>;

    /// Executes a read command in the state machine, returning a client result.
    /// Errors are also propagated back to the client.
    ///
    /// This is only executed on a single replica/node, so it must not result in
    /// any state changes (i.e. it must not write).
    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}
