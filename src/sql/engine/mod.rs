//! The SQL engine provides SQL data storage and access, as well as session and
//! transaction management. The `Local` engine provides node-local on-disk
//! storage, while the `Raft` engine submits commands through Raft consensus
//! before dispatching to the `Local` engine on each node.

mod engine;
mod local;
mod raft;
mod session;

pub use engine::{Catalog, Engine, Transaction};
pub use local::{Key, Local};
pub use raft::{Raft, Status, Write};
pub use session::{Session, StatementResult};
