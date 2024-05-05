mod log;
mod message;
mod node;
mod state;

pub use log::{Entry, Index, Log};
pub use message::{Envelope, Message, ReadSequence, Request, RequestID, Response, Status};
pub use node::{Node, NodeID, Term, Ticks};
pub use state::State;

/// The interval between Raft ticks. This is the unit of time for heartbeats and
/// elections.
pub const TICK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// The interval between leader heartbeats, in ticks.
pub const HEARTBEAT_INTERVAL: Ticks = 3;

/// The default election timeout range, in ticks. This is randomized in this
/// interval, to avoid election ties.
pub const ELECTION_TIMEOUT_RANGE: std::ops::Range<Ticks> = 10..20;
