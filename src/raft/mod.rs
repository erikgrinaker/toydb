mod log;
mod message;
mod node;
mod state;

pub use log::{Entry, Index, Log};
pub use message::{Envelope, Message, ReadSequence, Request, RequestID, Response, Status};
pub use node::{Node, NodeID, Options, Term, Ticks};
pub use state::State;

/// The interval between Raft ticks, the unit of time.
pub const TICK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// The interval between leader heartbeats in ticks.
const HEARTBEAT_INTERVAL: Ticks = 4;

/// The default election timeout range in ticks. This is randomized in this
/// interval, to avoid election ties.
const ELECTION_TIMEOUT_RANGE: std::ops::Range<Ticks> = 10..20;

/// The maximum number of entries to send in a single append message.
const MAX_APPEND_ENTRIES: usize = 100;
