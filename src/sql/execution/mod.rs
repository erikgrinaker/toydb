//! Executes statements and plans.

mod aggregator;
mod executor;
mod join;
mod session;

pub use executor::{ExecutionResult, Executor};
pub use session::{Session, StatementResult};
