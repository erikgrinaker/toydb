//! Executes a `Plan` against a `sql::engine::Engine`.

mod aggregate;
mod execute;
mod join;
mod source;
mod transform;
mod write;

pub use execute::{execute_plan, ExecutionResult};
