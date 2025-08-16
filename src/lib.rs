#![warn(clippy::all)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::module_inception)]
#![allow(clippy::type_complexity)]

pub mod client;
pub mod encoding;
pub mod error;
pub mod raft;
pub mod server;
pub mod sql;
pub mod storage;

pub use client::Client;
pub use server::Server;
pub use sql::execution::StatementResult;
