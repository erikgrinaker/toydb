#![warn(clippy::all)]
#![allow(clippy::new_without_default)]
#![allow(clippy::unneeded_field_pattern)]

pub mod client;
pub mod encoding;
pub mod error;
pub mod raft;
pub mod server;
pub mod sql;
pub mod storage;

pub use client::Client;
pub use server::Server;
pub use sql::execution::ResultSet;
