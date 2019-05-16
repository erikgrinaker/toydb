#![warn(clippy::all)]

mod error;
mod kvstore;
mod server;
mod service;

pub use error::Error;
pub use server::Server;
