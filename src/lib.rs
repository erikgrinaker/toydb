#![warn(clippy::all)]

#[macro_use]
extern crate log;

extern crate httpbis;

mod error;
mod kvstore;
mod server;
mod service;

pub use error::Error;
pub use server::Server;
