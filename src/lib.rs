#![warn(clippy::all)]

#[cfg(test)]
#[macro_use]
extern crate assert_matches;
extern crate config;
#[macro_use(select)]
extern crate crossbeam_channel;
extern crate httpbis;
#[macro_use]
extern crate log;
extern crate rand;
extern crate rmp_serde as rmps;
extern crate rustyline;
extern crate serde;
extern crate uuid;

mod client;
mod error;
mod kv;
mod raft;
mod server;
mod service;
mod sql;
mod state;
mod utility;

pub use client::Client;
pub use error::Error;
pub use server::Server;
