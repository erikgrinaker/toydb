#![warn(clippy::all)]
#![allow(clippy::new_without_default)]

#[cfg(test)]
#[macro_use]
extern crate assert_matches;
extern crate config;
#[macro_use]
extern crate derivative;
#[cfg(test)]
extern crate goldenfile;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate rand;
extern crate regex;
extern crate rmp_serde as rmps;
extern crate rustyline;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate uuid;

pub mod client;
mod error;
pub mod kv;
pub mod raft;
pub mod server;
pub mod sql;
mod utility;

pub use client::Client;
pub use error::Error;
pub use server::Server;
