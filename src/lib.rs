#![warn(clippy::all)]

#[cfg(test)]
#[macro_use]
extern crate assert_matches;
extern crate config;
#[macro_use(select)]
extern crate crossbeam;
#[macro_use]
extern crate derivative;
#[cfg(test)]
extern crate goldenfile;
extern crate httpbis;
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
mod kv;
mod raft;
pub mod server;
mod service;
pub mod sql;
mod utility;

pub use client::Client;
pub use error::Error;
pub use server::Server;
