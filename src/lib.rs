#![warn(clippy::all)]

extern crate config;
extern crate httpbis;
#[macro_use]
extern crate log;
extern crate rmp_serde as rmps;
extern crate serde;

mod error;
mod kv;
mod server;
mod service;

pub use error::Error;
pub use server::Server;
