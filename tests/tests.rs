#![warn(clippy::all)]

#[macro_use]
extern crate scopeguard;
extern crate serial_test;
extern crate tempdir;
extern crate toydb;

mod client;
mod cluster;
mod setup;
mod sql;
mod util;
