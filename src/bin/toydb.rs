#![warn(clippy::all)]

extern crate nanoid;
extern crate simplelog;
extern crate toydb;

fn main() -> Result<(), toydb::Error> {
    simplelog::SimpleLogger::init(simplelog::LevelFilter::Debug, simplelog::Config::default())?;
    toydb::Server { id: nanoid::simple(), addr: "127.0.0.1:9605".into(), threads: 8 }.listen()
}
