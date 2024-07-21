//! toydump is a debug tool that prints a toyDB BitCask database in
//! human-readable form. It can print both the SQL database and the Raft log
//! (via --raft). It only outputs live BitCask data, not garbage entries.

#![warn(clippy::all)]

use toydb::encoding::format::{self, Formatter as _};
use toydb::error::Result;
use toydb::storage::{BitCask, Engine as _};

use clap::Parser as _;

fn main() {
    if let Err(error) = Command::parse().run() {
        eprintln!("Error: {error}")
    }
}

/// The toydump command.
#[derive(clap::Parser)]
#[command(about = "Prints toyDB file contents.", version, propagate_version = true)]
struct Command {
    /// The BitCask file to dump (SQL database unless --raft).
    file: String,
    /// The file is a Raft log, not SQL database.
    #[arg(long)]
    raft: bool,
    /// Also show raw key and value.
    #[arg(long)]
    raw: bool,
}

impl Command {
    /// Runs the command.
    fn run(self) -> Result<()> {
        let mut engine = BitCask::new(self.file.into())?;
        let mut scan = engine.scan(..);
        while let Some((key, value)) = scan.next().transpose()? {
            let mut string = match self.raft {
                true => format::Raft::<format::SQLCommand>::key_value(&key, &value),
                false => format::MVCC::<format::SQL>::key_value(&key, &value),
            };
            if self.raw {
                string = format!("{string} [{}]", format::Raw::key_value(&key, &value))
            }
            println!("{string}");
        }
        Ok(())
    }
}
