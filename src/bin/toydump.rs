//! toydump is a debug tool that prints a toyDB BitCask database in
//! human-readable form. It only prints live BitCask data, not garbage entries.
#![warn(clippy::all)]

use toydb::encoding::format::{self, Formatter as _};
use toydb::error::Result;
use toydb::storage::{BitCask, Engine as _};

fn main() -> Result<()> {
    let args = clap::command!()
        .about("Prints toyDB file contents in human-readable form.")
        .args([
            clap::Arg::new("raft")
                .long("raft")
                .num_args(0)
                .help("file is a Raft log, not SQL database"),
            clap::Arg::new("raw").long("raw").num_args(0).help("also show raw key/value"),
            clap::Arg::new("file").required(true),
        ])
        .get_matches();
    let raft: bool = *args.get_one("raft").unwrap();
    let raw: bool = *args.get_one("raw").unwrap();
    let file: &String = args.get_one("file").unwrap();

    let mut engine = BitCask::new(file.into())?;
    let mut scan = engine.scan(..);
    while let Some((key, value)) = scan.next().transpose()? {
        let mut string = match raft {
            true => format::Raft::<format::SQLCommand>::key_value(&key, &value),
            false => format::MVCC::<format::SQL>::key_value(&key, &value),
        };
        if raw {
            string = format!("{string} [{}]", format::Raw::key_value(&key, &value))
        }
        println!("{string}");
    }
    Ok(())
}
