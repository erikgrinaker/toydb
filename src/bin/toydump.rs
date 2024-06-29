//! toydump is a debug tool that prints a toyDB BitCask database in
//! human-readable form. It only prints live BitCask data, not garbage entries.
#![warn(clippy::all)]

use toydb::encoding::format::{self, Formatter as _};
use toydb::error::Result;
use toydb::storage::{BitCask, Engine};

fn main() -> Result<()> {
    let args = clap::command!()
        .about("Prints toyDB BitCask contents in human-readable form.")
        .args([clap::Arg::new("file").required(true)])
        .get_matches();
    let file: &String = args.get_one("file").unwrap();

    let mut engine = BitCask::new(file.into())?;
    let mut scan = engine.scan(..);
    while let Some((key, value)) = scan.next().transpose()? {
        // TODO: handle SQL and Raft data too.
        println!("{}", format::MVCC::<format::Raw>::key_value(&key, &value));
    }
    Ok(())
}
