//! BLog is a B-tree indexed log. Since all historical data is retained, changed key/value pairs
//! are written sequentially (MessagePack-encoded) to a file, and a (currently in-memory) B-tree
//! is used to index keys to locations in the log. This also allows the log to act as a
//! write-ahead log, since the B-tree is reconstructed by replaying the log.

use super::{Range, Storage};
use crate::utility::{deserialize_read, serialize};
use crate::Error;

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Seek as _, SeekFrom, Write};
use std::sync::RwLock;

/// A B-tree indexed log
pub struct BLog {
    file: RwLock<File>,
    index: BTreeMap<Vec<u8>, u64>,
}

impl BLog {
    /// Creates a new B-log.
    pub fn new(mut file: File) -> Result<Self, Error> {
        let index = Self::build_index(&mut file)?;
        Ok(Self { file: RwLock::new(file), index })
    }

    /// Builds the index by scanning the file.
    fn build_index(mut file: &mut File) -> Result<BTreeMap<Vec<u8>, u64>, Error> {
        let size = file.metadata()?.len();
        let mut pos = file.seek(SeekFrom::Start(0))?;
        let mut index = BTreeMap::new();
        while pos < size {
            if let Some(Entry { key, .. }) = deserialize_read(&mut file)? {
                index.insert(key.to_vec(), pos);
            }
            pos = file.seek(SeekFrom::Current(0))?;
        }
        Ok(index)
    }

    /// Loads an entry from a position
    fn load(&self, pos: u64) -> Result<Entry, Error> {
        let mut file = self.file.write()?;
        file.seek(SeekFrom::Start(pos))?;
        Ok(deserialize_read(&*file)?)
    }

    /// Saves a value by appending to the log, returning its position
    fn save(&mut self, entry: &Entry) -> Result<u64, Error> {
        let mut cursor = self.file.write()?;
        let pos = cursor.seek(SeekFrom::End(0))?;
        cursor.write_all(&serialize(entry)?)?;
        Ok(pos)
    }
}

impl Storage for BLog {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(self.file.read()?.sync_all()?)
    }

    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if let Some(pos) = self.index.get(key) {
            return Ok(self.load(*pos)?.value);
        }
        Ok(None)
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), Error> {
        if self.index.contains_key(key) {
            self.save(&Entry { key: key.to_vec(), value: None })?;
            self.index.remove(key);
        }
        Ok(())
    }

    fn scan(&self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Range {
        // FIXME Needs to temporarily buffer results in a Vec to avoid dealing with
        // trait lifetimes right now.
        Box::new(
            self.index
                .range(range)
                .filter_map(|(k, p)| match self.load(*p) {
                    Ok(Entry { value: Some(v), .. }) => Some(Ok((k.clone(), v))),
                    Ok(Entry { value: None, .. }) => None,
                    Err(err) => Some(Err(err)),
                })
                .collect::<Vec<Result<_, Error>>>()
                .into_iter(),
        )
    }

    fn write(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        let pos = self.save(&Entry { key: key.to_vec(), value: Some(value) })?;
        self.index.insert(key.to_vec(), pos);
        Ok(())
    }
}

/// A log entry
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Entry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

#[cfg(test)]
impl super::TestSuite<BLog> for BLog {
    fn setup() -> Result<Self, Error> {
        extern crate tempfile;
        BLog::new(tempfile::tempfile()?)
    }
}

#[test]
fn tests() -> Result<(), Error> {
    use super::TestSuite;
    BLog::test()
}
