//! iLog is an indexed log. Changed key/value pairs are written sequentially to a file
//! (MessagePack-encoded), and an in-memory B-tree is used to index keys to locations in the
//! log. It is an initial prototype until a better storage backend is written.

use super::{Scan, Store};
use crate::utility::{deserialize_read, serialize};
use crate::Error;

use serde_derive::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Seek as _, SeekFrom, Write as _};
use std::sync::RwLock;

/// A log entry
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Entry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

/// A B-tree indexed log
pub struct ILog {
    file: RwLock<File>,
    index: BTreeMap<Vec<u8>, u64>,
}

impl ILog {
    /// Creates a new ILog.
    pub fn new(mut file: File) -> Result<Self, Error> {
        let index = Self::build_index(&mut file)?;
        Ok(Self { file: RwLock::new(file), index })
    }

    /// Builds the index by scanning the file.
    fn build_index(mut file: &mut File) -> Result<BTreeMap<Vec<u8>, u64>, Error> {
        let mut index = BTreeMap::new();
        let mut pos = file.seek(SeekFrom::Start(0))?;
        while let Some(entry) = deserialize_read::<_, Entry>(&mut file)? {
            index.insert(entry.key, pos);
            pos = file.seek(SeekFrom::Current(0))?;
        }
        Ok(index)
    }

    /// Loads an entry from a position.
    fn load(&self, pos: u64) -> Result<Entry, Error> {
        let mut cursor = self.file.write()?;
        cursor.seek(SeekFrom::Start(pos))?;
        deserialize_read(&*cursor)?.ok_or_else(|| Error::Value("No log entry found".into()))
    }

    /// Saves an entry by appending it to the log, returning its position.
    fn save(&mut self, entry: &Entry) -> Result<u64, Error> {
        let mut cursor = self.file.write()?;
        let pos = cursor.seek(SeekFrom::End(0))?;
        cursor.write_all(&serialize(entry)?)?;
        Ok(pos)
    }
}

impl Store for ILog {
    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        if self.index.contains_key(key) {
            self.save(&Entry { key: key.to_vec(), value: None })?;
            self.index.remove(key);
        }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if let Some(pos) = self.index.get(key) {
            return Ok(self.load(*pos)?.value);
        }
        Ok(None)
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(self.file.read()?.sync_all()?)
    }

    fn scan(&self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Scan {
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

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        let pos = self.save(&Entry { key: key.to_vec(), value: Some(value) })?;
        self.index.insert(key.to_vec(), pos);
        Ok(())
    }
}

#[cfg(test)]
impl super::TestSuite<ILog> for ILog {
    fn setup() -> Result<Self, Error> {
        ILog::new(tempfile::tempfile()?)
    }
}

#[test]
fn tests() -> Result<(), Error> {
    use super::TestSuite;
    ILog::test()
}
