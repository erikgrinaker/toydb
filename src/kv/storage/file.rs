use super::{Memory, Range, Storage};
use crate::Error;

use std::io::Seek;
use std::ops::RangeBounds;

/// A prototype file-based key-value backend. Uses a Memory store internally,
/// and writes it out to disk on every write, as a stop-gap solution until
/// a proper store is written.
pub struct File {
    /// The in-memory key-value store that is flushed to disk.
    data: Memory,
    /// Set to true if there is any changes not yet flushed to disk.
    dirty: bool,
    /// The file handle of the backing file.
    file: std::fs::File,
}

impl File {
    /// Creates a new file-based key-value storage backend.
    pub fn new(file: std::fs::File) -> Result<Self, Error> {
        let data = if file.metadata()?.len() > 0 {
            rmp_serde::decode::from_read(file.try_clone()?)?
        } else {
            Memory::new()
        };
        Ok(Self { file, data, dirty: false })
    }
}

impl Storage for File {
    fn flush(&mut self) -> Result<(), Error> {
        if self.dirty {
            self.file.seek(std::io::SeekFrom::Start(0))?;
            rmp_serde::encode::write(&mut self.file, &self.data)?;
            self.dirty = false;
        }
        Ok(())
    }

    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.data.read(key)
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), Error> {
        self.dirty = true;
        self.data.remove(key)
    }

    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Range {
        self.data.scan(range)
    }

    fn write(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        self.dirty = true;
        self.data.write(key, value)
    }
}

#[cfg(test)]
impl super::TestSuite<File> for File {
    fn setup() -> Result<Self, Error> {
        extern crate tempfile;
        File::new(tempfile::tempfile()?)
    }
}

#[test]
fn tests() -> Result<(), Error> {
    use super::TestSuite;
    File::test()
}
