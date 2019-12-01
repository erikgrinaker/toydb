use super::{Range, Storage};
use crate::Error;

use std::collections::BTreeMap;
use std::io::Seek;
use std::ops::RangeBounds;

/// A prototype file-based key-value backend. Keeps the entire dataset in
/// memory and writes it out to disk on every write, as a stop-gap solution
/// until a proper store is written.
#[derive(Debug)]
pub struct File {
    file: std::fs::File,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl File {
    /// Creates a new file-based key-value storage backend
    pub fn new(file: std::fs::File) -> Result<Self, Error> {
        let data = if file.metadata()?.len() > 0 {
            rmp_serde::decode::from_read(file.try_clone()?)?
        } else {
            BTreeMap::new()
        };
        Ok(Self { file, data })
    }

    /// Writes out the entire dataset to the file
    fn flush(&mut self) -> Result<(), Error> {
        self.file.seek(std::io::SeekFrom::Start(0))?;
        rmp_serde::encode::write(&mut self.file, &self.data)?;
        Ok(())
    }
}

impl Storage for File {
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.data.get(key).cloned())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), Error> {
        self.data.remove(key);
        self.flush()?;
        Ok(())
    }

    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Range {
        // FIXME This copies everything into a separate vec to not have to deal with
        // lifetimes, which is pretty terrible.
        Box::new(
            self.data
                .range(range)
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect::<Vec<Result<_, Error>>>()
                .into_iter(),
        )
    }

    fn write(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        self.data.insert(key.to_vec(), value);
        self.flush()?;
        Ok(())
    }
}

#[cfg(test)]
impl super::TestSuite<File> for File {
    fn setup() -> Result<Self, Error> {
        extern crate tempfile;
        use tempfile::tempfile;
        File::new(tempfile()?)
    }
}

#[test]
fn tests() -> Result<(), Error> {
    use super::TestSuite;
    File::test()
}
