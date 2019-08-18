use super::Store;
use crate::Error;
use std::collections::BTreeMap;
use std::io::Seek;

/// A prototype on-disk key-value store. The current version keeps all data in
/// memory and writes out the entire dataset to disk on every write. It is a
/// stop-gap solution until a proper store can be written.
#[derive(Debug)]
pub struct File {
    file: std::fs::File,
    data: BTreeMap<String, Vec<u8>>,
}

impl File {
    /// Creates a new file-backed key-value store.
    pub fn new(file: std::fs::File) -> Result<Self, Error> {
        let data = if file.metadata()?.len() > 0 {
            rmp_serde::decode::from_read(file.try_clone()?)?
        } else {
            BTreeMap::new()
        };
        Ok(Self { file, data })
    }

    /// Writes out the entire dataset to the file.
    fn flush(&mut self) -> Result<(), Error> {
        self.file.seek(std::io::SeekFrom::Start(0))?;
        rmp_serde::encode::write(&mut self.file, &self.data)?;
        Ok(())
    }
}

impl Store for File {
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.data.remove(key);
        self.flush()?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.data.get(key).cloned())
    }

    fn set(&mut self, key: &str, value: Vec<u8>) -> Result<(), Error> {
        self.data.insert(key.to_string(), value);
        self.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    extern crate tempfile;
    use super::super::tests::Suite;
    use super::*;
    use tempfile::tempfile;

    #[test]
    fn suite() {
        Suite::new(|| Box::new(File::new(tempfile().unwrap()).unwrap())).test()
    }
}
