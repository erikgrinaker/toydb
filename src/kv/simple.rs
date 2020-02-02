use super::storage::{Range, Storage};
use crate::Error;

use std::ops::RangeBounds;

/// A simple key-value store, which stores the key-value pairs directly in the storage backend.
pub struct Simple<S: Storage> {
    /// The underlying storage backend.
    storage: S,
}

impl<S: Storage> Simple<S> {
    /// Creates a new Simple store.
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    /// Deletes a key, if it exists.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.storage.remove(key)?;
        self.storage.flush()
    }

    /// Gets a value for a key, or `None` if it does not exist.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.storage.read(key)
    }

    /// Returns an iterator over a range of key/value pairs.
    #[allow(dead_code)]
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<Range, Error> {
        Ok(self.storage.scan(range))
    }

    /// Sets a value for a key, replacing the existing value if any.
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        self.storage.write(key, value)?;
        self.storage.flush()
    }
}

#[cfg(test)]
pub mod tests {
    use super::super::storage::Memory;
    use super::*;

    fn setup() -> Simple<Memory> {
        Simple::new(Memory::new())
    }

    #[test]
    fn test_delete() -> Result<(), Error> {
        let mut s = setup();
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        s.delete(b"a")?;
        assert_eq!(None, s.get(b"a")?);
        s.delete(b"b")?;
        Ok(())
    }

    #[test]
    fn test_get() -> Result<(), Error> {
        let mut s = setup();
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        assert_eq!(None, s.get(b"b")?);
        Ok(())
    }

    #[test]
    fn test_scan() -> Result<(), Error> {
        let mut s = setup();
        s.set(b"a", vec![0x01])?;
        s.set(b"b", vec![0x02])?;
        s.set(b"ba", vec![0x02, 0x01])?;
        s.set(b"bb", vec![0x02, 0x02])?;
        s.set(b"c", vec![0x03])?;

        assert_eq!(
            vec![
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
            ],
            s.scan(b"b".to_vec()..b"c".to_vec())?
                .collect::<Result<Vec<(Vec<u8>, Vec<u8>)>, Error>>()?,
        );
        assert_eq!(
            vec![
                (b"bb".to_vec(), vec![0x02, 0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"b".to_vec(), vec![0x02]),
            ],
            s.scan(b"b".to_vec()..b"c".to_vec())?
                .rev()
                .collect::<Result<Vec<(Vec<u8>, Vec<u8>)>, Error>>()?,
        );
        Ok(())
    }

    #[test]
    fn test_set() -> Result<(), Error> {
        let mut s = setup();
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        s.set(b"a", vec![0x02])?;
        assert_eq!(Some(vec![0x02]), s.get(b"a")?);
        Ok(())
    }
}
