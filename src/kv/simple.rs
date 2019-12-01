use super::storage::{Range, Storage};
use crate::Error;

use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

/// A simple key-value store
#[derive(Clone, Debug)]
pub struct Simple<S: Storage> {
    storage: Arc<RwLock<S>>,
}

impl<S: Storage> Simple<S> {
    /// Creates a new Simple store
    pub fn new(storage: S) -> Self {
        Self { storage: Arc::new(RwLock::new(storage)) }
    }

    /// Deletes a key
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.storage.write()?.remove(key)
    }

    /// Fetches a key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.storage.read()?.read(key)
    }

    /// Scans a key range
    #[allow(dead_code)]
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<Range, Error> {
        Ok(self.storage.read()?.scan(range))
    }

    /// Sets a key
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        self.storage.write()?.write(key, value)
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
