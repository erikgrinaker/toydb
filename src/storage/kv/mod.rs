mod memory;
pub mod mvcc;
#[cfg(test)]
mod test;

pub use memory::Memory;
pub use mvcc::MVCC;
#[cfg(test)]
pub use test::Test;

use crate::error::Result;
use std::ops::RangeBounds;

/// Key/value store.
pub trait Store {
    /// Deletes a key, if it exists.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Flushes data to storage.
    fn flush(&mut self) -> Result<()>;

    /// Gets a value for a key, or `None` if it does not exist.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Returns an iterator over a range of key/value pairs.
    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Scan;

    /// Writes a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
}

/// Iterator over a key/value range.
pub type Scan<'a> = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>;

#[cfg(test)]
trait TestSuite<S: Store> {
    fn setup() -> Result<S>;

    fn test() -> Result<()> {
        Self::test_delete()?;
        Self::test_get()?;
        Self::test_scan()?;
        Self::test_set()?;
        Ok(())
    }

    fn test_get() -> Result<()> {
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        assert_eq!(None, s.get(b"b")?);
        Ok(())
    }

    fn test_delete() -> Result<()> {
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        s.delete(b"a")?;
        assert_eq!(None, s.get(b"a")?);
        s.delete(b"b")?;
        Ok(())
    }

    fn test_scan() -> Result<()> {
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        s.set(b"b", vec![0x02])?;
        s.set(b"ba", vec![0x02, 0x01])?;
        s.set(b"bb", vec![0x02, 0x02])?;
        s.set(b"c", vec![0x03])?;

        // Forward/backward ranges
        assert_eq!(
            vec![
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
            ],
            s.scan(b"b".to_vec()..b"bz".to_vec()).collect::<Result<Vec<_>>>()?
        );
        assert_eq!(
            vec![
                (b"bb".to_vec(), vec![0x02, 0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"b".to_vec(), vec![0x02]),
            ],
            s.scan(b"b".to_vec()..b"bz".to_vec()).rev().collect::<Result<Vec<_>>>()?
        );

        // Inclusive/exclusive ranges
        assert_eq!(
            vec![(b"b".to_vec(), vec![0x02]), (b"ba".to_vec(), vec![0x02, 0x01]),],
            s.scan(b"b".to_vec()..b"bb".to_vec()).collect::<Result<Vec<_>>>()?
        );
        assert_eq!(
            vec![
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
            ],
            s.scan(b"b".to_vec()..=b"bb".to_vec()).collect::<Result<Vec<_>>>()?
        );

        // Open ranges
        assert_eq!(
            vec![(b"bb".to_vec(), vec![0x02, 0x02]), (b"c".to_vec(), vec![0x03]),],
            s.scan(b"bb".to_vec()..).collect::<Result<Vec<_>>>()?
        );
        assert_eq!(
            vec![(b"a".to_vec(), vec![0x01]), (b"b".to_vec(), vec![0x02]),],
            s.scan(..=b"b".to_vec()).collect::<Result<Vec<_>>>()?
        );

        // Full range
        assert_eq!(
            vec![
                (b"a".to_vec(), vec![0x01]),
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
                (b"c".to_vec(), vec![0x03]),
            ],
            s.scan(..).collect::<Result<Vec<_>>>()?
        );
        Ok(())
    }

    fn test_set() -> Result<()> {
        let mut s = Self::setup()?;
        s.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get(b"a")?);
        s.set(b"a", vec![0x02])?;
        assert_eq!(Some(vec![0x02]), s.get(b"a")?);
        Ok(())
    }
}
