//! Key-value storage backends, with primitive IO operations.

mod blog;
mod file;
mod memory;
#[cfg(test)]
mod test;

pub use blog::BLog;
pub use file::File;
pub use memory::Memory;
#[cfg(test)]
pub use test::Test;

use crate::Error;
use std::ops::RangeBounds;

/// Key/value storage backend.
pub trait Storage {
    /// Flushes data to disk.
    fn flush(&mut self) -> Result<(), Error>;

    /// Reads a value for a key, or `None` if it does not exist.
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;

    /// Removes a key, if it exists.
    fn remove(&mut self, key: &[u8]) -> Result<(), Error>;

    /// Returns an iterator over a range of key/value pairs.
    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Range;

    /// Writes a value for a key, replacing the existing value if any.
    fn write(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error>;
}

/// Iterator over a key/value range.
pub type Range<'a> = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>), Error>> + 'a>;

#[cfg(test)]
trait TestSuite<S: Storage> {
    fn setup() -> Result<S, Error>;

    fn test() -> Result<(), Error> {
        Self::test_read()?;
        Self::test_remove()?;
        Self::test_scan()?;
        Self::test_write()?;
        Ok(())
    }

    fn test_read() -> Result<(), Error> {
        let mut s = Self::setup()?;
        s.write(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.read(b"a")?);
        assert_eq!(None, s.read(b"b")?);
        Ok(())
    }

    fn test_remove() -> Result<(), Error> {
        let mut s = Self::setup()?;
        s.write(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.read(b"a")?);
        s.remove(b"a")?;
        assert_eq!(None, s.read(b"a")?);
        s.remove(b"b")?;
        Ok(())
    }

    fn test_scan() -> Result<(), Error> {
        let mut s = Self::setup()?;
        s.write(b"a", vec![0x01])?;
        s.write(b"b", vec![0x02])?;
        s.write(b"ba", vec![0x02, 0x01])?;
        s.write(b"bb", vec![0x02, 0x02])?;
        s.write(b"c", vec![0x03])?;

        // Forward/backward ranges
        assert_eq!(
            vec![
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
            ],
            s.scan(b"b".to_vec()..b"bz".to_vec()).collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            vec![
                (b"bb".to_vec(), vec![0x02, 0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"b".to_vec(), vec![0x02]),
            ],
            s.scan(b"b".to_vec()..b"bz".to_vec()).rev().collect::<Result<Vec<_>, _>>()?
        );

        // Inclusive/exclusive ranges
        assert_eq!(
            vec![(b"b".to_vec(), vec![0x02]), (b"ba".to_vec(), vec![0x02, 0x01]),],
            s.scan(b"b".to_vec()..b"bb".to_vec()).collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            vec![
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
            ],
            s.scan(b"b".to_vec()..=b"bb".to_vec()).collect::<Result<Vec<_>, _>>()?
        );

        // Open ranges
        assert_eq!(
            vec![(b"bb".to_vec(), vec![0x02, 0x02]), (b"c".to_vec(), vec![0x03]),],
            s.scan(b"bb".to_vec()..).collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            vec![(b"a".to_vec(), vec![0x01]), (b"b".to_vec(), vec![0x02]),],
            s.scan(..=b"b".to_vec()).collect::<Result<Vec<_>, _>>()?
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
            s.scan(..).collect::<Result<Vec<_>, _>>()?
        );
        Ok(())
    }

    fn test_write() -> Result<(), Error> {
        let mut s = Self::setup()?;
        s.write(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.read(b"a")?);
        s.write(b"a", vec![0x02])?;
        assert_eq!(Some(vec![0x02]), s.read(b"a")?);
        Ok(())
    }
}
