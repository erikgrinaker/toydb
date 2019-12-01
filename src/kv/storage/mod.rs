mod file;
#[cfg(test)]
mod memory;

pub use file::File;
#[cfg(test)]
pub use memory::Memory;

use crate::Error;
use std::ops::RangeBounds;

/// KV storage backend
pub trait Storage: 'static + Sync + Send {
    /// Reads a value
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;

    /// Removes a value
    fn remove(&mut self, key: &[u8]) -> Result<(), Error>;

    /// Returns an iterator over a range of pairs (inclusive)
    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Range;

    /// Writes a value
    fn write(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error>;
}

/// Iterator over a key range
pub type Range =
    Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>), Error>> + 'static + Sync + Send>;

#[cfg(test)]
trait TestSuite<S: Storage> {
    fn setup() -> Result<S, Error>;

    fn test() -> Result<(), Error> {
        Self::test_range()?;
        Self::test_read()?;
        Self::test_remove()?;
        Self::test_write()?;
        Ok(())
    }

    fn test_range() -> Result<(), Error> {
        let mut s = Self::setup()?;
        s.write(b"a", vec![0x01])?;
        s.write(b"b", vec![0x02])?;
        s.write(b"ba", vec![0x02, 0x01])?;
        s.write(b"bb", vec![0x02, 0x02])?;
        s.write(b"c", vec![0x03])?;

        assert_eq!(
            vec![
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
            ],
            s.scan(b"b".to_vec()..b"bz".to_vec()).collect::<Result<Vec<_>, _>>()?
        );
        Ok(())
    }

    fn test_read() -> Result<(), Error> {
        let mut b = Self::setup()?;
        b.write(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), b.read(b"a")?);
        assert_eq!(None, b.read(b"b")?);
        Ok(())
    }

    fn test_remove() -> Result<(), Error> {
        let mut b = Self::setup()?;
        b.write(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), b.read(b"a")?);
        b.remove(b"a")?;
        assert_eq!(None, b.read(b"a")?);
        b.remove(b"b")?;
        Ok(())
    }

    fn test_write() -> Result<(), Error> {
        let mut b = Self::setup()?;
        b.write(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), b.read(b"a")?);
        b.write(b"a", vec![0x02])?;
        assert_eq!(Some(vec![0x02]), b.read(b"a")?);
        Ok(())
    }
}
