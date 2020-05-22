mod memory;
pub mod mvcc;
mod std_memory;
#[cfg(test)]
mod test;

pub use memory::Memory;
pub use mvcc::MVCC;
pub use std_memory::StdMemory;
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
        Self::test_random()?;
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

    fn test_random() -> Result<()> {
        use rand::Rng;
        let mut s = Self::setup()?;
        let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(397_427_893);

        // Create a bunch of random items and insert them
        let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for i in 0..1000_u64 {
            items.push((rng.gen::<[u8; 32]>().to_vec(), i.to_be_bytes().to_vec()))
        }
        for (key, value) in items.iter() {
            s.set(key, value.clone())?;
        }

        // Fetch the random items, both via get() and scan()
        for (key, value) in items.iter() {
            assert_eq!(s.get(key)?, Some(value.clone()))
        }
        let mut expect = items.clone();
        expect.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(expect, s.scan(..).collect::<Result<Vec<_>>>()?);
        expect.reverse();
        assert_eq!(expect, s.scan(..).rev().collect::<Result<Vec<_>>>()?);

        // Remove the items
        for (key, _) in items {
            s.delete(&key)?;
            assert_eq!(None, s.get(&key)?);
        }
        assert!(s.scan(..).collect::<Result<Vec<_>>>()?.is_empty());

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
