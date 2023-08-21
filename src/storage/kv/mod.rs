mod bitcask;
pub mod encoding;
mod memory;
pub mod mvcc;
mod std_memory;
#[cfg(test)]
mod test;

pub use bitcask::BitCask;
pub use memory::Memory;
pub use mvcc::MVCC;
pub use std_memory::StdMemory;
#[cfg(test)]
pub use test::Test;

use crate::error::Result;
use std::fmt::Display;
use std::ops::{Bound, RangeBounds};

/// A key/value store.
pub trait Store: Display + Send + Sync {
    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Flushes any buffered data to the underlying storage medium.
    fn flush(&mut self) -> Result<()>;

    /// Gets a value for a key, if it exists.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan(&mut self, range: Range) -> Scan;

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
}

/// A scan range.
pub struct Range {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl Range {
    /// Creates a new range from the given Rust range. We can't use the RangeBounds directly in
    /// scan() since that prevents us from using Store as a trait object. Also, we can't take
    /// AsRef<[u8]> or other convenient types, since that won't work with e.g. .. ranges.
    pub fn from<R: RangeBounds<Vec<u8>>>(range: R) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }

    /// Checks if the given value is contained in the range.
    fn contains(&self, v: &[u8]) -> bool {
        (match &self.start {
            Bound::Included(start) => &**start <= v,
            Bound::Excluded(start) => &**start < v,
            Bound::Unbounded => true,
        }) && (match &self.end {
            Bound::Included(end) => v <= &**end,
            Bound::Excluded(end) => v < &**end,
            Bound::Unbounded => true,
        })
    }
}

impl RangeBounds<Vec<u8>> for Range {
    fn start_bound(&self) -> Bound<&Vec<u8>> {
        match &self.start {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&Vec<u8>> {
        match &self.end {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

/// Iterator over a key/value range.
pub type Scan<'a> = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send + 'a>;

#[cfg(test)]
mod tests {
    /// Generates common tests for any Store implementation.
    macro_rules! test_store {
        ($setup:expr) => {
            /// Tests Store point operations, i.e. set, get, and delete.
            #[test]
            fn point_ops() -> Result<()> {
                let mut s = $setup;

                // Getting a missing key should return None.
                assert_eq!(None, s.get(b"a")?);

                // Setting and getting a key should return its value.
                s.set(b"a", vec![0x01])?;
                assert_eq!(Some(vec![0x01]), s.get(b"a")?);

                // Setting a different key should not affect the first.
                s.set(b"b", vec![0x02])?;
                assert_eq!(Some(vec![0x02]), s.get(b"b")?);
                assert_eq!(Some(vec![0x01]), s.get(b"a")?);

                // Getting a different missing key should return None. The
                // comparison is case-insensitive for strings.
                assert_eq!(None, s.get(b"c")?);
                assert_eq!(None, s.get(b"A")?);

                // Setting an existing key should replace its value.
                s.set(b"a", vec![0x00])?;
                assert_eq!(Some(vec![0x00]), s.get(b"a")?);

                // Deleting a key should remove it, but not affect others.
                s.delete(b"a")?;
                assert_eq!(None, s.get(b"a")?);
                assert_eq!(Some(vec![0x02]), s.get(b"b")?);

                // Deletes are idempotent.
                s.delete(b"a")?;
                assert_eq!(None, s.get(b"a")?);

                Ok(())
            }

            #[test]
            /// Tests Store point operations on empty keys and values.
            /// These are as valid as any other key/value.
            fn point_ops_empty() -> Result<()> {
                let mut s = $setup;

                assert_eq!(None, s.get(b"")?);
                s.set(b"", vec![])?;
                assert_eq!(Some(vec![]), s.get(b"")?);
                s.delete(b"")?;
                assert_eq!(None, s.get(b"")?);

                Ok(())
            }

            #[test]
            /// Tests Store point operations on keys and values of
            /// increasing sizes, up to 16 MB.
            fn point_ops_sizes() -> Result<()> {
                let mut s = $setup;

                // Generate keys/values for increasing powers of two.
                for size in (1..=24).map(|i| 1 << i) {
                    let bytes = "x".repeat(size);
                    let key = bytes.as_bytes();
                    let value = bytes.clone().into_bytes();

                    assert_eq!(None, s.get(key)?);
                    s.set(key, value.clone())?;
                    assert_eq!(Some(value), s.get(key)?);
                    s.delete(key)?;
                    assert_eq!(None, s.get(key)?);
                }

                Ok(())
            }

            #[test]
            /// Tests various Store scans.
            fn scan() -> Result<()> {
                let mut s = $setup;
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
                    s.scan(Range::from(b"b".to_vec()..b"bz".to_vec()))
                        .collect::<Result<Vec<_>>>()?
                );
                assert_eq!(
                    vec![
                        (b"bb".to_vec(), vec![0x02, 0x02]),
                        (b"ba".to_vec(), vec![0x02, 0x01]),
                        (b"b".to_vec(), vec![0x02]),
                    ],
                    s.scan(Range::from(b"b".to_vec()..b"bz".to_vec()))
                        .rev()
                        .collect::<Result<Vec<_>>>()?
                );

                // Inclusive/exclusive ranges
                assert_eq!(
                    vec![(b"b".to_vec(), vec![0x02]), (b"ba".to_vec(), vec![0x02, 0x01]),],
                    s.scan(Range::from(b"b".to_vec()..b"bb".to_vec()))
                        .collect::<Result<Vec<_>>>()?
                );
                assert_eq!(
                    vec![
                        (b"b".to_vec(), vec![0x02]),
                        (b"ba".to_vec(), vec![0x02, 0x01]),
                        (b"bb".to_vec(), vec![0x02, 0x02]),
                    ],
                    s.scan(Range::from(b"b".to_vec()..=b"bb".to_vec()))
                        .collect::<Result<Vec<_>>>()?
                );

                // Open ranges
                assert_eq!(
                    vec![(b"bb".to_vec(), vec![0x02, 0x02]), (b"c".to_vec(), vec![0x03]),],
                    s.scan(Range::from(b"bb".to_vec()..)).collect::<Result<Vec<_>>>()?
                );
                assert_eq!(
                    vec![(b"a".to_vec(), vec![0x01]), (b"b".to_vec(), vec![0x02]),],
                    s.scan(Range::from(..=b"b".to_vec())).collect::<Result<Vec<_>>>()?
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
                    s.scan(Range::from(..)).collect::<Result<Vec<_>>>()?
                );
                Ok(())
            }

            #[test]
            fn random() -> Result<()> {
                use rand::Rng;
                let mut s = $setup;
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
                assert_eq!(expect, s.scan(Range::from(..)).collect::<Result<Vec<_>>>()?);
                expect.reverse();
                assert_eq!(expect, s.scan(Range::from(..)).rev().collect::<Result<Vec<_>>>()?);

                // Remove the items
                for (key, _) in items {
                    s.delete(&key)?;
                    assert_eq!(None, s.get(&key)?);
                }
                assert!(s.scan(Range::from(..)).collect::<Result<Vec<_>>>()?.is_empty());

                Ok(())
            }
        };
    }

    pub(super) use test_store; // Export for use in submodules.
}
