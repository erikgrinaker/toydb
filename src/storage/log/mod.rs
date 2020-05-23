mod hybrid;
mod memory;
#[cfg(test)]
mod test;

pub use hybrid::Hybrid;
pub use memory::Memory;
#[cfg(test)]
pub use test::Test;

use crate::error::Result;

use std::fmt::Display;
use std::ops::{Bound, RangeBounds};

/// A log store. Entry indexes are 1-based, to match Raft semantics.
pub trait Store: Display + Sync + Send {
    /// Appends a log entry, returning its index.
    fn append(&mut self, entry: Vec<u8>) -> Result<u64>;

    /// Commits log entries up to and including the given index, making them immutable.
    fn commit(&mut self, index: u64) -> Result<()>;

    /// Returns the committed index, if any.
    fn committed(&self) -> u64;

    /// Fetches a log entry, if it exists.
    fn get(&self, index: u64) -> Result<Option<Vec<u8>>>;

    /// Returns the number of entries in the log.
    fn len(&self) -> u64;

    /// Scans the log between the given indexes.
    fn scan(&self, range: Range) -> Scan;

    /// Returns the size of the log, in bytes.
    fn size(&self) -> u64;

    /// Truncates the log be removing any entries above the given index, and returns the
    /// highest index. Errors if asked to truncate any committed entries.
    fn truncate(&mut self, index: u64) -> Result<u64>;

    /// Gets a metadata value.
    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Sets a metadata value.
    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Returns true if the log has no entries.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A scan range.
pub struct Range {
    start: Bound<u64>,
    end: Bound<u64>,
}

impl Range {
    /// Creates a new range from the given Rust range. We can't use the RangeBounds directly in
    /// scan() since that prevents us from Store into a trait object.
    pub fn from(range: impl RangeBounds<u64>) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(*v),
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(*v),
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }
}

/// Iterator over a log range.
pub type Scan<'a> = Box<dyn Iterator<Item = Result<Vec<u8>>> + 'a>;

#[cfg(test)]
use crate::error::Error;

#[cfg(test)]
trait TestSuite<S: Store> {
    fn setup() -> Result<S>;

    fn test() -> Result<()> {
        Self::test_append()?;
        Self::test_commit_truncate()?;
        Self::test_get()?;
        Self::test_metadata()?;
        Self::test_scan()?;
        Ok(())
    }

    fn test_append() -> Result<()> {
        let mut s = Self::setup()?;
        assert_eq!(0, s.len());
        assert_eq!(1, s.append(vec![0x01])?);
        assert_eq!(2, s.append(vec![0x02])?);
        assert_eq!(3, s.append(vec![0x03])?);
        assert_eq!(3, s.len());
        assert_eq!(
            vec![vec![1], vec![2], vec![3]],
            s.scan(Range::from(..)).collect::<Result<Vec<_>>>()?
        );
        Ok(())
    }

    fn test_commit_truncate() -> Result<()> {
        let mut s = Self::setup()?;

        assert_eq!(0, s.committed());

        // Truncating an empty store should be fine.
        assert_eq!(0, s.truncate(0)?);

        s.append(vec![0x01])?;
        s.append(vec![0x02])?;
        s.append(vec![0x03])?;
        s.commit(1)?;
        assert_eq!(1, s.committed());

        // Truncating beyond the end should be fine.
        assert_eq!(3, s.truncate(4)?);
        assert_eq!(
            vec![vec![1], vec![2], vec![3]],
            s.scan(Range::from(..)).collect::<Result<Vec<_>>>()?
        );

        // Truncating a committed entry should error.
        assert_eq!(
            Err(Error::Internal("Cannot truncate below committed index 1".into())),
            s.truncate(0)
        );

        // Truncating above should work.
        assert_eq!(1, s.truncate(1)?);
        assert_eq!(vec![vec![1]], s.scan(Range::from(..)).collect::<Result<Vec<_>>>()?);

        Ok(())
    }

    fn test_get() -> Result<()> {
        let mut s = Self::setup()?;
        s.append(vec![0x01])?;
        s.append(vec![0x02])?;
        s.append(vec![0x03])?;
        assert_eq!(None, s.get(0)?);
        assert_eq!(Some(vec![0x01]), s.get(1)?);
        assert_eq!(None, s.get(4)?);
        Ok(())
    }

    fn test_metadata() -> Result<()> {
        let mut s = Self::setup()?;
        s.set_metadata(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), s.get_metadata(b"a")?);
        assert_eq!(None, s.get_metadata(b"b")?);
        Ok(())
    }

    fn test_scan() -> Result<()> {
        let mut s = Self::setup()?;
        s.append(vec![0x01])?;
        s.append(vec![0x02])?;
        s.append(vec![0x03])?;
        s.commit(2)?;

        assert_eq!(
            vec![vec![1], vec![2], vec![3]],
            s.scan(Range::from(..)).collect::<Result<Vec<_>>>()?
        );

        assert_eq!(vec![vec![1]], s.scan(Range::from(0..2)).collect::<Result<Vec<_>>>()?);
        assert_eq!(vec![vec![1], vec![2]], s.scan(Range::from(1..3)).collect::<Result<Vec<_>>>()?);
        assert_eq!(
            vec![vec![1], vec![2], vec![3]],
            s.scan(Range::from(1..=3)).collect::<Result<Vec<_>>>()?
        );
        assert!(s.scan(Range::from(3..1)).collect::<Result<Vec<_>>>()?.is_empty());
        assert!(s.scan(Range::from(1..1)).collect::<Result<Vec<_>>>()?.is_empty());
        assert_eq!(vec![vec![2]], s.scan(Range::from(2..=2)).collect::<Result<Vec<_>>>()?);
        assert_eq!(vec![vec![2], vec![3]], s.scan(Range::from(2..5)).collect::<Result<Vec<_>>>()?);

        assert!(s.scan(Range::from(..0)).collect::<Result<Vec<_>>>()?.is_empty());
        assert_eq!(vec![vec![1]], s.scan(Range::from(..=1)).collect::<Result<Vec<_>>>()?);
        assert_eq!(vec![vec![1], vec![2]], s.scan(Range::from(..3)).collect::<Result<Vec<_>>>()?);

        assert!(s.scan(Range::from(4..)).collect::<Result<Vec<_>>>()?.is_empty());
        assert_eq!(vec![vec![3]], s.scan(Range::from(3..)).collect::<Result<Vec<_>>>()?);
        assert_eq!(vec![vec![2], vec![3]], s.scan(Range::from(2..)).collect::<Result<Vec<_>>>()?);

        Ok(())
    }
}
