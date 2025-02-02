use super::{Engine, Status};
use crate::error::Result;

/// An in-memory key/value storage engine using the Rust standard library B-tree
/// implementation. Data is not persisted.
pub struct Memory {
    data: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Memory {
    /// Creates a new Memory key-value storage engine.
    pub fn new() -> Self {
        Self { data: std::collections::BTreeMap::new() }
    }
}

impl Engine for Memory {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_> {
        ScanIterator { inner: self.data.range(range) }
    }

    fn scan_dyn(
        &mut self,
        range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
    ) -> Box<dyn super::ScanIterator + '_> {
        Box::new(self.scan(range))
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_vec(), value);
        Ok(())
    }

    fn status(&mut self) -> Result<Status> {
        Ok(Status {
            name: "memory".to_string(),
            keys: self.data.len() as u64,
            size: self.data.iter().fold(0, |size, (k, v)| size + k.len() as u64 + v.len() as u64),
            total_disk_size: 0,
            live_disk_size: 0,
            garbage_disk_size: 0,
        })
    }
}

pub struct ScanIterator<'a> {
    inner: std::collections::btree_map::Range<'a, Vec<u8>, Vec<u8>>,
}

impl ScanIterator<'_> {
    fn map(item: (&Vec<u8>, &Vec<u8>)) -> <Self as Iterator>::Item {
        let (key, value) = item;
        Ok((key.clone(), value.clone()))
    }
}

impl Iterator for ScanIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Self::map)
    }
}

impl DoubleEndedIterator for ScanIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(Self::map)
    }
}

/// Most storage tests are Goldenscripts under src/storage/testscripts.
#[cfg(test)]
mod tests {
    use super::super::engine::test::Runner;
    use super::*;

    use test_each_file::test_each_path;

    // Run common goldenscript tests in src/storage/testscripts/engine.
    test_each_path! { in "src/storage/testscripts/engine" as engine => test_goldenscript }

    // Also run Memory-specific tests in src/storage/testscripts/memory.
    test_each_path! { in "src/storage/testscripts/memory" as scripts => test_goldenscript }

    fn test_goldenscript(path: &std::path::Path) {
        goldenscript::run(&mut Runner::new(Memory::new()), path).expect("goldenscript failed")
    }
}
