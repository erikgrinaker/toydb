use super::Store;
use crate::error::Result;

/// An in-memory key/value store using the Rust standard library B-tree
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

impl std::fmt::Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl Store for Memory {
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

    fn scan<R: std::ops::RangeBounds<Vec<u8>>>(&mut self, range: R) -> Self::ScanIterator<'_> {
        ScanIterator { inner: self.data.range(range) }
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_vec(), value);
        Ok(())
    }
}

pub struct ScanIterator<'a> {
    inner: std::collections::btree_map::Range<'a, Vec<u8>, Vec<u8>>,
}

impl<'a> ScanIterator<'a> {
    fn map(item: (&Vec<u8>, &Vec<u8>)) -> <Self as Iterator>::Item {
        let (key, value) = item;
        Ok((key.clone(), value.clone()))
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Self::map)
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(Self::map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    super::super::tests::test_store!(Memory::new());
}
