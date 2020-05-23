use super::{Range, Store};
use crate::error::{Error, Result};

use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Bound;

// An in-memory log store.
pub struct Memory {
    log: Vec<Vec<u8>>,
    committed: u64,
    metadata: HashMap<Vec<u8>, Vec<u8>>,
}

impl Memory {
    /// Creates a new in-memory log.
    pub fn new() -> Self {
        Self { log: Vec::new(), committed: 0, metadata: HashMap::new() }
    }
}

impl Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl Store for Memory {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64> {
        self.log.push(entry);
        Ok(self.log.len() as u64)
    }

    fn commit(&mut self, index: u64) -> Result<()> {
        if index > self.len() {
            return Err(Error::Internal(format!("Cannot commit non-existant index {}", index)));
        }
        if index < self.committed {
            return Err(Error::Internal(format!(
                "Cannot commit below current index {}",
                self.committed
            )));
        }
        self.committed = index;
        Ok(())
    }

    fn committed(&self) -> u64 {
        self.committed
    }

    fn get(&self, index: u64) -> Result<Option<Vec<u8>>> {
        match index {
            0 => Ok(None),
            i => Ok(self.log.get(i as usize - 1).cloned()),
        }
    }

    fn len(&self) -> u64 {
        self.log.len() as u64
    }

    fn scan(&self, range: Range) -> super::Scan {
        Box::new(
            self.log
                .iter()
                .take(match range.end {
                    Bound::Included(n) => n as usize,
                    Bound::Excluded(0) => 0,
                    Bound::Excluded(n) => n as usize - 1,
                    Bound::Unbounded => std::usize::MAX,
                })
                .skip(match range.start {
                    Bound::Included(0) => 0,
                    Bound::Included(n) => n as usize - 1,
                    Bound::Excluded(n) => n as usize,
                    Bound::Unbounded => 0,
                })
                .cloned()
                .map(Ok),
        )
    }

    fn size(&self) -> u64 {
        self.log.iter().map(|v| v.len() as u64).sum()
    }

    fn truncate(&mut self, index: u64) -> Result<u64> {
        if index < self.committed {
            return Err(Error::Internal(format!(
                "Cannot truncate below committed index {}",
                self.committed
            )));
        }
        self.log.truncate(index as usize);
        Ok(self.log.len() as u64)
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.metadata.get(key).cloned())
    }

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.metadata.insert(key.to_vec(), value);
        Ok(())
    }
}

#[cfg(test)]
impl super::TestSuite<Memory> for Memory {
    fn setup() -> Result<Self> {
        Ok(Memory::new())
    }
}

#[test]
fn tests() -> Result<()> {
    use super::TestSuite;
    Memory::test()
}
