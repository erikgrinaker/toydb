use super::{Range, Scan, Store};
use crate::error::Result;

use std::collections::BTreeMap;
use std::fmt::Display;

/// In-memory key-value store using the Rust standard library B-tree implementation.
pub struct StdMemory {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl StdMemory {
    /// Creates a new Memory key-value storage engine.
    pub fn new() -> Self {
        Self { data: BTreeMap::new() }
    }
}

impl Display for StdMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stdmemory")
    }
}

impl Store for StdMemory {
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

    fn scan(&mut self, range: Range) -> Scan {
        Box::new(self.data.range(range).map(|(k, v)| Ok((k.clone(), v.clone()))))
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_vec(), value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    super::super::tests::test_store!(StdMemory::new());
}
