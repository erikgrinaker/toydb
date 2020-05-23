use super::{Memory, Range, Scan, Store};
use crate::error::Result;

use std::fmt::Display;
use std::sync::{Arc, RwLock};

/// Log storage backend for testing. Protects an inner Memory backend using a mutex, so it can
/// be cloned and inspected.
#[derive(Clone)]
pub struct Test {
    store: Arc<RwLock<Memory>>,
}

impl Test {
    /// Creates a new Test key-value storage engine.
    pub fn new() -> Self {
        Self { store: Arc::new(RwLock::new(Memory::new())) }
    }
}

impl Display for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test")
    }
}

impl Store for Test {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64> {
        self.store.write()?.append(entry)
    }

    fn commit(&mut self, index: u64) -> Result<()> {
        self.store.write()?.commit(index)
    }

    fn committed(&self) -> u64 {
        self.store.read().unwrap().committed()
    }

    fn get(&self, index: u64) -> Result<Option<Vec<u8>>> {
        self.store.read()?.get(index)
    }

    fn len(&self) -> u64 {
        self.store.read().unwrap().len()
    }

    fn scan(&self, range: Range) -> Scan {
        // Since the mutex guard is scoped to this method, we simply buffer the result.
        Box::new(self.store.read().unwrap().scan(range).collect::<Vec<Result<_>>>().into_iter())
    }

    fn size(&self) -> u64 {
        self.store.read().unwrap().size()
    }

    fn truncate(&mut self, index: u64) -> Result<u64> {
        self.store.write()?.truncate(index)
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.store.read()?.get_metadata(key)
    }

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.store.write()?.set_metadata(key, value)
    }
}

#[cfg(test)]
impl super::TestSuite<Test> for Test {
    fn setup() -> Result<Self> {
        Ok(Test::new())
    }
}

#[test]
fn tests() -> Result<()> {
    use super::TestSuite;
    Test::test()
}
