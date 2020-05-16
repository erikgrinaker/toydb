use super::{Memory, Scan, Store};
use crate::error::Result;

use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

/// Key-value storage backend for testing. Protects an inner Memory backend using a mutex, so it can
/// be cloned and inspected.
#[derive(Clone)]
pub struct Test {
    kv: Arc<RwLock<Memory>>,
}

impl Test {
    /// Creates a new Test key-value storage engine.
    pub fn new() -> Self {
        Self { kv: Arc::new(RwLock::new(Memory::new())) }
    }
}

impl Store for Test {
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.kv.write()?.delete(key)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.read()?.get(key)
    }

    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Scan {
        // Since the mutex guard is scoped to this method, we simply buffer the result.
        Box::new(self.kv.read().unwrap().scan(range).collect::<Vec<Result<_>>>().into_iter())
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.write()?.set(key, value)
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
