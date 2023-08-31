use super::{Memory, Range, Scan, Store};
use crate::error::Result;

use std::fmt::Display;
use std::sync::{Arc, Mutex};

/// Key-value storage backend for testing. Protects an inner Memory backend using a mutex, so it can
/// be cloned and inspected.
#[derive(Clone)]
pub struct Test {
    kv: Arc<Mutex<Memory>>,
}

impl Test {
    /// Creates a new Test key-value storage engine.
    pub fn new() -> Self {
        Self { kv: Arc::new(Mutex::new(Memory::new())) }
    }
}

impl Display for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test")
    }
}

impl Store for Test {
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.kv.lock()?.delete(key)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.lock()?.get(key)
    }

    fn scan(&mut self, range: Range) -> Scan {
        // Since the mutex guard is scoped to this method, we simply buffer the result.
        Box::new(self.kv.lock().unwrap().scan(range).collect::<Vec<Result<_>>>().into_iter())
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.lock()?.set(key, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    super::super::tests::test_store!(Test::new());
}
