use super::{Memory, Range, Storage};
use crate::Error;

use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

/// Key-value storage backend for testing. Protects an inner Memory backend
/// using a mutex, so it can be cloned and inspected.
#[derive(Clone)]
pub struct Test {
    /// The underlying key-value store.
    data: Arc<RwLock<Memory>>,
}

impl Test {
    /// Creates a new Test key-value storage engine.
    pub fn new() -> Self {
        Self { data: Arc::new(RwLock::new(Memory::new())) }
    }
}

impl Storage for Test {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.data.read()?.read(key)
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), Error> {
        self.data.write()?.remove(key)
    }

    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Range {
        // Since the mutex guard is scoped to this method, we have to
        // buffer the result and return an owned iterator.
        Box::new(
            self.data.read().unwrap().scan(range).collect::<Vec<Result<_, Error>>>().into_iter(),
        )
    }

    fn write(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        self.data.write()?.write(key, value)
    }
}

#[cfg(test)]
impl super::TestSuite<Test> for Test {
    fn setup() -> Result<Self, Error> {
        Ok(Test::new())
    }
}

#[test]
fn tests() -> Result<(), Error> {
    use super::TestSuite;
    Test::test()
}
