use super::{Iter, Range, Store};
use crate::Error;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// An in-memory key-value store. It is primarily used for
/// prototyping and testing, and protects its internal state
/// using a arc-mutex so it can be shared between threads.
#[derive(Clone, Debug)]
pub struct Memory {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl Memory {
    /// Creates a new Memory key-value store
    pub fn new() -> Self {
        Self { data: Arc::new(RwLock::new(BTreeMap::new())) }
    }
}

impl Store for Memory {
    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.data.write()?.remove(key);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.data.read()?.get(key).cloned())
    }

    fn iter_prefix(&self, prefix: &[u8]) -> Box<Range> {
        let from = prefix.to_vec();
        let mut to = from.clone();
        to.extend_from_slice(std::char::MAX.to_string().as_bytes());
        Box::new(Iter::from(self.data.read().unwrap().range(from..to)))
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        self.data.write()?.insert(key.to_vec(), value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::Suite;
    use super::*;

    #[test]
    fn suite() {
        Suite::new(|| Box::new(Memory::new())).test()
    }
}
