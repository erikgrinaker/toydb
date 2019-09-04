use super::{Iter, Pair, Store};
use crate::Error;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// An in-memory key-value store. It is primarily used for
/// prototyping and testing, and protects its internal state
/// using a arc-mutex so it can be shared between threads.
#[derive(Clone, Debug)]
pub struct Memory {
    data: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
}

impl Memory {
    /// Creates a new Memory key-value store
    pub fn new() -> Self {
        Self { data: Arc::new(RwLock::new(BTreeMap::new())) }
    }
}

impl Store for Memory {
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.data.clone().write()?.remove(key);
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.data.clone().read()?.get(key).cloned())
    }

    fn iter_prefix(&self, prefix: &str) -> Box<dyn Iterator<Item = Result<Pair, Error>>> {
        let from = prefix.to_string();
        let to = from.clone() + &std::char::MAX.to_string();
        Box::new(Iter::from(self.data.read().unwrap().range(from..to)))
    }

    fn set(&mut self, key: &str, value: Vec<u8>) -> Result<(), Error> {
        self.data.clone().write()?.insert(key.to_string(), value);
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
