use std::collections::HashMap;

/// A KVStore is a persistent key-value store. It's currently implemented as a
/// transient in-memory store while prototyping the interface.
pub struct KVStore<'a> {
    data: HashMap<&'a str, String>,
}

impl<'a> KVStore<'a> {
    /// Creates a new KVStore
    pub fn new() -> KVStore<'a> {
        KVStore { data: HashMap::new() }
    }

    /// Deletes a value from the store
    pub fn delete(&mut self, key: &str) {
        self.data.remove(key);
    }

    /// Gets a value from the store
    pub fn get(&mut self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    /// Puts a value into the store
    pub fn put(&mut self, key: &'a str, value: String) {
        self.data.insert(key, value);
    }
}
