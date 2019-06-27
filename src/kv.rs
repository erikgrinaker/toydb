use super::Error;
use std::collections::BTreeMap;

/// A Store is a persistent key-value store for values of type V, serialized as
/// MessagePack. It's currently implemented as a transient in-memory store while
/// prototyping the interface.
pub struct Store {
    data: BTreeMap<String, Vec<u8>>,
}

impl Store {
    /// Creates a new Store
    pub fn new() -> Self {
        Store { data: BTreeMap::new() }
    }

    /// Deletes a value from the store
    pub fn delete(&mut self, key: &str) -> Result<bool, Error> {
        Ok(self.data.remove(key).is_some())
    }

    /// Gets a value from the store
    pub fn get<'de, V: serde::Deserialize<'de>>(&mut self, key: &str) -> Result<Option<V>, Error> {
        Ok(match self.data.get(key) {
            Some(v) => Some(serde::Deserialize::deserialize(&mut rmps::Deserializer::new(&v[..]))?),
            None => None,
        })
    }

    /// Sets a value in the store
    pub fn set<V: serde::Serialize>(&mut self, key: &str, value: V) -> Result<String, Error> {
        let mut buffer = Vec::new();
        value.serialize(&mut rmps::Serializer::new(&mut buffer))?;
        self.data.insert(key.to_owned(), buffer);
        Ok(key.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete() {
        let mut s = Store::new();

        s.set("a", "1").unwrap();
        assert_eq!("1", s.get::<String>("a").unwrap().unwrap());

        assert_eq!(true, s.delete("a").unwrap());
        assert_eq!(None, s.get::<String>("a").unwrap());

        assert_eq!(false, s.delete("b").unwrap());
    }

    #[test]
    fn test_get() {
        let mut s = Store::new();
        s.set("a", "1").unwrap();
        assert_eq!("1", s.get::<String>("a").unwrap().unwrap());
        assert_eq!(None, s.get::<String>("b").unwrap());
    }

    #[test]
    fn test_set() {
        let mut s = Store::new();
        s.set("a", "1").unwrap();
        assert_eq!("1", s.get::<String>("a").unwrap().unwrap());
        s.set("a", "2").unwrap();
        assert_eq!("2", s.get::<String>("a").unwrap().unwrap());
        s.set("a", 3).unwrap();
        assert_eq!(3, s.get::<u64>("a").unwrap().unwrap())
    }
}