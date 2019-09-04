mod file;
#[cfg(test)]
mod memory;
mod raft;

use crate::Error;
pub use file::File;
#[cfg(test)]
pub use memory::Memory;
pub use raft::Raft;

type Pair = (String, Vec<u8>);

/// A key-value store
pub trait Store: 'static + Sync + Send + std::fmt::Debug {
    /// Deletes a value in the store, regardless of whether it existed before.
    fn delete(&mut self, key: &str) -> Result<(), Error>;

    /// Gets a value from the store
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;

    /// Returns an iterator over all pairs in the store under a key prefix
    fn iter_prefix(&self, prefix: &str) -> Box<dyn Iterator<Item = Result<Pair, Error>>>;

    /// Sets a value in the store
    fn set(&mut self, key: &str, value: Vec<u8>) -> Result<(), Error>;
}

/// This is a terrible, temporary iterator implementation which is prepopulated
/// with all data, to avoid having to deal with trait lifetimes right now.
struct Iter {
    stack: Vec<Result<Pair, Error>>,
}

impl Iter {
    fn from<'a, I>(iter: I) -> Self
    where
        I: DoubleEndedIterator + Iterator<Item = (&'a String, &'a Vec<u8>)>,
    {
        Self { stack: iter.map(|(k, v)| Ok((k.clone(), v.clone()))).rev().collect() }
    }

    fn from_vec(vec: Vec<(String, Vec<u8>)>) -> Self {
        Self{
            stack: vec.into_iter().map(Ok).rev().collect(),
        }
    }
}

impl Iterator for Iter {
    type Item = Result<Pair, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.stack.pop()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub struct Suite {
        factory: Box<dyn Fn() -> Box<dyn Store>>,
    }

    impl Suite {
        pub fn new<F>(setup: F) -> Self
        where
            F: 'static + Fn() -> Box<dyn Store>,
        {
            Self { factory: Box::new(setup) }
        }

        fn setup(&self) -> Box<dyn Store> {
            (&self.factory)()
        }

        pub fn test(&self) {
            self.test_delete();
            self.test_get();
            self.test_iter_prefix();
            self.test_set();
        }

        pub fn test_delete(&self) {
            let mut s = self.setup();
            s.set("a", vec![0x01]).unwrap();
            assert_eq!(vec![0x01], s.get("a").unwrap().unwrap());
            s.delete("a").unwrap();
            assert_eq!(None, s.get("a").unwrap());
            s.delete("b").unwrap();
        }

        pub fn test_get(&self) {
            let mut s = self.setup();
            s.set("a", vec![0x01]).unwrap();
            assert_eq!(vec![0x01], s.get("a").unwrap().unwrap());
            assert_eq!(None, s.get("b").unwrap());
        }

        pub fn test_iter_prefix(&self) {
            let mut s = self.setup();
            s.set("a", vec![0x01]).unwrap();
            s.set("b", vec![0x02]).unwrap();
            s.set("ba", vec![0x02, 0x01]).unwrap();
            s.set("bb", vec![0x02, 0x02]).unwrap();
            s.set("c", vec![0x03]).unwrap();

            assert_eq!(
                vec![
                    ("b".to_string(), vec![0x02]),
                    ("ba".to_string(), vec![0x02, 0x01]),
                    ("bb".to_string(), vec![0x02, 0x02]),
                ],
                s.iter_prefix("b").collect::<Result<Vec<(String, Vec<u8>)>, Error>>().unwrap()
            )
        }

        pub fn test_set(&self) {
            let mut s = self.setup();
            s.set("a", vec![0x01]).unwrap();
            assert_eq!(vec![0x01], s.get("a").unwrap().unwrap());
            s.set("a", vec![0x02]).unwrap();
            assert_eq!(vec![0x02], s.get("a").unwrap().unwrap());
        }
    }
}
