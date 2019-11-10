mod file;
#[cfg(test)]
mod memory;
mod raft;

use crate::Error;
pub use file::File;
#[cfg(test)]
pub use memory::Memory;
pub use raft::Raft;

type Range = dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), Error>> + Sync + Send;

/// A key-value store
pub trait Store: 'static + Sync + Send + std::fmt::Debug {
    /// Deletes a value in the store, regardless of whether it existed before.
    fn delete(&mut self, key: &[u8]) -> Result<(), Error>;

    /// Gets a value from the store
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;

    /// Returns an iterator over all pairs in the store under a key prefix
    fn iter_prefix(&self, prefix: &[u8]) -> Box<Range>;

    /// Sets a value in the store
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error>;
}

/// This is a terrible, temporary iterator implementation which is prepopulated
/// with all data, to avoid having to deal with trait lifetimes right now.
struct Iter {
    #[allow(clippy::type_complexity)]
    stack: Vec<Result<(Vec<u8>, Vec<u8>), Error>>,
}

impl Iter {
    fn from<'a, I>(iter: I) -> Self
    where
        I: DoubleEndedIterator + Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)>,
    {
        Self { stack: iter.map(|(k, v)| Ok((k.clone(), v.clone()))).rev().collect() }
    }

    fn from_vec(vec: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { stack: vec.into_iter().map(Ok).rev().collect() }
    }
}

impl Iterator for Iter {
    type Item = Result<(Vec<u8>, Vec<u8>), Error>;

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
            s.set(b"a", vec![0x01]).unwrap();
            assert_eq!(vec![0x01], s.get(b"a").unwrap().unwrap());
            s.delete(b"a").unwrap();
            assert_eq!(None, s.get(b"a").unwrap());
            s.delete(b"b").unwrap();
        }

        pub fn test_get(&self) {
            let mut s = self.setup();
            s.set(b"a", vec![0x01]).unwrap();
            assert_eq!(vec![0x01], s.get(b"a").unwrap().unwrap());
            assert_eq!(None, s.get(b"b").unwrap());
        }

        pub fn test_iter_prefix(&self) {
            let mut s = self.setup();
            s.set(b"a", vec![0x01]).unwrap();
            s.set(b"b", vec![0x02]).unwrap();
            s.set(b"ba", vec![0x02, 0x01]).unwrap();
            s.set(b"bb", vec![0x02, 0x02]).unwrap();
            s.set(b"c", vec![0x03]).unwrap();

            assert_eq!(
                vec![
                    (b"b".to_vec(), vec![0x02]),
                    (b"ba".to_vec(), vec![0x02, 0x01]),
                    (b"bb".to_vec(), vec![0x02, 0x02]),
                ],
                s.iter_prefix(b"b").collect::<Result<Vec<(Vec<u8>, Vec<u8>)>, Error>>().unwrap()
            )
        }

        pub fn test_set(&self) {
            let mut s = self.setup();
            s.set(b"a", vec![0x01]).unwrap();
            assert_eq!(vec![0x01], s.get(b"a").unwrap().unwrap());
            s.set(b"a", vec![0x02]).unwrap();
            assert_eq!(vec![0x02], s.get(b"a").unwrap().unwrap());
        }
    }
}
