mod file;
#[cfg(test)]
mod memory;

use crate::Error;
pub use file::File;
#[cfg(test)]
pub use memory::Memory;

/// A key-value store
pub trait Store: 'static + Sync + Send + std::fmt::Debug {
    /// Deletes a value in the store, regardless of whether it existed before.
    fn delete(&mut self, key: &str) -> Result<(), Error>;

    /// Gets a value from the store
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;

    /// Sets a value in the store
    fn set(&mut self, key: &str, value: Vec<u8>) -> Result<(), Error>;
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub struct Suite {
        factory: Box<Fn() -> Box<Store>>,
    }

    impl Suite {
        pub fn new<F>(setup: F) -> Self
        where
            F: 'static + Fn() -> Box<Store>,
        {
            Self { factory: Box::new(setup) }
        }

        fn setup(&self) -> Box<Store> {
            (&self.factory)()
        }

        pub fn test(&self) {
            self.test_delete();
            self.test_get();
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

        pub fn test_set(&self) {
            let mut s = self.setup();
            s.set("a", vec![0x01]).unwrap();
            assert_eq!(vec![0x01], s.get("a").unwrap().unwrap());
            s.set("a", vec![0x02]).unwrap();
            assert_eq!(vec![0x02], s.get("a").unwrap().unwrap());
        }
    }
}
