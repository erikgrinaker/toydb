use super::{Engine, Status};
use crate::error::Result;

/// A debug engine, which wraps another engine and logs mutations.
pub struct Debug<E: Engine> {
    /// The wrapped engine.
    inner: E,
    /// Write log as key/value tuples. Value is None for deletes.
    write_log: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl<E: Engine> std::fmt::Display for Debug<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "debug:{}", self.inner)
    }
}

impl<E: Engine> Debug<E> {
    pub fn new(inner: E) -> Self {
        Self { inner, write_log: Vec::new() }
    }

    /// Returns and resets the write log. The next call only returns new writes.
    pub fn take_write_log(&mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let mut write_log = Vec::new();
        std::mem::swap(&mut write_log, &mut self.write_log);
        write_log
    }
}

impl<E: Engine> Engine for Debug<E> {
    type ScanIterator<'a> = E::ScanIterator<'a> where E: 'a;

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)?;
        self.write_log.push((key.to_vec(), None));
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    fn scan<R: std::ops::RangeBounds<Vec<u8>>>(&mut self, range: R) -> Self::ScanIterator<'_> {
        self.inner.scan(range)
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.inner.set(key, value.clone())?;
        self.write_log.push((key.to_vec(), Some(value)));
        Ok(())
    }

    fn status(&mut self) -> Result<Status> {
        self.inner.status()
    }
}

#[cfg(test)]
mod tests {
    use super::super::Memory;
    use super::*;

    super::super::tests::test_engine!(Debug::new(Memory::new()));
}
