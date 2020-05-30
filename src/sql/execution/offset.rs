use super::super::engine::Transaction;
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

/// An OFFSET executor
pub struct Offset<T: Transaction> {
    /// The source of rows to limit
    source: Box<dyn Executor<T>>,
    /// The number of rows to skip
    offset: u64,
}

impl<T: Transaction> Offset<T> {
    pub fn new(source: Box<dyn Executor<T>>, offset: u64) -> Box<Self> {
        Box::new(Self { source, offset })
    }
}

impl<T: Transaction> Executor<T> for Offset<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Query { columns, rows } = self.source.execute(txn)? {
            Ok(ResultSet::Query { columns, rows: Box::new(rows.skip(self.offset as usize)) })
        } else {
            Err(Error::Internal("Unexpected result".into()))
        }
    }
}
