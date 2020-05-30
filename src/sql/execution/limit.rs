use super::super::engine::Transaction;
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

/// A LIMIT executor
pub struct Limit<T: Transaction> {
    /// The source of rows to limit
    source: Box<dyn Executor<T>>,
    /// The number of rows to limit results to
    limit: u64,
}

impl<T: Transaction> Limit<T> {
    pub fn new(source: Box<dyn Executor<T>>, limit: u64) -> Box<Self> {
        Box::new(Self { source, limit })
    }
}

impl<T: Transaction> Executor<T> for Limit<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Query { columns, rows } = self.source.execute(txn)? {
            Ok(ResultSet::Query { columns, rows: Box::new(rows.take(self.limit as usize)) })
        } else {
            Err(Error::Internal("Unexpected result".into()))
        }
    }
}
