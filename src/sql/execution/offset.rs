use super::super::engine::Transaction;
use super::{Context, Executor, ResultSet};
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
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet> {
        let result = self.source.execute(ctx)?;
        if let ResultSet::Query { mut relation } = result {
            if let Some(rows) = relation.rows {
                relation.rows = Some(Box::new(rows.skip(self.offset as usize)))
            }
            return Ok(ResultSet::Query { relation });
        }
        Err(Error::Internal("Unexpected result".into()))
    }
}
