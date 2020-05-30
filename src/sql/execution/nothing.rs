use super::super::engine::Transaction;
use super::super::types::Row;
use super::{Executor, ResultSet};
use crate::error::Result;

/// An executor that produces a single empty row
pub struct Nothing;

impl Nothing {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}

impl<T: Transaction> Executor<T> for Nothing {
    fn execute(self: Box<Self>, _: &mut T) -> Result<ResultSet> {
        Ok(ResultSet::Query {
            columns: Vec::new(),
            rows: Box::new(std::iter::once(Ok(Row::new()))),
        })
    }
}
