use super::super::engine::Transaction;
use super::{Context, Executor, ResultSet};
use crate::Error;

/// A table scan executor
pub struct Scan {
    /// The table to scan
    table: String,
}

impl Scan {
    pub fn new(table: String) -> Box<Self> {
        Box::new(Self { table })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        let table = ctx.txn.must_read_table(&self.table)?;
        let columns = table.columns.into_iter().map(|c| c.name).collect();
        let rows = ctx.txn.scan(&table.name)?;
        // FIXME We use extra Box to cast to ResultRows iterator (apparently)
        Ok(ResultSet::from_rows(columns, Box::new(rows)))
    }
}
