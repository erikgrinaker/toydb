use super::super::engine::Transaction;
use super::{Context, Executor, ResultColumns, ResultSet};
use crate::Error;

/// A table scan executor
pub struct Scan {
    /// The table to scan
    table: String,
    /// The table alias to use
    alias: Option<String>,
}

impl Scan {
    pub fn new(table: String, alias: Option<String>) -> Box<Self> {
        Box::new(Self { table, alias })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        let table = ctx.txn.must_read_table(&self.table)?;
        let name = if let Some(alias) = &self.alias { alias } else { &table.name };
        let columns = ResultColumns::new(
            table.columns.iter().map(|c| (Some(name.clone()), Some(c.name.clone()))).collect(),
        );
        let rows = ctx.txn.scan(&table.name)?;
        // FIXME We use extra Box to cast to ResultRows iterator (apparently)
        Ok(ResultSet::from_rows(columns, Box::new(rows)))
    }
}
