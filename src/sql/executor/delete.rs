use super::super::engine::Transaction;
use super::{Context, Effect, Executor, ResultSet};
use crate::Error;

/// A DELETE executor
pub struct Delete<T: Transaction> {
    /// Table name to delete from
    table: String,
    /// Source of rows to delete (must be complete rows from the table)
    source: Box<dyn Executor<T>>,
}

impl<T: Transaction> Delete<T> {
    pub fn new(table: String, source: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { table, source })
    }
}

impl<T: Transaction> Executor<T> for Delete<T> {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        let table = ctx.txn.must_read_table(&self.table)?;
        let mut count = 0;
        let mut rows = self.source.execute(ctx)?;
        while let Some(row) = rows.next().transpose()? {
            ctx.txn.delete(&table.name, &table.get_row_key(&row)?)?;
            count += 1
        }
        Ok(ResultSet::from_effect(Effect::Delete { count }))
    }
}
