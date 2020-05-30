use super::super::engine::Transaction;
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

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
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_read_table(&self.table)?;
        let mut count = 0;
        match self.source.execute(txn)? {
            ResultSet::Query { mut rows, .. } => {
                while let Some(row) = rows.next().transpose()? {
                    txn.delete(&table.name, &table.get_row_key(&row)?)?;
                    count += 1
                }
                Ok(ResultSet::Delete { count })
            }
            r => Err(Error::Internal(format!("Unexpected result {:?}", r))),
        }
    }
}
