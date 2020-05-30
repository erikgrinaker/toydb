use super::super::engine::Transaction;
use super::super::types::Expression;
use super::{Executor, ResultSet};
use crate::error::Result;

/// An INSERT executor
pub struct Insert {
    /// The table to insert into
    table: String,
    /// The columns to insert into
    columns: Vec<String>,
    /// The row expressions to insert
    rows: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table: String, columns: Vec<String>, rows: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self { table, columns, rows })
    }
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_read_table(&self.table)?;
        let mut count = 0;
        for expressions in self.rows {
            let mut row =
                expressions.into_iter().map(|expr| expr.evaluate(None)).collect::<Result<_>>()?;
            if self.columns.is_empty() {
                row = table.pad_row(row)?;
            } else {
                row = table.make_row(&self.columns, row)?;
            }
            txn.create(&table.name, row)?;
            count += 1;
        }
        Ok(ResultSet::Create { count })
    }
}
