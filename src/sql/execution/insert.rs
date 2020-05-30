use super::super::engine::Transaction;
use super::super::types::Expressions;
use super::{Context, Executor, ResultSet};
use crate::error::Result;

/// An INSERT executor
pub struct Insert {
    /// The table to insert into
    table: String,
    /// The columns to insert into
    columns: Vec<String>,
    /// The row expressions to insert
    rows: Vec<Expressions>,
}

impl Insert {
    pub fn new(table: String, columns: Vec<String>, rows: Vec<Expressions>) -> Box<Self> {
        Box::new(Self { table, columns, rows })
    }
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet> {
        let table = ctx.txn.must_read_table(&self.table)?;
        let mut count = 0;
        for expressions in self.rows {
            let mut row =
                expressions.into_iter().map(|expr| expr.evaluate(None)).collect::<Result<_>>()?;
            if self.columns.is_empty() {
                row = table.pad_row(row)?;
            } else {
                row = table.make_row(&self.columns, row)?;
            }
            ctx.txn.create(&table.name, row)?;
            count += 1;
        }
        Ok(ResultSet::Create { count })
    }
}
