use super::super::engine::Transaction;
use super::super::types::{Column, Expression};
use super::{Context, Executor, ResultSet};
use crate::error::Result;

/// A table scan executor
pub struct Scan {
    table: String,
    label: String,
    filter: Option<Expression>,
}

impl Scan {
    // FIXME label should not be necessary.
    pub fn new(table: String, label: String, filter: Option<Expression>) -> Box<Self> {
        Box::new(Self { table, label, filter })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet> {
        let table = ctx.txn.must_read_table(&self.table)?;
        Ok(ResultSet::Query {
            columns: table
                .columns
                .iter()
                .map(|c| Column { table: Some(self.label.clone()), name: Some(c.name.clone()) })
                .collect(),
            rows: Box::new(ctx.txn.scan(&table.name, self.filter)?),
        })
    }
}
