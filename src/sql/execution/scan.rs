use super::super::engine::Transaction;
use super::super::types::{Column, Expression, Relation};
use super::{Context, Executor, ResultSet};
use crate::error::Result;

/// A table scan executor
pub struct Scan {
    /// The table to scan
    table: String,
    /// The table alias to use
    alias: Option<String>,
    /// The row filter to apply
    filter: Option<Expression>,
}

impl Scan {
    pub fn new(table: String, alias: Option<String>, filter: Option<Expression>) -> Box<Self> {
        Box::new(Self { table, alias, filter })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet> {
        let table = ctx.txn.must_read_table(&self.table)?;
        let name = if let Some(alias) = &self.alias { alias } else { &table.name };
        Ok(ResultSet::Query {
            relation: Relation {
                columns: table
                    .columns
                    .iter()
                    .map(|c| Column { relation: Some(name.clone()), name: Some(c.name.clone()) })
                    .collect(),
                rows: Some(Box::new(ctx.txn.scan(&table.name, self.filter)?)),
            },
        })
    }
}
