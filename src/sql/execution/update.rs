use super::super::engine::Transaction;
use super::super::types::Expression;
use super::{Context, Executor, ResultSet};
use crate::error::{Error, Result};

use std::collections::BTreeMap;

/// An UPDATE executor
pub struct Update<T: Transaction> {
    /// The table to update
    table: String,
    /// The source of rows to update
    source: Box<dyn Executor<T>>,
    /// The expressions to update columns with
    /// FIXME Uses BTreeMap instead of HashMap for test stability
    expressions: BTreeMap<String, Expression>,
}

impl<T: Transaction> Update<T> {
    pub fn new(
        table: String,
        source: Box<dyn Executor<T>>,
        expressions: BTreeMap<String, Expression>,
    ) -> Box<Self> {
        Box::new(Self { table, source, expressions })
    }
}

impl<T: Transaction> Executor<T> for Update<T> {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet> {
        match self.source.execute(ctx)? {
            ResultSet::Query { mut relation } => {
                let table = ctx.txn.must_read_table(&self.table)?;
                let mut count = 0;
                while let Some(row) = relation.next().transpose()? {
                    let id = table.get_row_key(&row)?;
                    let env = table.row_env(&row);
                    let mut new = row.clone();
                    for (field, expr) in &self.expressions {
                        table.set_row_field(&mut new, field, expr.evaluate(&env)?)?;
                    }
                    ctx.txn.update(&table.name, &id, new)?;
                    count += 1
                }
                Ok(ResultSet::Update { count })
            }
            r => Err(Error::Internal(format!("Unexpected response {:?}", r))),
        }
    }
}
