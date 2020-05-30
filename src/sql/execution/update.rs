use super::super::engine::Transaction;
use super::super::types::Expression;
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

use std::collections::{BTreeMap, HashSet};

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
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Query { mut rows, .. } => {
                let table = txn.must_read_table(&self.table)?;

                // The iterator will see our changes, such that the same item may be iterated over
                // multiple times. We keep track of the primary keys here to avoid that, althought
                // it may cause ballooning memory usage for large updates.
                //
                // FIXME This is not safe for primary key updates, which may still be processed
                // multiple times - it should be possible to come up with a pathological case that
                // loops forever (e.g. UPDATE test SET id = id + 1).
                let mut updated = HashSet::new();
                while let Some(row) = rows.next().transpose()? {
                    let id = table.get_row_key(&row)?;
                    if updated.contains(&id) {
                        continue;
                    }
                    let mut new = row.clone();
                    for (field, expr) in &self.expressions {
                        table.set_row_field(&mut new, field, expr.evaluate(Some(&row))?)?;
                    }
                    txn.update(&table.name, &id, new)?;
                    updated.insert(id);
                }
                Ok(ResultSet::Update { count: updated.len() as u64 })
            }
            r => Err(Error::Internal(format!("Unexpected response {:?}", r))),
        }
    }
}
