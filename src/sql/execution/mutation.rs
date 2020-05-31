use super::super::engine::Transaction;
use super::super::types::Expression;
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

/// An INSERT executor
pub struct Insert {
    table: String,
    columns: Vec<String>,
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

use std::collections::{BTreeMap, HashSet};

/// An UPDATE executor
pub struct Update<T: Transaction> {
    table: String,
    source: Box<dyn Executor<T>>,
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

/// A DELETE executor
pub struct Delete<T: Transaction> {
    table: String,
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
