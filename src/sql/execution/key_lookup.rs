use super::super::engine::Transaction;
use super::super::types::{Column, Row, Value};
use super::{Executor, ResultSet};
use crate::error::Result;

/// A primary key lookup executor
pub struct KeyLookup {
    /// The table to look up
    table: String,
    /// The primary keys to look up
    keys: Vec<Value>,
}

impl KeyLookup {
    pub fn new(table: String, keys: Vec<Value>) -> Box<Self> {
        Box::new(Self { table, keys })
    }
}

impl<T: Transaction> Executor<T> for KeyLookup {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_read_table(&self.table)?;

        // FIXME Is there a way to pass the txn into an iterator closure instead?
        let rows = self
            .keys
            .into_iter()
            .filter_map(|key| txn.read(&table.name, &key).transpose())
            .collect::<Result<Vec<Row>>>()?;

        Ok(ResultSet::Query {
            columns: table.columns.iter().map(|c| Column { name: Some(c.name.clone()) }).collect(),
            rows: Box::new(rows.into_iter().map(Ok)),
        })
    }
}
