use super::super::engine::Transaction;
use super::super::types::{Column, Row, Value};
use super::{Executor, ResultSet};
use crate::error::Result;

use std::collections::HashSet;

/// A primary key lookup executor
pub struct IndexLookup {
    /// The table to look up
    table: String,
    /// The table alias to use
    /// FIXME Shouldn't be here, see: https://github.com/erikgrinaker/toydb/issues/21
    alias: Option<String>,
    /// The column index to use
    column: String,
    /// The index values to look up
    keys: Vec<Value>,
}

impl IndexLookup {
    pub fn new(
        table: String,
        alias: Option<String>,
        column: String,
        keys: Vec<Value>,
    ) -> Box<Self> {
        Box::new(Self { table, alias, column, keys })
    }
}

impl<T: Transaction> Executor<T> for IndexLookup {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_read_table(&self.table)?;
        let name = if let Some(alias) = &self.alias { alias } else { &table.name };

        let mut pks: HashSet<Value> = HashSet::new();
        for key in self.keys {
            pks.extend(txn.read_index(&self.table, &self.column, &key)?);
        }

        // FIXME Is there a way to pass the txn into an iterator closure instead?
        let rows = pks
            .into_iter()
            .filter_map(|pk| txn.read(&table.name, &pk).transpose())
            .collect::<Result<Vec<Row>>>()?;

        Ok(ResultSet::Query {
            columns: table
                .columns
                .iter()
                .map(|c| Column { table: Some(name.clone()), name: Some(c.name.clone()) })
                .collect(),
            rows: Box::new(rows.into_iter().map(Ok)),
        })
    }
}
