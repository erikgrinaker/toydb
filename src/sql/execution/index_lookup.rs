use super::super::engine::Transaction;
use super::super::types::{Column, Relation, Row, Value};
use super::{Context, Executor, ResultSet};
use crate::Error;

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
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        let table = ctx.txn.must_read_table(&self.table)?;
        let name = if let Some(alias) = &self.alias { alias } else { &table.name };

        let mut pks: HashSet<Value> = HashSet::new();
        for key in self.keys {
            pks.extend(ctx.txn.read_index(&self.table, &self.column, &key)?);
        }

        // FIXME Is there a way to pass the txn into an iterator closure instead?
        let rows = pks
            .into_iter()
            .filter_map(|pk| ctx.txn.read(&table.name, &pk).transpose())
            .collect::<Result<Vec<Row>, Error>>()?;

        Ok(ResultSet::Query {
            relation: Relation {
                columns: table
                    .columns
                    .iter()
                    .map(|c| Column { relation: Some(name.clone()), name: Some(c.name.clone()) })
                    .collect(),
                rows: Some(Box::new(rows.into_iter().map(Ok))),
            },
        })
    }
}
