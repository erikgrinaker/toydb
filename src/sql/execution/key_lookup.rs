use super::super::engine::Transaction;
use super::super::types::{Column, Relation, Row, Value};
use super::{Context, Executor, ResultSet};
use crate::Error;

/// A primary key lookup executor
pub struct KeyLookup {
    /// The table to look up
    table: String,
    /// The table alias to use
    /// FIXME Shouldn't be here, see: https://github.com/erikgrinaker/toydb/issues/21
    alias: Option<String>,
    /// The primary keys to look up
    keys: Vec<Value>,
}

impl KeyLookup {
    pub fn new(table: String, alias: Option<String>, keys: Vec<Value>) -> Box<Self> {
        Box::new(Self { table, alias, keys })
    }
}

impl<T: Transaction> Executor<T> for KeyLookup {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        let table = ctx.txn.must_read_table(&self.table)?;
        let name = if let Some(alias) = &self.alias { alias } else { &table.name };

        // FIXME Is there a way to pass the txn into an iterator closure instead?
        let rows = self
            .keys
            .into_iter()
            .filter_map(|key| ctx.txn.read(&table.name, &key).transpose())
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
