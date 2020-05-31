use super::super::engine::Transaction;
use super::super::types::{Column, Expression, Row, Value};
use super::{Executor, ResultSet};
use crate::error::Result;

use std::collections::HashSet;

/// A table scan executor
pub struct Scan {
    table: String,
    filter: Option<Expression>,
}

impl Scan {
    pub fn new(table: String, filter: Option<Expression>) -> Box<Self> {
        Box::new(Self { table, filter })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_read_table(&self.table)?;
        Ok(ResultSet::Query {
            columns: table.columns.iter().map(|c| Column { name: Some(c.name.clone()) }).collect(),
            rows: Box::new(txn.scan(&table.name, self.filter)?),
        })
    }
}

/// A primary key lookup executor
pub struct KeyLookup {
    table: String,
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

/// An index value lookup executor
pub struct IndexLookup {
    table: String,
    column: String,
    values: Vec<Value>,
}

impl IndexLookup {
    pub fn new(table: String, column: String, values: Vec<Value>) -> Box<Self> {
        Box::new(Self { table, column, values })
    }
}

impl<T: Transaction> Executor<T> for IndexLookup {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_read_table(&self.table)?;

        let mut pks: HashSet<Value> = HashSet::new();
        for value in self.values {
            pks.extend(txn.read_index(&self.table, &self.column, &value)?);
        }

        // FIXME Is there a way to pass the txn into an iterator closure instead?
        let rows = pks
            .into_iter()
            .filter_map(|pk| txn.read(&table.name, &pk).transpose())
            .collect::<Result<Vec<Row>>>()?;

        Ok(ResultSet::Query {
            columns: table.columns.iter().map(|c| Column { name: Some(c.name.clone()) }).collect(),
            rows: Box::new(rows.into_iter().map(Ok)),
        })
    }
}

/// An executor that produces a single empty row
pub struct Nothing;

impl Nothing {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}

impl<T: Transaction> Executor<T> for Nothing {
    fn execute(self: Box<Self>, _: &mut T) -> Result<ResultSet> {
        Ok(ResultSet::Query {
            columns: Vec::new(),
            rows: Box::new(std::iter::once(Ok(Row::new()))),
        })
    }
}
