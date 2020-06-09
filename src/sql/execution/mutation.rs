use super::super::engine::Transaction;
use super::super::schema::Table;
use super::super::types::{Expression, Row, Value};
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

use std::collections::{HashMap, HashSet};

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

    // Builds a row from a set of column names and values, padding it with default values.
    pub fn make_row(table: &Table, columns: &[String], values: Vec<Value>) -> Result<Row> {
        if columns.len() != values.len() {
            return Err(Error::Value("Column and value counts do not match".into()));
        }
        let mut inputs = HashMap::new();
        for (c, v) in columns.iter().zip(values.into_iter()) {
            table.get_column(c)?;
            if inputs.insert(c.clone(), v).is_some() {
                return Err(Error::Value(format!("Column {} given multiple times", c)));
            }
        }
        let mut row = Row::new();
        for column in table.columns.iter() {
            if let Some(value) = inputs.get(&column.name) {
                row.push(value.clone())
            } else if let Some(value) = &column.default {
                row.push(value.clone())
            } else {
                return Err(Error::Value(format!("No value given for column {}", column.name)));
            }
        }
        Ok(row)
    }

    /// Pads a row with default values where possible.
    fn pad_row(table: &Table, mut row: Row) -> Result<Row> {
        for column in table.columns.iter().skip(row.len()) {
            if let Some(default) = &column.default {
                row.push(default.clone())
            } else {
                return Err(Error::Value(format!("No default value for column {}", column.name)));
            }
        }
        Ok(row)
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
                row = Self::pad_row(&table, row)?;
            } else {
                row = Self::make_row(&table, &self.columns, row)?;
            }
            txn.create(&table.name, row)?;
            count += 1;
        }
        Ok(ResultSet::Create { count })
    }
}

/// An UPDATE executor
pub struct Update<T: Transaction> {
    table: String,
    source: Box<dyn Executor<T>>,
    expressions: Vec<(usize, Expression)>,
}

impl<T: Transaction> Update<T> {
    pub fn new(
        table: String,
        source: Box<dyn Executor<T>>,
        expressions: Vec<(usize, Expression)>,
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
                        new[*field] = expr.evaluate(Some(&row))?;
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
