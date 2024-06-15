use super::super::engine::Transaction;
use super::super::types::schema::Table;
use super::super::types::{Expression, Row, Value};
use super::QueryIterator;
use crate::errinput;
use crate::error::Result;

use std::collections::{HashMap, HashSet};

/// An INSERT executor
pub struct Insert {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table: String, columns: Vec<String>, rows: Vec<Vec<Expression>>) -> Self {
        Self { table, columns, rows }
    }

    pub fn execute(self, txn: &mut impl Transaction) -> Result<u64> {
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
        Ok(count)
    }

    // Builds a row from a set of column names and values, padding it with default values.
    pub fn make_row(table: &Table, columns: &[String], values: Vec<Value>) -> Result<Row> {
        if columns.len() != values.len() {
            return errinput!("column and value counts do not match");
        }
        let mut inputs = HashMap::new();
        for (c, v) in columns.iter().zip(values.into_iter()) {
            table.get_column(c)?;
            if inputs.insert(c.clone(), v).is_some() {
                return errinput!("column {c} given multiple times");
            }
        }
        let mut row = Row::new();
        for column in table.columns.iter() {
            if let Some(value) = inputs.get(&column.name) {
                row.push(value.clone())
            } else if let Some(value) = &column.default {
                row.push(value.clone())
            } else {
                return errinput!("no value given for column {}", column.name);
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
                return errinput!("no default value for column {}", column.name);
            }
        }
        Ok(row)
    }
}

/// An UPDATE executor
pub struct Update {
    table: String,
    source: QueryIterator,
    expressions: Vec<(usize, Expression)>,
}

impl Update {
    pub fn new(
        table: String,
        source: QueryIterator,
        expressions: Vec<(usize, Expression)>,
    ) -> Self {
        Self { table, source, expressions }
    }

    pub fn execute(mut self, txn: &mut impl Transaction) -> Result<u64> {
        let table = txn.must_read_table(&self.table)?;
        // The iterator will see our changes, such that the same item may be
        // iterated over multiple times. We keep track of the primary keys here
        // to avoid that, althought it may cause ballooning memory usage for
        // large updates.
        //
        // FIXME This is not safe for primary key updates, which may still be
        // processed multiple times - it should be possible to come up with a
        // pathological case that loops forever (e.g. UPDATE test SET id = id +
        // 1).
        let mut updated = HashSet::new();
        while let Some(row) = self.source.next().transpose()? {
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
        Ok(updated.len() as u64)
    }
}

/// A DELETE executor
pub struct Delete {
    table: String,
    source: QueryIterator,
}

impl Delete {
    pub fn new(table: String, source: QueryIterator) -> Self {
        Self { table, source }
    }

    pub fn execute(mut self, txn: &mut impl Transaction) -> Result<u64> {
        let table = txn.must_read_table(&self.table)?;
        let mut count = 0;
        while let Some(row) = self.source.next().transpose()? {
            txn.delete(&table.name, &table.get_row_key(&row)?)?;
            count += 1
        }
        Ok(count)
    }
}
