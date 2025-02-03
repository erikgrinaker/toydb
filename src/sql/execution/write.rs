use std::collections::{BTreeMap, HashMap};

use itertools::Itertools as _;

use crate::errinput;
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::types::{Expression, Rows, Table, Value};

/// Deletes rows, taking primary keys from the source (i.e. DELETE) using the
/// primary_key column index. Returns the number of rows deleted.
pub fn delete(
    txn: &impl Transaction,
    table: String,
    primary_key: usize,
    source: Rows,
) -> Result<u64> {
    let ids: Vec<Value> =
        source.map_ok(|row| row.into_iter().nth(primary_key).expect("short row")).try_collect()?;
    let count = ids.len() as u64;
    txn.delete(&table, &ids)?;
    Ok(count)
}

/// Inserts rows into a table (i.e. INSERT) from the given source.
///
/// If given, column_map contains the mapping of table -> source columns for all
/// columns in source. Otherwise, every column in source is the corresponding
/// column in table, but the source may not have all columns in table (there may
/// be a missing tail).
pub fn insert(
    txn: &impl Transaction,
    table: Table,
    column_map: Option<HashMap<usize, usize>>,
    mut source: Rows,
) -> Result<u64> {
    let mut rows = Vec::new();
    while let Some(values) = source.next().transpose()? {
        // Fast path: the row is already complete, with no column mapping.
        if values.len() == table.columns.len() && column_map.is_none() {
            rows.push(values);
            continue;
        }
        if values.len() > table.columns.len() {
            return errinput!("too many values for table {}", table.name);
        }
        if let Some(column_map) = &column_map {
            if column_map.len() != values.len() {
                return errinput!("column and value counts do not match");
            }
        }

        // Map source columns to table columns, and fill in default values.
        let mut row = Vec::with_capacity(table.columns.len());
        for (i, column) in table.columns.iter().enumerate() {
            if column_map.is_none() && i < values.len() {
                // Pass through the source column to the table column.
                row.push(values[i].clone())
            } else if let Some(vi) = column_map.as_ref().and_then(|c| c.get(&i)).copied() {
                // Map the source column to the table column.
                row.push(values[vi].clone())
            } else if let Some(default) = &column.default {
                // Column not given in source, use the default.
                row.push(default.clone())
            } else {
                return errinput!("no value given for column {} with no default", column.name);
            }
        }
        rows.push(row);
    }
    let count = rows.len() as u64;
    txn.insert(&table.name, rows)?;
    Ok(count)
}

/// Updates rows passed in from the source (i.e. UPDATE). Returns the number of
/// rows updated.
pub fn update(
    txn: &impl Transaction,
    table: String,
    primary_key: usize,
    mut source: Rows,
    expressions: Vec<(usize, Expression)>,
) -> Result<u64> {
    let mut updates = BTreeMap::new();
    while let Some(row) = source.next().transpose()? {
        let mut update = row.clone();
        for (column, expr) in &expressions {
            update[*column] = expr.evaluate(Some(&row))?;
        }
        let id = row.into_iter().nth(primary_key).expect("short row");
        updates.insert(id, update);
    }
    let count = updates.len() as u64;
    txn.update(&table, updates)?;
    Ok(count)
}
