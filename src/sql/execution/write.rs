use super::QueryIterator;
use crate::error::{Error, Result};
use crate::sql::engine::Transaction;
use crate::sql::types::{Expression, Table};
use crate::{errdata, errinput};

use std::collections::HashMap;

/// Deletes rows, taking primary keys from the source (i.e. DELETE) using the
/// primary_key field index. Returns the number of rows deleted.
pub(super) fn delete(
    txn: &impl Transaction,
    table: String,
    primary_key: usize,
    source: QueryIterator,
) -> Result<u64> {
    let ids = source
        // TODO: consider moving this out to a QueryIterator helper.
        .map(|r| r.and_then(|row| row.into_iter().nth(primary_key).ok_or(errdata!("short row"))))
        .collect::<Result<Vec<_>>>()?;
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
pub(super) fn insert(
    txn: &impl Transaction,
    table: Table,
    column_map: Option<HashMap<usize, usize>>,
    mut source: QueryIterator,
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

        // Fill in the row with default values for missing fields, and map
        // source fields to table fields.
        let mut row = Vec::with_capacity(table.columns.len());
        for (cidx, column) in table.columns.iter().enumerate() {
            if column_map.is_none() && cidx < values.len() {
                // Pass through the source column to the table column.
                row.push(values[cidx].clone())
            } else if let Some(vidx) = column_map.as_ref().and_then(|c| c.get(&cidx)).copied() {
                // Map the source column to the table column.
                row.push(values[vidx].clone())
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
pub(super) fn update(
    txn: &impl Transaction,
    table: String,
    primary_key: usize,
    mut source: QueryIterator,
    expressions: Vec<(usize, Expression)>,
) -> Result<u64> {
    let mut updates = HashMap::new();
    while let Some(row) = source.next().transpose()? {
        let mut new = row.clone();
        for (field, expr) in &expressions {
            new[*field] = expr.evaluate(Some(&row))?;
        }
        let id = row.into_iter().nth(primary_key).ok_or::<Error>(errdata!("short row"))?;
        updates.insert(id, new);
    }
    let count = updates.len() as u64;
    txn.update(&table, updates)?;
    Ok(count)
}
