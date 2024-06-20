use super::QueryIterator;
use crate::errinput;
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::types::{Expression, Row, Table, Value};

/// Deletes rows, taking primary keys from the source (i.e. DELETE).
/// Returns the number of rows deleted.
pub(super) fn delete(
    txn: &impl Transaction,
    table: String,
    key_index: usize,
    source: QueryIterator,
) -> Result<u64> {
    let ids: Vec<_> = source.map(|r| r.map(|row| row[key_index].clone())).collect::<Result<_>>()?;
    let count = ids.len() as u64;
    txn.delete(&table, &ids)?;
    Ok(count)
}

/// Inserts rows into a table (i.e. INSERT).
///
/// TODO: this should take rows from a values source.
pub(super) fn insert(
    txn: &impl Transaction,
    table: Table,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
) -> Result<u64> {
    let mut rows = Vec::with_capacity(values.len());
    for expressions in values {
        let mut row =
            expressions.into_iter().map(|expr| expr.evaluate(None)).collect::<Result<_>>()?;
        if columns.is_empty() {
            row = pad_row(&table, row)?;
        } else {
            row = make_row(&table, &columns, row)?;
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
    key_index: usize,
    mut source: QueryIterator,
    expressions: Vec<(usize, Expression)>,
) -> Result<u64> {
    let mut update = std::collections::HashMap::new();
    while let Some(row) = source.next().transpose()? {
        let id = row[key_index].clone();
        let mut new = row.clone();
        for (field, expr) in &expressions {
            new[*field] = expr.evaluate(Some(&row))?;
        }
        update.insert(id, new);
    }
    let count = update.len() as u64;
    txn.update(&table, update)?;
    Ok(count)
}

// Builds a row from a set of column names and values, padding it with default values.
pub fn make_row(table: &Table, columns: &[String], values: Vec<Value>) -> Result<Row> {
    if columns.len() != values.len() {
        return errinput!("column and value counts do not match");
    }
    let mut inputs = std::collections::HashMap::new();
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
