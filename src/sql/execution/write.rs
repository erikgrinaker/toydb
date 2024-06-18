use super::QueryIterator;
use crate::errinput;
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::types::schema::Table;
use crate::sql::types::{Expression, Row, Value};

/// Deletes rows, taking primary keys from the source (i.e. DELETE).
/// Returns the number of rows deleted.
pub(super) fn delete(
    txn: &impl Transaction,
    table: &str,
    mut source: QueryIterator,
) -> Result<u64> {
    // TODO: should be prepared by planner.
    let table = txn.must_get_table(table)?;
    let mut count = 0;
    while let Some(row) = source.next().transpose()? {
        txn.delete(&table.name, &table.get_row_key(&row)?)?;
        count += 1
    }
    Ok(count)
}

/// Inserts rows into a table (i.e. INSERT).
///
/// TODO: this should take rows from a values source.
pub(super) fn insert(
    txn: &impl Transaction,
    table: &str,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
) -> Result<u64> {
    let table = txn.must_get_table(table)?;
    let mut count = 0;
    for expressions in values {
        let mut row =
            expressions.into_iter().map(|expr| expr.evaluate(None)).collect::<Result<_>>()?;
        if columns.is_empty() {
            row = pad_row(&table, row)?;
        } else {
            row = make_row(&table, &columns, row)?;
        }
        txn.insert(&table.name, row)?;
        count += 1;
    }
    Ok(count)
}

/// Updates rows passed in from the source (i.e. UPDATE). Returns the number of
/// rows updated.
pub(super) fn update(
    txn: &impl Transaction,
    table: &str,
    mut source: QueryIterator,
    expressions: Vec<(usize, Expression)>,
) -> Result<u64> {
    let table = txn.must_get_table(table)?;
    // The iterator will see our changes, such that the same item may be
    // iterated over multiple times. We keep track of the primary keys here
    // to avoid that, althought it may cause ballooning memory usage for
    // large updates.
    //
    // FIXME This is not safe for primary key updates, which may still be
    // processed multiple times - it should be possible to come up with a
    // pathological case that loops forever (e.g. UPDATE test SET id = id +
    // 1).
    let mut updated = std::collections::HashSet::new();
    while let Some(row) = source.next().transpose()? {
        let id = table.get_row_key(&row)?;
        if updated.contains(&id) {
            continue;
        }
        let mut new = row.clone();
        for (field, expr) in &expressions {
            new[*field] = expr.evaluate(Some(&row))?;
        }
        txn.update(&table.name, &id, new)?;
        updated.insert(id);
    }
    Ok(updated.len() as u64)
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
