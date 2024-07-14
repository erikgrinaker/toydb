use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::types::{Expression, Rows, Table, Value};

/// A table scan source.
pub(super) fn scan(
    txn: &impl Transaction,
    table: Table,
    filter: Option<Expression>,
) -> Result<Rows> {
    Ok(Box::new(txn.scan(&table.name, filter)?))
}

/// A primary key lookup source.
pub(super) fn lookup_key(txn: &impl Transaction, table: String, keys: Vec<Value>) -> Result<Rows> {
    Ok(Box::new(txn.get(&table, &keys)?.into_iter().map(Ok)))
}

/// An index lookup source.
pub(super) fn lookup_index(
    txn: &impl Transaction,
    table: String,
    column: String,
    values: Vec<Value>,
) -> Result<Rows> {
    let ids: Vec<_> = txn.lookup_index(&table, &column, &values)?.into_iter().collect();
    Ok(Box::new(txn.get(&table, &ids)?.into_iter().map(Ok)))
}

/// Returns nothing. Used to short-circuit nodes that can't produce any rows.
pub(super) fn nothing() -> Rows {
    Box::new(std::iter::empty())
}

/// Emits predefined constant values.
pub(super) fn values(rows: Vec<Vec<Expression>>) -> Rows {
    Box::new(rows.into_iter().map(|row| row.into_iter().map(|expr| expr.evaluate(None)).collect()))
}
