use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::types::{Expression, Row, Rows, Table, Value};

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

/// Produces a single empty row. Used for queries without a FROM clause, e.g.
/// SELECT 1+1, in order to have something to project against.
/// TODO: should actually return nothing.
pub(super) fn nothing() -> Rows {
    Box::new(std::iter::once(Ok(Row::new())))
}

/// Emits predefined constant values.
pub(super) fn values(rows: Vec<Vec<Expression>>) -> Rows {
    Box::new(rows.into_iter().map(|row| row.into_iter().map(|expr| expr.evaluate(None)).collect()))
}
