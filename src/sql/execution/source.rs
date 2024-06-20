use super::QueryIterator;
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::types::{Expression, Row, Table, Value};

/// A table scan source.
pub(super) fn scan(
    txn: &impl Transaction,
    table: Table,
    filter: Option<Expression>,
) -> Result<QueryIterator> {
    Ok(QueryIterator {
        columns: table.columns.into_iter().map(|c| Some(c.name)).collect(),
        rows: Box::new(txn.scan(&table.name, filter)?),
    })
}

/// A primary key lookup source.
pub(super) fn lookup_key(
    txn: &impl Transaction,
    table: Table,
    keys: Vec<Value>,
) -> Result<QueryIterator> {
    Ok(QueryIterator {
        columns: table.columns.into_iter().map(|c| Some(c.name)).collect(),
        rows: Box::new(txn.get(&table.name, &keys)?.into_iter().map(Ok)),
    })
}

/// An index lookup source.
pub(super) fn lookup_index(
    txn: &impl Transaction,
    table: Table,
    column: String,
    values: Vec<Value>,
) -> Result<QueryIterator> {
    let pks: Vec<_> = txn.lookup_index(&table.name, &column, &values)?.into_iter().collect();
    let rows = txn.get(&table.name, &pks)?;

    Ok(QueryIterator {
        columns: table.columns.into_iter().map(|c| Some(c.name)).collect(),
        rows: Box::new(rows.into_iter().map(Ok)),
    })
}

/// Produces a single empty row. Used for queries without a FROM clause, e.g.
/// SELECT 1+1, in order to have something to project against.
pub(super) fn nothing() -> QueryIterator {
    QueryIterator { columns: Vec::new(), rows: Box::new(std::iter::once(Ok(Row::new()))) }
}
