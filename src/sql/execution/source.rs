use super::QueryIterator;
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::types::{Column, Expression, Row, Value};

/// A table scan source.
pub(super) fn scan(
    txn: &impl Transaction,
    table: &str,
    filter: Option<Expression>,
) -> Result<QueryIterator> {
    // TODO: this should not be fallible. Pass the schema in the plan node.
    let table = txn.must_get_table(table)?;
    Ok(QueryIterator {
        columns: table.columns.into_iter().map(|c| Column { name: Some(c.name) }).collect(),
        rows: Box::new(txn.scan(&table.name, filter)?),
    })
}

/// A primary key lookup source.
pub(super) fn lookup_key(
    txn: &impl Transaction,
    table: &str,
    keys: Vec<Value>,
) -> Result<QueryIterator> {
    // TODO: move catalog lookup elsewhere and make this infallible.
    let table = txn.must_get_table(table)?;

    // TODO: don't collect into vec, requires shared txn borrow.
    let rows = keys
        .into_iter()
        .filter_map(|key| txn.get(&table.name, &key).transpose())
        .collect::<Result<Vec<Row>>>()?;

    Ok(QueryIterator {
        columns: table.columns.iter().map(|c| Column { name: Some(c.name.clone()) }).collect(),
        rows: Box::new(rows.into_iter().map(Ok)),
    })
}

/// An index lookup source.
pub(super) fn lookup_index(
    txn: &impl Transaction,
    table: &str,
    column: &str,
    values: Vec<Value>,
) -> Result<QueryIterator> {
    // TODO: pass in from planner.
    let table = txn.must_get_table(table)?;

    // TODO: consider cleaning up.
    let mut pks = std::collections::HashSet::new();
    for value in values {
        pks.extend(txn.lookup_index(&table.name, column, &value)?);
    }

    // TODO: use a shared txn borrow instead.
    let rows = pks
        .into_iter()
        .filter_map(|pk| txn.get(&table.name, &pk).transpose())
        .collect::<Result<Vec<Row>>>()?;

    Ok(QueryIterator {
        columns: table.columns.iter().map(|c| Column { name: Some(c.name.clone()) }).collect(),
        rows: Box::new(rows.into_iter().map(Ok)),
    })
}

/// Produces a single empty row. Used for queries without a FROM clause, e.g.
/// SELECT 1+1, in order to have something to project against.
pub(super) fn nothing() -> QueryIterator {
    QueryIterator { columns: Vec::new(), rows: Box::new(std::iter::once(Ok(Row::new()))) }
}
