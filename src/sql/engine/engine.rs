#![allow(clippy::module_inception)]

use super::Session;
use crate::errinput;
use crate::error::Result;
use crate::sql::types::schema::Table;
use crate::sql::types::{Expression, Row, Rows, Value};
use crate::storage::mvcc;

use std::collections::{HashMap, HashSet};

/// The SQL engine interface.
pub trait Engine<'a>: Sized {
    /// The transaction type.
    type Transaction: Transaction + 'a;

    /// Begins a read-write transaction.
    fn begin(&self) -> Result<Self::Transaction>;
    /// Begins a read-only transaction.
    fn begin_read_only(&self) -> Result<Self::Transaction>;
    /// Begins a read-only transaction as of a historical version.
    fn begin_as_of(&self, version: mvcc::Version) -> Result<Self::Transaction>;

    /// Creates a client session for executing SQL statements.
    fn session(&'a self) -> Session<'a, Self> {
        Session::new(self)
    }
}

/// A SQL transaction.
///
/// All methods operate on row batches rather than single rows to amortize the
/// cost. We have to do a Raft roundtrip for every call, and we'd rather not
/// have to do a Raft roundtrip for every row.
///
/// TODO: split out Catalog trait and don't have Transaction depend on it. This
/// enforces cleaner separation of when catalog access is valid (i.e. during
/// planning but not execution).
pub trait Transaction: Catalog {
    /// The transaction's MVCC version.
    fn version(&self) -> mvcc::Version;
    /// Whether the transaction is read-only.
    fn read_only(&self) -> bool;

    /// Commits the transaction.
    fn commit(self) -> Result<()>;
    /// Rolls back the transaction.
    fn rollback(self) -> Result<()>;

    /// Deletes table rows by primary key.
    fn delete(&self, table: &str, ids: &[Value]) -> Result<()>;
    /// Fetches table rows by primary key.
    fn get(&self, table: &str, ids: &[Value]) -> Result<Vec<Row>>;
    /// Inserts new table rows.
    fn insert(&self, table: &str, rows: Vec<Row>) -> Result<()>;
    /// Looks up a set of table primary keys by an index value.
    /// TODO: should this just return a Vec instead?
    fn lookup_index(&self, table: &str, column: &str, values: &[Value]) -> Result<HashSet<Value>>;
    /// Scans a table's rows, optionally applying the given filter.
    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Rows>;
    /// Scans a column's index entries.
    /// TODO: this is only used for tests. Remove it?
    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan>;
    /// Updates table rows by primary key.
    fn update(&self, table: &str, rows: HashMap<Value, Row>) -> Result<()>;
}

/// An index scan iterator.
pub type IndexScan = Box<dyn Iterator<Item = Result<(Value, HashSet<Value>)>>>;

/// The catalog stores schema information
pub trait Catalog {
    /// Creates a new table.
    fn create_table(&self, table: Table) -> Result<()>;
    /// Drops a table. Errors if it does not exist.
    ///
    /// TODO: consider taking an if_exists parameter, but that will incur a Raft
    /// roundtrip.
    fn drop_table(&self, table: &str) -> Result<()>;
    /// Fetches a table schema.
    fn get_table(&self, table: &str) -> Result<Option<Table>>;
    /// Lists tables.
    fn list_tables(&self) -> Result<Vec<Table>>;

    /// Reads a table, errors if it does not exist.
    fn must_get_table(&self, table: &str) -> Result<Table> {
        self.get_table(table)?.ok_or(errinput!("table {table} does not exist"))
    }

    /// Returns all references to a table, as table,column pairs.
    /// TODO: make this actually be table,column, instead of a column vec.
    fn references(&self, table: &str, with_self: bool) -> Result<Vec<(String, Vec<String>)>> {
        Ok(self
            .list_tables()?
            .into_iter()
            .filter(|t| with_self || t.name != table)
            .map(|t| {
                (
                    t.name,
                    t.columns
                        .iter()
                        .filter(|c| c.references.as_deref() == Some(table))
                        .map(|c| c.name.clone())
                        .collect::<Vec<_>>(),
                )
            })
            .filter(|(_, cs)| !cs.is_empty())
            .collect())
    }
}
