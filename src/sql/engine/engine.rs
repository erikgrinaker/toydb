use std::collections::{BTreeMap, BTreeSet};

use super::Session;
use crate::errinput;
use crate::error::Result;
use crate::sql::types::{Expression, Row, Rows, Table, Value};
use crate::storage::mvcc;

/// A SQL engine. This provides low-level CRUD (create, read, update, delete)
/// operations for table rows, a schema catalog for accessing and modifying
/// table schemas, and interactive SQL sessions that execute client SQL
/// statements. All engine access is transactional with snapshot isolation.
pub trait Engine<'a>: Sized {
    /// The engine's transaction type. This provides both row-level CRUD operations and
    /// transactional access to the schema catalog.
    type Transaction: Transaction + 'a;

    /// Begins a read-write transaction.
    fn begin(&'a self) -> Result<Self::Transaction>;
    /// Begins a read-only transaction.
    fn begin_read_only(&'a self) -> Result<Self::Transaction>;
    /// Begins a read-only transaction as of a historical version.
    fn begin_as_of(&'a self, version: mvcc::Version) -> Result<Self::Transaction>;

    /// Creates a client session for executing SQL statements.
    fn session(&'a self) -> Session<'a, Self> {
        Session::new(self)
    }
}

/// A SQL transaction. Executes transactional CRUD operations on table rows.
/// Provides snapshot isolation (see `storage::mvcc` module for details).
///
/// All methods operate on row batches rather than single rows to amortize the
/// cost. With the Raft engine, each call results in a Raft roundtrip, and we'd
/// rather not have to do that for every single row that's modified.
pub trait Transaction: Catalog {
    /// The transaction's internal MVCC state.
    fn state(&self) -> &mvcc::TransactionState;

    /// Commits the transaction.
    fn commit(self) -> Result<()>;
    /// Rolls back the transaction.
    fn rollback(self) -> Result<()>;

    /// Deletes table rows by primary key, if they exist.
    fn delete(&self, table: &str, ids: &[Value]) -> Result<()>;
    /// Fetches table rows by primary key, if they exist.
    fn get(&self, table: &str, ids: &[Value]) -> Result<Vec<Row>>;
    /// Inserts new table rows.
    fn insert(&self, table: &str, rows: Vec<Row>) -> Result<()>;
    /// Looks up a set of primary keys by index values. BTreeSet for testing.
    fn lookup_index(&self, table: &str, column: &str, values: &[Value]) -> Result<BTreeSet<Value>>;
    /// Scans a table's rows, optionally applying the given filter.
    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Rows>;
    /// Updates table rows by primary key. BTreeMap for testing.
    fn update(&self, table: &str, rows: BTreeMap<Value, Row>) -> Result<()>;
}

/// The catalog stores table schema information. It must be implemented for
/// Transaction, and is thus fully transactional. For simplicity, it only
/// supports creating and dropping tables -- there are no ALTER TABLE schema
/// changes, nor CREATE INDEX.
pub trait Catalog {
    /// Creates a new table. Errors if it already exists.
    fn create_table(&self, table: Table) -> Result<()>;
    /// Drops a table. Errors if it does not exist, unless if_exists is true.
    /// Returns true if the table existed and was deleted.
    fn drop_table(&self, table: &str, if_exists: bool) -> Result<bool>;
    /// Fetches a table schema, or None if it doesn't exist.
    fn get_table(&self, table: &str) -> Result<Option<Table>>;
    /// Returns a list of all table schemas.
    fn list_tables(&self) -> Result<Vec<Table>>;

    /// Fetches a table schema, or errors if it does not exist.
    fn must_get_table(&self, table: &str) -> Result<Table> {
        self.get_table(table)?.ok_or_else(|| errinput!("table {table} does not exist"))
    }
}
