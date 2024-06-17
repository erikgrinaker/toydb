#![allow(clippy::module_inception)]

use super::Session;
use crate::errinput;
use crate::error::Result;
use crate::sql::types::schema::Table;
use crate::sql::types::{Expression, Row, Value};

use std::collections::HashSet;

/// The SQL engine interface
pub trait Engine: Clone {
    /// The transaction type
    type Transaction: Transaction;

    /// Begins a read-write transaction.
    fn begin(&self) -> Result<Self::Transaction>;

    /// Begins a read-only transaction.
    fn begin_read_only(&self) -> Result<Self::Transaction>;

    /// Begins a read-only transaction as of a historical version.
    fn begin_as_of(&self, version: u64) -> Result<Self::Transaction>;

    /// Begins a session for executing individual statements
    fn session(&self) -> Session<Self> {
        Session::new(self.clone())
    }
}

/// A SQL transaction.
///
/// TODO: split out Catalog trait and don't have Transaction depend on it. This
/// enforces cleaner separation of when catalog access is valid (i.e. during
/// planning but not execution).
pub trait Transaction: Catalog {
    /// The transaction's version
    fn version(&self) -> u64;
    /// Whether the transaction is read-only
    fn read_only(&self) -> bool;

    /// Commits the transaction
    fn commit(self) -> Result<()>;
    /// Rolls back the transaction
    fn rollback(self) -> Result<()>;

    /// Creates a new table row
    fn create(&mut self, table: &str, row: Row) -> Result<()>;
    /// Deletes a table row
    fn delete(&mut self, table: &str, id: &Value) -> Result<()>;
    /// Reads a table row, if it exists
    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>>;
    /// Reads an index entry, if it exists
    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>>;
    /// Scans a table's rows
    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Scan>;
    /// Scans a column's index entries
    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan>;
    /// Updates a table row
    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()>;
}

/// A row scan iterator
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;

/// An index scan iterator
pub type IndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Value, HashSet<Value>)>> + Send>;

/// The catalog stores schema information
pub trait Catalog {
    /// Creates a new table
    fn create_table(&mut self, table: Table) -> Result<()>;
    /// Deletes an existing table, or errors if it does not exist
    fn delete_table(&mut self, table: &str) -> Result<()>;
    /// Reads a table, if it exists
    fn read_table(&self, table: &str) -> Result<Option<Table>>;
    /// Lists tables.
    fn list_tables(&self) -> Result<Vec<Table>>;

    /// Reads a table, and errors if it does not exist
    fn must_read_table(&self, table: &str) -> Result<Table> {
        self.read_table(table)?.ok_or(errinput!("table {table} does not exist"))
    }

    /// Returns all references to a table, as table,column pairs.
    fn table_references(&self, table: &str, with_self: bool) -> Result<Vec<(String, Vec<String>)>> {
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
