//! The SQL engine provides fundamental CRUD storage operations.
mod kv;
mod raft;

pub use kv::KV;
pub use raft::Raft;

use super::types::schema::Table;
use super::types::{Row, Value};
use crate::Error;

/// The SQL engine interface
pub trait Engine {
    /// The transaction type
    type Transaction: Transaction;

    /// Begins a typical read-write transaction
    fn begin(&self) -> Result<Self::Transaction, Error> {
        self.begin_with_mode(Mode::ReadWrite)
    }

    /// Begins a transaction in the given mode
    fn begin_with_mode(&self, mode: Mode) -> Result<Self::Transaction, Error>;

    /// Resumes an active transaction with the given ID
    fn resume(&self, id: u64) -> Result<Self::Transaction, Error>;
}

/// An SQL transaction
pub trait Transaction {
    /// The transaction ID
    fn id(&self) -> u64;
    /// The transaction mode
    fn mode(&self) -> Mode;
    /// Commits the transaction
    fn commit(self) -> Result<(), Error>;
    /// Rolls back the transaction
    fn rollback(self) -> Result<(), Error>;

    /// Creates a new table row
    fn create(&mut self, table: &str, row: Row) -> Result<(), Error>;
    /// Deletes a table row
    fn delete(&mut self, table: &str, id: &Value) -> Result<(), Error>;
    /// Reads a table row, if it exists
    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>, Error>;
    /// Scans a table's rows
    fn scan(&self, table: &str) -> Result<Scan, Error>;
    /// Updates a table row
    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<(), Error>;

    /// Creates a new table
    fn create_table(&mut self, table: &Table) -> Result<(), Error>;
    /// Deletes an existing table, or errors if it does not exist
    fn delete_table(&mut self, table: &str) -> Result<(), Error>;
    /// Reads a table, if it exists
    fn read_table(&self, table: &str) -> Result<Option<Table>, Error>;
    /// Iterates over all tables
    fn scan_tables(&self) -> Result<TableScan, Error>;

    /// Reads a table, and errors if it does not exist
    fn must_read_table(&self, table: &str) -> Result<Table, Error> {
        self.read_table(table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))
    }
}

/// The transaction mode
pub type Mode = crate::kv::Mode;

/// A row scan iterator
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<Row, Error>> + 'static + Sync + Send>;

/// A table scan iterator
pub type TableScan = Box<dyn DoubleEndedIterator<Item = Table> + 'static + Sync + Send>;
