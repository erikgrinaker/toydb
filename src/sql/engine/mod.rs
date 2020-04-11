//! The SQL engine provides fundamental CRUD storage operations.
mod kv;
mod raft;
pub use kv::KV;
pub use raft::Raft;

use super::execution::{Context, ResultSet};
use super::parser::{ast, Parser};
use super::plan::Plan;
use super::schema::Table;
use super::types::{Row, Value};
use crate::Error;

/// The SQL engine interface
pub trait Engine: Clone {
    /// The transaction type
    type Transaction: Transaction;

    /// Begins a typical read-write transaction
    fn begin(&self) -> Result<Self::Transaction, Error> {
        self.begin_with_mode(Mode::ReadWrite)
    }

    /// Begins a session, optionally resuming an active transaction with the given ID
    fn session(&self, id: Option<u64>) -> Result<Session<Self>, Error> {
        let txn = if let Some(id) = id { Some(self.resume(id)?) } else { None };
        Ok(Session { engine: self.clone(), txn })
    }

    /// Begins a transaction in the given mode
    fn begin_with_mode(&self, mode: Mode) -> Result<Self::Transaction, Error>;

    /// Resumes an active transaction with the given ID
    fn resume(&self, id: u64) -> Result<Self::Transaction, Error>;

    /// Runs a closure in a transaction
    fn with_txn<R, F>(&self, mode: Mode, f: F) -> Result<R, Error>
    where
        F: FnOnce(&mut Self::Transaction) -> Result<R, Error>,
    {
        let mut txn = self.begin_with_mode(mode)?;
        let res = f(&mut txn);
        txn.rollback()?;
        res
    }
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

/// An SQL session, which handles transaction control and simplified query execution
pub struct Session<E: Engine> {
    /// The underlying engine
    engine: E,
    /// The current session transaction, if any
    txn: Option<E::Transaction>,
}

impl<E: Engine + 'static> Session<E> {
    /// Executes a query, managing transaction status for the session
    pub fn execute(&mut self, query: &str) -> Result<ResultSet, Error> {
        // FIXME We should match on self.txn as well, but get this error:
        // error[E0009]: cannot bind by-move and by-ref in the same pattern
        // ...which seems like an arbitrary compiler limitation
        match Parser::new(query).parse()? {
            ast::Statement::Begin { .. } if self.txn.is_some() => {
                Err(Error::Value("Already in a transaction".into()))
            }
            ast::Statement::Begin { readonly: true, version: None } => {
                let txn = self.engine.begin_with_mode(Mode::ReadOnly)?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Begin { readonly: true, version: Some(version) } => {
                let txn = self.engine.begin_with_mode(Mode::Snapshot { version })?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Begin { readonly: false, version: Some(_) } => {
                Err(Error::Value("Can't start read-write transaction in a given version".into()))
            }
            ast::Statement::Begin { readonly: false, version: None } => {
                let txn = self.engine.begin_with_mode(Mode::ReadWrite)?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Commit | ast::Statement::Rollback if self.txn.is_none() => {
                Err(Error::Value("Not in a transaction".into()))
            }
            ast::Statement::Commit => {
                let txn = std::mem::replace(&mut self.txn, None).unwrap();
                let result = ResultSet::Commit { id: txn.id() };
                txn.commit()?;
                Ok(result)
            }
            ast::Statement::Rollback => {
                let txn = std::mem::replace(&mut self.txn, None).unwrap();
                let result = ResultSet::Rollback { id: txn.id() };
                txn.rollback()?;
                Ok(result)
            }
            statement if self.txn.is_some() => Plan::build(statement)?
                .optimize()?
                .execute(Context { txn: self.txn.as_mut().unwrap() }),
            statement @ ast::Statement::Select { .. } => {
                let mut txn = self.engine.begin_with_mode(Mode::ReadOnly)?;
                let result =
                    Plan::build(statement)?.optimize()?.execute(Context { txn: &mut txn })?;
                txn.commit()?;
                Ok(result)
            }
            statement => {
                let mut txn = self.engine.begin_with_mode(Mode::ReadWrite)?;
                let result =
                    Plan::build(statement)?.optimize()?.execute(Context { txn: &mut txn })?;
                txn.commit()?;
                Ok(result)
            }
        }
    }
}

/// The transaction mode
pub type Mode = crate::kv::Mode;

/// A row scan iterator
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<Row, Error>> + Send>;

/// A table scan iterator
pub type TableScan = Box<dyn DoubleEndedIterator<Item = Table> + Send>;
