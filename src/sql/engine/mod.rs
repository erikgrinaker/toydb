//! The SQL engine provides fundamental CRUD storage operations.
mod kv;
pub mod raft;
pub use kv::KV;
pub use raft::{Raft, Status};

use super::execution::ResultSet;
use super::parser::{ast, Parser};
use super::plan::Plan;
use super::schema::Catalog;
use super::types::{Expression, Row, Value};
use crate::error::{Error, Result};

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
        Session { engine: self.clone(), txn: None }
    }
}

/// An SQL transaction
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

/// An SQL session, which handles transaction control and simplified query execution
pub struct Session<E: Engine + 'static> {
    /// The underlying engine
    engine: E,
    /// The current session transaction, if any
    txn: Option<E::Transaction>,
}

impl<E: Engine + 'static> Session<E> {
    /// Executes a query, managing transaction status for the session
    pub fn execute(&mut self, query: &str) -> Result<ResultSet> {
        // FIXME We should match on self.txn as well, but get this error:
        // error[E0009]: cannot bind by-move and by-ref in the same pattern
        // ...which seems like an arbitrary compiler limitation
        match Parser::new(query).parse()? {
            ast::Statement::Begin { .. } if self.txn.is_some() => {
                Err(Error::Value("Already in a transaction".into()))
            }
            ast::Statement::Begin { read_only: true, as_of: None } => {
                let txn = self.engine.begin_read_only()?;
                let result = ResultSet::Begin { version: txn.version(), read_only: true };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Begin { read_only: true, as_of: Some(version) } => {
                let txn = self.engine.begin_as_of(version)?;
                let result = ResultSet::Begin { version, read_only: true };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Begin { read_only: false, as_of: Some(_) } => {
                Err(Error::Value("Can't start read-write transaction in a given version".into()))
            }
            ast::Statement::Begin { read_only: false, as_of: None } => {
                let txn = self.engine.begin()?;
                let result = ResultSet::Begin { version: txn.version(), read_only: false };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Commit | ast::Statement::Rollback if self.txn.is_none() => {
                Err(Error::Value("Not in a transaction".into()))
            }
            ast::Statement::Commit => {
                let txn = self.txn.take().unwrap();
                let version = txn.version();
                txn.commit()?;
                Ok(ResultSet::Commit { version })
            }
            ast::Statement::Rollback => {
                let txn = self.txn.take().unwrap();
                let version = txn.version();
                txn.rollback()?;
                Ok(ResultSet::Rollback { version })
            }
            ast::Statement::Explain(statement) => self.with_txn_read_only(|txn| {
                Ok(ResultSet::Explain(Plan::build(*statement, txn)?.optimize(txn)?.0))
            }),
            statement if self.txn.is_some() => Plan::build(statement, self.txn.as_mut().unwrap())?
                .optimize(self.txn.as_mut().unwrap())?
                .execute(self.txn.as_mut().unwrap()),
            statement @ ast::Statement::Select { .. } => {
                let mut txn = self.engine.begin_read_only()?;
                let result =
                    Plan::build(statement, &mut txn)?.optimize(&mut txn)?.execute(&mut txn);
                txn.rollback()?;
                result
            }
            statement => {
                let mut txn = self.engine.begin()?;
                match Plan::build(statement, &mut txn)?.optimize(&mut txn)?.execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    }
                    Err(error) => {
                        txn.rollback()?;
                        Err(error)
                    }
                }
            }
        }
    }

    /// Runs a read-only closure in the session's transaction, or a new
    /// read-only transaction if none is active.
    ///
    /// TODO: reconsider this.
    pub fn with_txn_read_only<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut E::Transaction) -> Result<R>,
    {
        if let Some(ref mut txn) = self.txn {
            return f(txn);
        }
        let mut txn = self.engine.begin_read_only()?;
        let result = f(&mut txn);
        txn.rollback()?;
        result
    }
}

impl Session<Raft> {
    pub fn status(&self) -> Result<Status> {
        self.engine.status()
    }
}

impl<E: Engine + 'static> Drop for Session<E> {
    fn drop(&mut self) {
        self.execute("ROLLBACK").ok();
    }
}

/// A row scan iterator
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;

/// An index scan iterator
pub type IndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Value, HashSet<Value>)>> + Send>;
