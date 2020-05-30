//! The SQL engine provides fundamental CRUD storage operations.
mod kv;
pub mod raft;
pub use kv::KV;
pub use raft::{Raft, Status};

use super::execution::{Context, ResultSet};
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

    /// Begins a transaction in the given mode
    fn begin(&self, mode: Mode) -> Result<Self::Transaction>;

    /// Begins a session for executing individual statements
    fn session(&self) -> Result<Session<Self>> {
        Ok(Session { engine: self.clone(), txn: None })
    }

    /// Resumes an active transaction with the given ID
    fn resume(&self, id: u64) -> Result<Self::Transaction>;
}

/// An SQL transaction
pub trait Transaction: Catalog {
    /// The transaction ID
    fn id(&self) -> u64;
    /// The transaction mode
    fn mode(&self) -> Mode;
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
pub struct Session<E: Engine> {
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
            ast::Statement::Begin { readonly: true, version: None } => {
                let txn = self.engine.begin(Mode::ReadOnly)?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Begin { readonly: true, version: Some(version) } => {
                let txn = self.engine.begin(Mode::Snapshot { version })?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Begin { readonly: false, version: Some(_) } => {
                Err(Error::Value("Can't start read-write transaction in a given version".into()))
            }
            ast::Statement::Begin { readonly: false, version: None } => {
                let txn = self.engine.begin(Mode::ReadWrite)?;
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
            ast::Statement::Explain(statement) => self.with_txn(Mode::ReadOnly, |txn| {
                Ok(ResultSet::Explain(Plan::build(*statement, txn)?.optimize(txn)?.0))
            }),
            statement if self.txn.is_some() => Plan::build(statement, self.txn.as_mut().unwrap())?
                .optimize(self.txn.as_mut().unwrap())?
                .execute(Context { txn: self.txn.as_mut().unwrap() }),
            statement @ ast::Statement::Select { .. } => {
                let mut txn = self.engine.begin(Mode::ReadOnly)?;
                let result = Plan::build(statement, &mut txn)?
                    .optimize(&mut txn)?
                    .execute(Context { txn: &mut txn });
                txn.rollback()?;
                result
            }
            statement => {
                let mut txn = self.engine.begin(Mode::ReadWrite)?;
                match Plan::build(statement, &mut txn)?
                    .optimize(&mut txn)?
                    .execute(Context { txn: &mut txn })
                {
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

    /// Runs a closure in the session's transaction, or a new transaction if none is active.
    pub fn with_txn<R, F>(&mut self, mode: Mode, f: F) -> Result<R>
    where
        F: FnOnce(&mut E::Transaction) -> Result<R>,
    {
        if let Some(ref mut txn) = self.txn {
            if !txn.mode().satisfies(&mode) {
                return Err(Error::Value(
                    "The operation cannot run in the current transaction".into(),
                ));
            }
            return f(txn);
        }
        let mut txn = self.engine.begin(mode)?;
        let result = f(&mut txn);
        txn.rollback()?;
        result
    }
}

/// The transaction mode
pub type Mode = crate::storage::kv::mvcc::Mode;

/// A row scan iterator
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;

/// An index scan iterator
pub type IndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Value, HashSet<Value>)>> + Send>;
