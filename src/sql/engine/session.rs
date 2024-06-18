use super::raft::{Raft, Status};
use super::{Engine, Transaction as _};
use crate::error::{Error, Result};
use crate::sql::execution::ExecutionResult;
use crate::sql::parser::{ast, Parser};
use crate::sql::plan::Plan;
use crate::sql::types::{Columns, Row, Rows, Value};
use crate::{errdata, errinput};

use serde::{Deserialize, Serialize};

/// An SQL session, which handles transaction control and simplified query execution
pub struct Session<'a, E: Engine<'a>> {
    /// The underlying engine
    engine: &'a E,
    /// The current session transaction, if any
    txn: Option<E::Transaction>,
}

impl<'a, E: Engine<'a>> Session<'a, E> {
    pub fn new(engine: &'a E) -> Self {
        Self { engine, txn: None }
    }

    /// Executes a query, managing transaction status for the session
    pub fn execute(&mut self, query: &str) -> Result<StatementResult> {
        // FIXME We should match on self.txn as well, but get this error:
        // error[E0009]: cannot bind by-move and by-ref in the same pattern
        // ...which seems like an arbitrary compiler limitation
        match Parser::new(query).parse()? {
            ast::Statement::Begin { .. } if self.txn.is_some() => {
                errinput!("already in a transaction")
            }
            ast::Statement::Begin { read_only: true, as_of: None } => {
                let txn = self.engine.begin_read_only()?;
                let result = StatementResult::Begin { version: txn.version(), read_only: true };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Begin { read_only: true, as_of: Some(version) } => {
                let txn = self.engine.begin_as_of(version)?;
                let result = StatementResult::Begin { version, read_only: true };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Begin { read_only: false, as_of: Some(_) } => {
                errinput!("can't start read-write transaction in a given version")
            }
            ast::Statement::Begin { read_only: false, as_of: None } => {
                let txn = self.engine.begin()?;
                let result = StatementResult::Begin { version: txn.version(), read_only: false };
                self.txn = Some(txn);
                Ok(result)
            }
            ast::Statement::Commit | ast::Statement::Rollback if self.txn.is_none() => {
                errinput!("not in a transaction")
            }
            ast::Statement::Commit => {
                let txn = self.txn.take().unwrap();
                let version = txn.version();
                txn.commit()?;
                Ok(StatementResult::Commit { version })
            }
            ast::Statement::Rollback => {
                let txn = self.txn.take().unwrap();
                let version = txn.version();
                txn.rollback()?;
                Ok(StatementResult::Rollback { version })
            }
            // TODO: this needs testing.
            ast::Statement::Explain(statement) => self.with_txn_read_only(|txn| {
                Ok(StatementResult::Explain(Plan::build(*statement, txn)?.optimize()?))
            }),
            statement if self.txn.is_some() => {
                Self::execute_with(statement, self.txn.as_mut().unwrap())
            }
            statement @ ast::Statement::Select { .. } => {
                let mut txn = self.engine.begin_read_only()?;
                let result = Self::execute_with(statement, &mut txn);
                txn.rollback()?;
                result
            }
            statement => {
                let mut txn = self.engine.begin()?;
                let result = Self::execute_with(statement, &mut txn);
                match &result {
                    Ok(_) => txn.commit()?,
                    Err(_) => txn.rollback()?,
                }
                result
            }
        }
    }

    /// Helper function to execute a statement with the given transaction.
    /// Allows using a mutable borrow either to the session's transaction
    /// or a temporary read-only or read/write transaction.
    ///
    /// TODO: reconsider this.
    fn execute_with(
        statement: ast::Statement,
        txn: &mut E::Transaction,
    ) -> Result<StatementResult> {
        Plan::build(statement, txn)?.optimize()?.execute(txn)?.try_into()
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

impl<'a> Session<'a, Raft> {
    pub fn status(&self) -> Result<Status> {
        self.engine.status()
    }
}

impl<'a, E: Engine<'a>> Drop for Session<'a, E> {
    fn drop(&mut self) {
        self.execute("ROLLBACK").ok();
    }
}

/// A session statement result. This is also sent across the wire to SQL
/// clients.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum StatementResult {
    // Transaction started
    Begin { version: u64, read_only: bool },
    // Transaction committed
    Commit { version: u64 },
    // Transaction rolled back
    Rollback { version: u64 },
    // Rows created
    Create { count: u64 },
    // Rows deleted
    Delete { count: u64 },
    // Rows updated
    Update { count: u64 },
    // Table created
    CreateTable { name: String },
    // Table dropped
    DropTable { name: String, existed: bool },
    // Query result.
    //
    // For simplicity, buffer and send the entire result as a vector instead of
    // streaming it to the client. Streaming reads haven't been implemented from
    // Raft either.
    Query { columns: Columns, rows: Vec<Row> },
    // Explain result
    Explain(Plan),
}

impl StatementResult {
    /// Converts the ResultSet into a row, or errors if not a query result with rows.
    pub fn into_row(self) -> Result<Row> {
        self.into_rows()?.next().transpose()?.ok_or(errdata!("no rows returned"))
    }

    /// Converts the ResultSet into a row iterator, or errors if not a query
    /// result with rows.
    pub fn into_rows(self) -> Result<Rows> {
        if let StatementResult::Query { rows, .. } = self {
            Ok(Box::new(rows.into_iter().map(Ok)))
        } else {
            errdata!("not a query result: {self:?}")
        }
    }

    /// Converts the ResultSet into a value, if possible.
    /// TODO: use TryFrom for this, also to primitive types via Value as TryFrom.
    pub fn into_value(self) -> Result<Value> {
        self.into_row()?.into_iter().next().ok_or(errdata!("no value returned"))
    }
}

// TODO: remove or revisit this.
impl TryFrom<ExecutionResult> for StatementResult {
    type Error = Error;

    fn try_from(result: ExecutionResult) -> Result<Self> {
        Ok(match result {
            ExecutionResult::CreateTable { name } => StatementResult::CreateTable { name },
            ExecutionResult::DropTable { name, existed } => {
                StatementResult::DropTable { name, existed }
            }
            ExecutionResult::Delete { count } => StatementResult::Delete { count },
            ExecutionResult::Insert { count } => StatementResult::Create { count },
            ExecutionResult::Select { iter } => StatementResult::Query {
                columns: iter.columns,
                rows: iter.rows.collect::<Result<_>>()?,
            },
            ExecutionResult::Update { count } => StatementResult::Update { count },
        })
    }
}
