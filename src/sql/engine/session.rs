use super::raft::{Raft, Status};
use super::{Engine, Transaction as _};
use crate::error::{Error, Result};
use crate::sql::execution::ExecutionResult;
use crate::sql::parser::{ast, Parser};
use crate::sql::planner::Plan;
use crate::sql::types::{Label, Row, Rows, Value};
use crate::storage::mvcc;
use crate::{errdata, errinput};

use itertools::Itertools as _;
use log::error;
use serde::{Deserialize, Serialize};

/// A SQL client session. Executes raw SQL statements against a SQL engine and
/// handles transaction control.
pub struct Session<'a, E: Engine<'a>> {
    /// The SQL engine.
    engine: &'a E,
    /// The current transaction, if any.
    txn: Option<E::Transaction>,
}

impl<'a, E: Engine<'a>> Session<'a, E> {
    /// Creates a new session using the given SQL engine.
    pub fn new(engine: &'a E) -> Self {
        Self { engine, txn: None }
    }

    /// Executes a client statement.
    pub fn execute(&mut self, statement: &str) -> Result<StatementResult> {
        // Parse and execute the statement. Transaction control is done here,
        // other statements are executed by the SQL engine.
        Ok(match Parser::new(statement).parse()? {
            ast::Statement::Begin { read_only, as_of } => {
                if self.txn.is_some() {
                    return errinput!("already in a transaction");
                }
                let txn = match (read_only, as_of) {
                    (false, None) => self.engine.begin()?,
                    (true, None) => self.engine.begin_read_only()?,
                    (true, Some(as_of)) => self.engine.begin_as_of(as_of)?,
                    (false, Some(_)) => {
                        return errinput!("can't start read-write transaction in a given version")
                    }
                };
                let state = txn.state().clone();
                self.txn = Some(txn);
                StatementResult::Begin(state)
            }
            ast::Statement::Commit => {
                let Some(txn) = self.txn.take() else {
                    return errinput!("not in a transaction");
                };
                let version = txn.version();
                txn.commit()?;
                StatementResult::Commit { version }
            }
            ast::Statement::Rollback => {
                let Some(txn) = self.txn.take() else {
                    return errinput!("not in a transaction");
                };
                let version = txn.version();
                txn.rollback()?;
                StatementResult::Rollback { version }
            }
            ast::Statement::Explain(statement) => self.with_txn(true, |txn| {
                Ok(StatementResult::Explain(Plan::build(*statement, txn)?.optimize()?))
            })?,
            statement => {
                let read_only = matches!(statement, ast::Statement::Select { .. });
                self.with_txn(read_only, |txn| {
                    Plan::build(statement, txn)?.optimize()?.execute(txn)?.try_into()
                })?
            }
        })
    }

    /// Runs a closure in the session's explicit transaction, if there is one,
    /// otherwise a temporary implicit transaction. If read_only is true, uses a
    /// read-only implicit transaction. Does not retry errors.
    pub fn with_txn<F, T>(&mut self, read_only: bool, f: F) -> Result<T>
    where
        F: FnOnce(&mut E::Transaction) -> Result<T>,
    {
        // Use the current explicit transaction, if there is one.
        if let Some(ref mut txn) = self.txn {
            return f(txn);
        }
        // Otherwise, use an implicit transaction. Doing this session-side
        // results in additional Raft roundtrips to begin and complete the
        // transaction -- we could avoid this if the below-Raft engine supported
        // implicit transactions, but we keep it simple.
        let mut txn = match read_only {
            true => self.engine.begin_read_only()?,
            false => self.engine.begin()?,
        };
        let result = f(&mut txn);
        match result {
            Ok(_) => txn.commit()?,
            Err(_) => txn.rollback()?,
        }
        result
    }
}

impl<'a> Session<'a, Raft> {
    /// Returns Raft SQL engine status.
    pub fn status(&self) -> Result<Status> {
        self.engine.status()
    }
}

/// If the session has an open transaction when dropped, roll it back.
impl<'a, E: Engine<'a>> Drop for Session<'a, E> {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take() {
            if let Err(error) = txn.rollback() {
                error!("implicit transaction rollback failed: {error}")
            }
        }
    }
}

/// A session statement result. Sent across the wire to SQL clients.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum StatementResult {
    Begin(mvcc::TransactionState),
    Commit { version: mvcc::Version },
    Rollback { version: mvcc::Version },
    Explain(Plan),
    CreateTable { name: String },
    DropTable { name: String, existed: bool },
    Delete { count: u64 },
    Insert { count: u64 },
    Update { count: u64 },
    // For simplicity, we buffer and send the entire set of rows as a vector
    // instead of streaming them to the client. Streaming reads haven't been
    // implemented from Raft either, so they're buffered all the way through.
    Select { columns: Vec<Label>, rows: Vec<Row> },
}

/// Converts an execution result into a statement result.
impl TryFrom<ExecutionResult> for StatementResult {
    type Error = Error;

    fn try_from(result: ExecutionResult) -> Result<Self> {
        Ok(match result {
            ExecutionResult::CreateTable { name } => Self::CreateTable { name },
            ExecutionResult::DropTable { name, existed } => Self::DropTable { name, existed },
            ExecutionResult::Delete { count } => Self::Delete { count },
            ExecutionResult::Insert { count } => Self::Insert { count },
            ExecutionResult::Update { count } => Self::Update { count },
            ExecutionResult::Select { rows, columns } => {
                // We buffer the entire set of rows, for simplicity.
                Self::Select { columns, rows: rows.try_collect()? }
            }
        })
    }
}

/// Converts a query result into a row iterator.
impl TryFrom<StatementResult> for Rows {
    type Error = Error;

    fn try_from(result: StatementResult) -> Result<Self> {
        let StatementResult::Select { rows, .. } = result else {
            return errdata!("expected select result, found {result:?}");
        };
        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

/// Extracts the first row from a query result.
impl TryFrom<StatementResult> for Row {
    type Error = Error;

    fn try_from(result: StatementResult) -> Result<Self> {
        let mut rows: Rows = result.try_into()?;
        rows.next().transpose()?.ok_or_else(|| errdata!("no rows returned"))
    }
}

/// Extracts the value of the first column in the first row.
impl TryFrom<StatementResult> for Value {
    type Error = Error;

    fn try_from(result: StatementResult) -> Result<Self> {
        let row: Row = result.try_into()?;
        row.into_iter().next().ok_or_else(|| errdata!("no columns returned"))
    }
}

/// Extracts the first boolean value of the first column in the first row.
impl TryFrom<StatementResult> for bool {
    type Error = Error;

    fn try_from(result: StatementResult) -> Result<Self> {
        Value::try_from(result)?.try_into()
    }
}

/// Extracts the first f64 value of the first column in the first row.
impl TryFrom<StatementResult> for f64 {
    type Error = Error;

    fn try_from(result: StatementResult) -> Result<Self> {
        Value::try_from(result)?.try_into()
    }
}

/// Extracts the first i64 value of the first column in the first row.
impl TryFrom<StatementResult> for i64 {
    type Error = Error;

    fn try_from(result: StatementResult) -> Result<Self> {
        Value::try_from(result)?.try_into()
    }
}

/// Extracts the first string value of the first column in the first row.
impl TryFrom<StatementResult> for String {
    type Error = Error;

    fn try_from(result: StatementResult) -> Result<Self> {
        Value::try_from(result)?.try_into()
    }
}
