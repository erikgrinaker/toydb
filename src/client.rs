use std::cmp::min;
use std::io::Write as _;
use std::time::Duration;

use rand::Rng;

use crate::encoding::Value as _;
use crate::errdata;
use crate::error::{Error, Result};
use crate::server::{Request, Response, Status};
use crate::sql::engine::StatementResult;
use crate::sql::types::Table;
use crate::storage::mvcc;

/// A toyDB client. Connects to a server via TCP and submits SQL statements and
/// other requests.
pub struct Client {
    /// Inbound response stream.
    reader: std::io::BufReader<std::net::TcpStream>,
    /// Outbound request stream.
    writer: std::io::BufWriter<std::net::TcpStream>,
    /// The current transaction, if any.
    txn: Option<mvcc::TransactionState>,
}

impl Client {
    /// Connects to a toyDB server, creating a new client.
    pub fn connect(addr: impl std::net::ToSocketAddrs) -> Result<Self> {
        let socket = std::net::TcpStream::connect(addr)?;
        let reader = std::io::BufReader::new(socket.try_clone()?);
        let writer = std::io::BufWriter::new(socket);
        Ok(Self { reader, writer, txn: None })
    }

    /// Sends a request to the server, returning the response.
    fn request(&mut self, request: Request) -> Result<Response> {
        request.encode_into(&mut self.writer)?;
        self.writer.flush()?;
        Result::decode_from(&mut self.reader)?
    }

    /// Executes a SQL statement.
    pub fn execute(&mut self, statement: &str) -> Result<StatementResult> {
        let result = match self.request(Request::Execute(statement.to_string()))? {
            Response::Execute(result) => result,
            response => return errdata!("unexpected response {response:?}"),
        };
        // Update the transaction state.
        match &result {
            StatementResult::Begin(state) => self.txn = Some(state.clone()),
            StatementResult::Commit { .. } => self.txn = None,
            StatementResult::Rollback { .. } => self.txn = None,
            _ => {}
        }
        Ok(result)
    }

    /// Fetches a table schema.
    pub fn get_table(&mut self, table: &str) -> Result<Table> {
        match self.request(Request::GetTable(table.to_string()))? {
            Response::GetTable(table) => Ok(table),
            response => errdata!("unexpected response: {response:?}"),
        }
    }

    /// Lists database tables.
    pub fn list_tables(&mut self) -> Result<Vec<String>> {
        match self.request(Request::ListTables)? {
            Response::ListTables(tables) => Ok(tables),
            response => errdata!("unexpected response: {response:?}"),
        }
    }

    /// Returns server status.
    pub fn status(&mut self) -> Result<Status> {
        match self.request(Request::Status)? {
            Response::Status(status) => Ok(status),
            response => errdata!("unexpected response: {response:?}"),
        }
    }

    /// Returns the transaction state.
    pub fn txn(&self) -> Option<&mvcc::TransactionState> {
        self.txn.as_ref()
    }

    /// Runs the given closure, automatically retrying serialization and abort
    /// errors. If a transaction is open following an error, it is automatically
    /// rolled back. It is the caller's responsibility to use a transaction in
    /// the closure where appropriate (i.e. when it is not idempotent).
    pub fn with_retry<T>(&mut self, f: impl Fn(&mut Client) -> Result<T>) -> Result<T> {
        const MAX_RETRIES: u32 = 10;
        const MIN_WAIT: u64 = 10;
        const MAX_WAIT: u64 = 2_000;
        let mut retries: u32 = 0;
        loop {
            match f(self) {
                Ok(result) => return Ok(result),
                Err(Error::Serialization | Error::Abort) if retries < MAX_RETRIES => {
                    if self.txn().is_some() {
                        self.execute("ROLLBACK")?;
                    }
                    // Use exponential backoff starting at MIN_WAIT doubling up
                    // to MAX_WAIT, but randomize the wait time in this interval
                    // to reduce the chance of collisions.
                    let mut wait = min(MIN_WAIT * 2_u64.pow(retries), MAX_WAIT);
                    wait = rand::thread_rng().gen_range(MIN_WAIT..=wait);
                    std::thread::sleep(Duration::from_millis(wait));
                    retries += 1;
                }
                Err(error) => {
                    if self.txn().is_some() {
                        self.execute("ROLLBACK").ok(); // ignore rollback error
                    }
                    return Err(error);
                }
            }
        }
    }
}
