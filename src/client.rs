use std::io::Write as _;

use crate::encoding::Value as _;
use crate::errdata;
use crate::error::{Error, Result};
use crate::server::{Request, Response, Status};
use crate::sql::engine::StatementResult;
use crate::sql::types::Table;
use crate::storage::mvcc;

use rand::Rng;

/// A toyDB client
pub struct Client {
    reader: std::io::BufReader<std::net::TcpStream>,
    writer: std::io::BufWriter<std::net::TcpStream>,
    txn: Option<mvcc::TransactionState>,
}

impl Client {
    /// Creates a new client
    pub fn new(addr: impl std::net::ToSocketAddrs) -> Result<Self> {
        let socket = std::net::TcpStream::connect(addr)?;
        let reader = std::io::BufReader::new(socket.try_clone()?);
        let writer = std::io::BufWriter::new(socket);
        Ok(Self { reader, writer, txn: None })
    }

    /// Call a server method
    fn call(&mut self, request: Request) -> Result<Response> {
        request.encode_into(&mut self.writer)?;
        self.writer.flush()?;
        Result::<Response>::decode_from(&mut self.reader)?
    }

    /// Executes a query
    pub fn execute(&mut self, query: &str) -> Result<StatementResult> {
        let resultset = match self.call(Request::Execute(query.into()))? {
            Response::Execute(rs) => rs,
            response => return errdata!("unexpected response {response:?}"),
        };
        match &resultset {
            StatementResult::Begin { state } => self.txn = Some(state.clone()),
            StatementResult::Commit { .. } => self.txn = None,
            StatementResult::Rollback { .. } => self.txn = None,
            _ => {}
        }
        Ok(resultset)
    }

    /// Fetches the table schema as SQL
    pub fn get_table(&mut self, table: &str) -> Result<Table> {
        match self.call(Request::GetTable(table.into()))? {
            Response::GetTable(t) => Ok(t),
            resp => errdata!("unexpected response: {resp:?}"),
        }
    }

    /// Lists database tables
    pub fn list_tables(&mut self) -> Result<Vec<String>> {
        match self.call(Request::ListTables)? {
            Response::ListTables(t) => Ok(t),
            resp => errdata!("unexpected response: {resp:?}"),
        }
    }

    /// Checks server status
    pub fn status(&mut self) -> Result<Status> {
        match self.call(Request::Status)? {
            Response::Status(s) => Ok(s),
            resp => errdata!("unexpected response: {resp:?}"),
        }
    }

    /// Returns the transaction state.
    pub fn txn(&self) -> Option<&mvcc::TransactionState> {
        self.txn.as_ref()
    }

    /// Runs the given closure, automatically retrying serialization and abort
    /// errors. If a transaction is open following an error, it is automatically
    /// rolled back. It is the caller's responsibility to use a transaction
    /// in the closure where appropriate (i.e. when it is not idempotent).
    ///
    /// TODO: test this.
    pub fn with_retry<F, T>(&mut self, mut f: F) -> Result<T>
    where
        F: FnMut(&mut Client) -> Result<T>,
    {
        const MAX_RETRIES: u32 = 10;
        const MIN_WAIT: u64 = 10;
        const MAX_WAIT: u64 = 2_000;

        let mut retries: u32 = 0;
        loop {
            match f(self) {
                Ok(r) => return Ok(r),
                Err(Error::Serialization | Error::Abort) if retries < MAX_RETRIES => {
                    if self.txn().is_some() {
                        self.execute("ROLLBACK")?;
                    }

                    // Use exponential backoff starting at MIN_WAIT doubling up
                    // to MAX_WAIT, but randomize the wait time in this interval
                    // to reduce the chance of collisions.
                    let mut wait = std::cmp::min(MIN_WAIT * 2_u64.pow(retries), MAX_WAIT);
                    wait = rand::thread_rng().gen_range(MIN_WAIT..=wait);
                    std::thread::sleep(std::time::Duration::from_millis(wait));
                    retries += 1;
                }
                Err(e) => {
                    if self.txn().is_some() {
                        self.execute("ROLLBACK").ok(); // ignore rollback error
                    }
                    return Err(e);
                }
            }
        }
    }
}
