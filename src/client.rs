use crate::encoding::bincode;
use crate::error::{Error, Result};
use crate::server::{Request, Response};
use crate::sql::engine::Status;
use crate::sql::execution::ResultSet;
use crate::sql::schema::Table;

use rand::Rng;
use std::io::Write as _;

/// A toyDB client
pub struct Client {
    reader: std::io::BufReader<std::net::TcpStream>,
    writer: std::io::BufWriter<std::net::TcpStream>,
    txn: Option<(u64, bool)>,
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
        bincode::serialize_into(&mut self.writer, &request)?;
        self.writer.flush()?;
        bincode::deserialize_from(&mut self.reader)?
    }

    /// Executes a query
    pub fn execute(&mut self, query: &str) -> Result<ResultSet> {
        let mut resultset = match self.call(Request::Execute(query.into()))? {
            Response::Execute(rs) => rs,
            resp => return Err(Error::Internal(format!("Unexpected response {:?}", resp))),
        };
        if let ResultSet::Query { columns, .. } = resultset {
            // FIXME We buffer rows for now to avoid lifetime hassles
            let mut rows = Vec::new();
            loop {
                match bincode::deserialize_from::<_, Result<_>>(&mut self.reader)?? {
                    Response::Row(Some(row)) => rows.push(row),
                    Response::Row(None) => break,
                    response => {
                        return Err(Error::Internal(format!("Unexpected response {:?}", response)))
                    }
                }
            }
            resultset = ResultSet::Query { columns, rows: Box::new(rows.into_iter().map(Ok)) }
        };
        match &resultset {
            ResultSet::Begin { version, read_only } => self.txn = Some((*version, *read_only)),
            ResultSet::Commit { .. } => self.txn = None,
            ResultSet::Rollback { .. } => self.txn = None,
            _ => {}
        }
        Ok(resultset)
    }

    /// Fetches the table schema as SQL
    pub fn get_table(&mut self, table: &str) -> Result<Table> {
        match self.call(Request::GetTable(table.into()))? {
            Response::GetTable(t) => Ok(t),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Lists database tables
    pub fn list_tables(&mut self) -> Result<Vec<String>> {
        match self.call(Request::ListTables)? {
            Response::ListTables(t) => Ok(t),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Checks server status
    pub fn status(&mut self) -> Result<Status> {
        match self.call(Request::Status)? {
            Response::Status(s) => Ok(s),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Returns the version and read-only state of the txn
    pub fn txn(&self) -> Option<(u64, bool)> {
        self.txn
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
