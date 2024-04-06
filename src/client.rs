use crate::error::{Error, Result};
use crate::server::{Request, Response};
use crate::sql::engine::Status;
use crate::sql::execution::ResultSet;
use crate::sql::schema::Table;

use futures::sink::SinkExt as _;
use futures::stream::TryStreamExt as _;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

type Connection = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Result<Response>,
    Request,
    tokio_serde::formats::Bincode<Result<Response>, Request>,
>;

/// A toyDB client
pub struct Client {
    conn: Connection,
    txn: Option<(u64, bool)>,
}

impl Client {
    /// Creates a new client
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        Ok(Self {
            conn: tokio_serde::Framed::new(
                Framed::new(TcpStream::connect(addr).await?, LengthDelimitedCodec::new()),
                tokio_serde::formats::Bincode::default(),
            ),
            txn: None,
        })
    }

    /// Call a server method
    async fn call(&mut self, request: Request) -> Result<Response> {
        self.conn.send(request).await?;
        match self.conn.try_next().await? {
            Some(result) => result,
            None => Err(Error::Internal("Server disconnected".into())),
        }
    }

    /// Executes a query
    pub async fn execute(&mut self, query: &str) -> Result<ResultSet> {
        let mut resultset = match self.call(Request::Execute(query.into())).await? {
            Response::Execute(rs) => rs,
            resp => return Err(Error::Internal(format!("Unexpected response {:?}", resp))),
        };
        if let ResultSet::Query { columns, .. } = resultset {
            // FIXME We buffer rows for now to avoid lifetime hassles
            let mut rows = Vec::new();
            while let Some(result) = self.conn.try_next().await? {
                match result? {
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
    pub async fn get_table(&mut self, table: &str) -> Result<Table> {
        match self.call(Request::GetTable(table.into())).await? {
            Response::GetTable(t) => Ok(t),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Lists database tables
    pub async fn list_tables(&mut self) -> Result<Vec<String>> {
        match self.call(Request::ListTables).await? {
            Response::ListTables(t) => Ok(t),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Checks server status
    pub async fn status(&mut self) -> Result<Status> {
        match self.call(Request::Status).await? {
            Response::Status(s) => Ok(s),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Returns the version and read-only state of the txn
    pub fn txn(&self) -> Option<(u64, bool)> {
        self.txn
    }
}
