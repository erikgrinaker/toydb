use crate::server::{Request, Response, Status};
use crate::sql::execution::ResultSet;
use crate::sql::schema::Table;
use crate::Error;

use futures::future::FutureExt as _;
use futures::sink::SinkExt as _;
use futures::stream::TryStreamExt as _;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

type Connection = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Response,
    Request,
    tokio_serde::formats::Cbor<Response, Request>,
>;

/// A ToyDB client
pub struct Client {
    conn: Mutex<Connection>,
}

impl Client {
    /// Creates a new client
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        Ok(Self {
            conn: Mutex::new(tokio_serde::Framed::new(
                Framed::new(TcpStream::connect(addr).await?, LengthDelimitedCodec::new()),
                tokio_serde::formats::Cbor::default(),
            )),
        })
    }

    /// Call a server method
    async fn call(&self, request: Request) -> Result<Response, Error> {
        let mut conn = self.conn.lock().await;
        self.call_locked(&mut conn, request).await
    }

    /// Call a server method while holding the mutex lock
    async fn call_locked(
        &self,
        conn: &mut MutexGuard<'_, Connection>,
        request: Request,
    ) -> Result<Response, Error> {
        conn.send(request).await?;
        match conn.try_next().await? {
            Some(Response::Error(err)) => Err(err),
            Some(response) => Ok(response),
            None => Err(Error::Internal("Server disconnected".into())),
        }
    }

    /// Executes a query
    pub async fn execute(&self, query: &str) -> Result<ResultSet, Error> {
        let mut conn = self.conn.lock().await;
        let mut resultset =
            match self.call_locked(&mut conn, Request::Execute(query.into())).await? {
                Response::Execute(rs) => rs,
                resp => return Err(Error::Internal(format!("Unexpected response {:?}", resp))),
            };
        if let ResultSet::Query { ref mut relation } = &mut resultset {
            // FIXME We buffer rows for now to avoid lifetime hassles
            let mut rows = Vec::new();
            while let Some(response) = conn.try_next().await? {
                match response {
                    Response::Row(Some(row)) => rows.push(row),
                    Response::Row(None) => break,
                    Response::Error(error) => return Err(error),
                    response => {
                        return Err(Error::Internal(format!("Unexpected response {:?}", response)))
                    }
                }
            }
            relation.rows = Some(Box::new(rows.into_iter().map(Ok)));
        };
        Ok(resultset)
    }

    /// Fetches the table schema as SQL
    pub async fn get_table(&self, table: &str) -> Result<Table, Error> {
        match self.call(Request::GetTable(table.into())).await? {
            Response::GetTable(t) => Ok(t),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Lists database tables
    pub async fn list_tables(&self) -> Result<Vec<String>, Error> {
        match self.call(Request::ListTables).await? {
            Response::ListTables(t) => Ok(t),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Checks server status
    pub async fn status(&self) -> Result<Status, Error> {
        match self.call(Request::Status).await? {
            Response::Status(s) => Ok(s),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }
}

/// A ToyDB client pool
pub struct Pool {
    clients: Vec<Mutex<Client>>,
}

impl Pool {
    /// Creates a new connection pool for the given servers, eagerly connecting clients.
    pub async fn new<A: ToSocketAddrs + Clone>(addrs: Vec<A>, size: u64) -> Result<Self, Error> {
        let mut addrs = addrs.into_iter().cycle();
        let clients = futures::future::try_join_all(
            std::iter::from_fn(|| {
                Some(Client::new(addrs.next().unwrap()).map(|r| r.map(Mutex::new)))
            })
            .take(size as usize),
        )
        .await?;
        Ok(Self { clients })
    }

    /// Fetches a client from the pool, returning it when it goes out of scope.
    /// FIXME Should rollback txn (if any) when returning.
    pub async fn get(&self) -> (usize, MutexGuard<'_, Client>) {
        let (client, index, _) =
            futures::future::select_all(self.clients.iter().map(|m| m.lock().boxed())).await;
        (index, client)
    }

    /// Returns the size of the pool
    pub fn size(&self) -> usize {
        self.clients.len()
    }
}
