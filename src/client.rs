use crate::error::{Error, Result};
use crate::server::{Request, Response};
use crate::sql::engine::{Mode, Status};
use crate::sql::execution::ResultSet;
use crate::sql::schema::Table;

use futures::future::FutureExt as _;
use futures::sink::SinkExt as _;
use futures::stream::TryStreamExt as _;
use rand::Rng as _;
use std::cell::Cell;
use std::future::Future;
use std::ops::{Deref, Drop};
use std::sync::Arc;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

type Connection = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Result<Response>,
    Request,
    tokio_serde::formats::Bincode<Result<Response>, Request>,
>;

/// Number of serialization retries in with_txn()
const WITH_TXN_RETRIES: u8 = 8;

/// A toyDB client
#[derive(Clone)]
pub struct Client {
    conn: Arc<Mutex<Connection>>,
    txn: Cell<Option<(u64, Mode)>>,
}

impl Client {
    /// Creates a new client
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        Ok(Self {
            conn: Arc::new(Mutex::new(tokio_serde::Framed::new(
                Framed::new(TcpStream::connect(addr).await?, LengthDelimitedCodec::new()),
                tokio_serde::formats::Bincode::default(),
            ))),
            txn: Cell::new(None),
        })
    }

    /// Call a server method
    async fn call(&self, request: Request) -> Result<Response> {
        let mut conn = self.conn.lock().await;
        self.call_locked(&mut conn, request).await
    }

    /// Call a server method while holding the mutex lock
    async fn call_locked(
        &self,
        conn: &mut MutexGuard<'_, Connection>,
        request: Request,
    ) -> Result<Response> {
        conn.send(request).await?;
        match conn.try_next().await? {
            Some(result) => result,
            None => Err(Error::Internal("Server disconnected".into())),
        }
    }

    /// Executes a query
    pub async fn execute(&self, query: &str) -> Result<ResultSet> {
        let mut conn = self.conn.lock().await;
        let mut resultset =
            match self.call_locked(&mut conn, Request::Execute(query.into())).await? {
                Response::Execute(rs) => rs,
                resp => return Err(Error::Internal(format!("Unexpected response {:?}", resp))),
            };
        if let ResultSet::Query { columns, .. } = resultset {
            // FIXME We buffer rows for now to avoid lifetime hassles
            let mut rows = Vec::new();
            while let Some(result) = conn.try_next().await? {
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
            ResultSet::Begin { id, mode } => self.txn.set(Some((*id, *mode))),
            ResultSet::Commit { .. } => self.txn.set(None),
            ResultSet::Rollback { .. } => self.txn.set(None),
            _ => {}
        }
        Ok(resultset)
    }

    /// Fetches the table schema as SQL
    pub async fn get_table(&self, table: &str) -> Result<Table> {
        match self.call(Request::GetTable(table.into())).await? {
            Response::GetTable(t) => Ok(t),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Lists database tables
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        match self.call(Request::ListTables).await? {
            Response::ListTables(t) => Ok(t),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Checks server status
    pub async fn status(&self) -> Result<Status> {
        match self.call(Request::Status).await? {
            Response::Status(s) => Ok(s),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Returns the transaction status of the client
    pub fn txn(&self) -> Option<(u64, Mode)> {
        self.txn.get()
    }

    /// Runs a query in a transaction, automatically retrying serialization failures with
    /// exponential backoff.
    pub async fn with_txn<W, F, R>(&self, mut with: W) -> Result<R>
    where
        W: FnMut(Client) -> F,
        F: Future<Output = Result<R>>,
    {
        for i in 0..WITH_TXN_RETRIES {
            if i > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(
                    2_u64.pow(i as u32 - 1) * rand::thread_rng().gen_range(25..=75),
                ))
                .await;
            }
            let result = async {
                self.execute("BEGIN").await?;
                let result = with(self.clone()).await?;
                self.execute("COMMIT").await?;
                Ok(result)
            }
            .await;
            if result.is_err() {
                self.execute("ROLLBACK").await.ok();
                if matches!(result, Err(Error::Serialization) | Err(Error::Abort)) {
                    continue;
                }
            }
            return result;
        }
        Err(Error::Serialization)
    }
}

/// A toyDB client pool
pub struct Pool {
    clients: Vec<Mutex<Client>>,
}

impl Pool {
    /// Creates a new connection pool for the given servers, eagerly connecting clients.
    pub async fn new<A: ToSocketAddrs + Clone>(addrs: Vec<A>, size: u64) -> Result<Self> {
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

    /// Fetches a client from the pool. It is reset (i.e. any open txns are rolled back) and
    /// returned when it goes out of scope.
    pub async fn get(&self) -> PoolClient<'_> {
        let (client, index, _) =
            futures::future::select_all(self.clients.iter().map(|m| m.lock().boxed())).await;
        PoolClient::new(index, client)
    }

    /// Returns the size of the pool
    pub fn size(&self) -> usize {
        self.clients.len()
    }
}

/// A client returned from the pool
pub struct PoolClient<'a> {
    id: usize,
    client: MutexGuard<'a, Client>,
}

impl<'a> PoolClient<'a> {
    /// Creates a new PoolClient
    fn new(id: usize, client: MutexGuard<'a, Client>) -> Self {
        Self { id, client }
    }

    /// Returns the ID of the client in the pool
    pub fn id(&self) -> usize {
        self.id
    }
}

impl<'a> Deref for PoolClient<'a> {
    type Target = MutexGuard<'a, Client>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<'a> Drop for PoolClient<'a> {
    fn drop(&mut self) {
        if self.txn().is_some() {
            // FIXME This should disconnect or destroy the client if it errors.
            futures::executor::block_on(self.client.execute("ROLLBACK")).ok();
        }
    }
}
