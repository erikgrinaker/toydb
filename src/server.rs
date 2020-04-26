use crate::kv;
use crate::raft;
use crate::sql;
use crate::sql::engine::{Engine as _, Mode};
use crate::sql::execution::ResultSet;
use crate::sql::schema::{Catalog as _, Table};
use crate::sql::types::Row;
use crate::Error;

use futures::sink::SinkExt as _;
use log::{error, info};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt as _;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// A ToyDB server.
pub struct Server {
    raft: raft::Server<kv::storage::BLog, sql::engine::raft::State<kv::storage::File>>,
    raft_listener: Option<TcpListener>,
    sql_listener: Option<TcpListener>,
}

impl Server {
    /// Creates a new ToyDB server.
    pub fn new(id: &str, peers: HashMap<String, String>, dir: &str) -> Result<Self, Error> {
        let path = Path::new(dir);
        fs::create_dir_all(path)?;
        Ok(Server {
            raft: raft::Server::new(
                id,
                peers,
                raft::Log::new(kv::Simple::new(kv::storage::BLog::new(
                    fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(path.join("raft"))?,
                )?))?,
                sql::engine::Raft::new_state(kv::MVCC::new(kv::storage::File::new(
                    fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(path.join("state"))?,
                )?)),
            )?,
            raft_listener: None,
            sql_listener: None,
        })
    }

    /// Starts listening on the given ports. Must be called before serve.
    pub async fn listen(mut self, sql_addr: &str, raft_addr: &str) -> Result<Self, Error> {
        let (sql, raft) =
            tokio::try_join!(TcpListener::bind(sql_addr), TcpListener::bind(raft_addr),)?;
        info!("Listening on {} (SQL) and {} (Raft)", sql.local_addr()?, raft.local_addr()?);
        self.sql_listener = Some(sql);
        self.raft_listener = Some(raft);
        Ok(self)
    }

    /// Serves Raft and SQL requests until the returned future is dropped. Consumes the server.
    pub async fn serve(self) -> Result<(), Error> {
        let sql_listener = self
            .sql_listener
            .ok_or_else(|| Error::Internal("Must listen before serving".into()))?;
        let raft_listener = self
            .raft_listener
            .ok_or_else(|| Error::Internal("Must listen before serving".into()))?;
        let sql_engine = sql::engine::Raft::new(self.raft.client().await);

        tokio::try_join!(
            self.raft.serve(raft_listener),
            Self::serve_sql(sql_listener, sql_engine),
        )?;
        Ok(())
    }

    /// Serves SQL clients.
    async fn serve_sql(mut listener: TcpListener, engine: sql::engine::Raft) -> Result<(), Error> {
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = Session::new(engine.clone())?;
            tokio::spawn(async move {
                info!("Client {} connected", peer);
                match session.handle(socket).await {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(err) => error!("Client {} error: {}", peer, err),
                }
            });
        }
        Ok(())
    }
}

/// A client request.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Execute(String),
    GetTable(String),
    ListTables,
    Status,
}

/// A server response.
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Error(Error),
    Execute(ResultSet),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    Status(raft::Status),
}

/// A client session coupled to a SQL session.
pub struct Session {
    engine: sql::engine::Raft,
    sql: sql::engine::Session<sql::engine::Raft>,
}

impl Session {
    /// Creates a new client session.
    fn new(engine: sql::engine::Raft) -> Result<Self, Error> {
        Ok(Self { sql: engine.session()?, engine })
    }

    /// Handles a client connection.
    async fn handle(mut self, socket: TcpStream) -> Result<(), Error> {
        let mut stream = tokio_serde::Framed::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::Cbor::default(),
        );
        while let Some(request) = stream.try_next().await? {
            let mut response = self.request(request).await;
            let mut rows: Box<dyn Iterator<Item = Response> + Send> = Box::new(std::iter::empty());
            if let Response::Execute(ResultSet::Query { ref mut relation }) = &mut response {
                rows = Box::new(
                    relation
                        .rows
                        .take()
                        .unwrap_or_else(|| Box::new(std::iter::empty()))
                        .map(|result| match result {
                            Ok(row) => Response::Row(Some(row)),
                            Err(error) => Response::Error(error),
                        })
                        .chain(std::iter::once(Response::Row(None)))
                        .scan(false, |err_sent, response| match (&err_sent, &response) {
                            (true, _) => None,
                            (_, Response::Error(_)) => {
                                *err_sent = true;
                                Some(response)
                            }
                            _ => Some(response),
                        })
                        .fuse(),
                );
            }
            stream.send(response).await?;
            stream.send_all(&mut tokio::stream::iter(rows.map(Ok))).await?;
        }
        Ok(())
    }

    /// Runs a request, returning errors as Response::Error.
    /// FIXME Blocks the thread, since the SQL engine is not async.
    async fn request(&mut self, request: Request) -> Response {
        match tokio::task::block_in_place(|| self.call(request)) {
            Ok(response) => response,
            Err(error) => Response::Error(error),
        }
    }

    /// Executes a simple request/response call, without error handling.
    fn call(&mut self, request: Request) -> Result<Response, Error> {
        Ok(match request {
            Request::Execute(query) => Response::Execute(self.sql.execute(&query)?),
            Request::GetTable(table) => Response::GetTable(
                self.sql.with_txn(Mode::ReadOnly, |txn| txn.must_read_table(&table))?,
            ),
            Request::ListTables => {
                Response::ListTables(self.sql.with_txn(Mode::ReadOnly, |txn| {
                    Ok(txn.scan_tables()?.map(|t| t.name).collect())
                })?)
            }
            Request::Status => Response::Status(self.engine.status()?),
        })
    }
}
