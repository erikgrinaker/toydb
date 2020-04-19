use crate::sql::engine::{Engine as _, Mode, Raft, Session as SQLSession};
use crate::sql::execution::ResultSet;
use crate::sql::schema::{Catalog as _, Table};
use crate::sql::types::Row;
use crate::Error;

use futures_util::sink::{Sink, SinkExt as _};
use futures_util::stream::Stream;
use std::marker::Unpin;
use tokio::net::TcpListener;
use tokio::stream::StreamExt as _;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

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
    Status(Status),
}

/// Server status, returned to clients.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub id: String,
    pub version: String,
}

/// The ToyDB SQL server.
// FIXME This should be moved into main server struct
pub struct ToyDB {
    pub id: String,
    pub engine: Raft,
}

impl ToyDB {
    /// Listens for connections from clients.
    pub async fn listen(&self, addr: &str) -> Result<(), Error> {
        let mut listener = TcpListener::bind(addr).await?;
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let stream = tokio_serde::Framed::new(
                Framed::new(socket, LengthDelimitedCodec::new()),
                tokio_serde::formats::Cbor::default(),
            );
            let session = Session::new(&self.id, &self.engine)?;
            tokio::spawn(async move {
                info!("Client {} connected", peer);
                match session.handle(stream).await {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(err) => error!("Client {} error: {}", peer, err),
                }
            });
        }
        Ok(())
    }
}

/// A client session coupled to a SQL session.
pub struct Session {
    server_id: String,
    sql: SQLSession<Raft>,
}

impl Session {
    /// Creates a new client session.
    fn new(server_id: &str, engine: &Raft) -> Result<Self, Error> {
        Ok(Self { server_id: server_id.into(), sql: engine.session()? })
    }

    /// Handles a client connection.
    async fn handle<S>(mut self, mut stream: S) -> Result<(), Error>
    where
        S: Stream<Item = std::io::Result<Request>> + Sink<Response, Error = std::io::Error> + Unpin,
    {
        while let Some(request) = stream.try_next().await? {
            // FIXME call() blocks here, the SQL engine should (possibly) be async
            let mut response = match self.call(request) {
                Ok(response) => response,
                Err(err) => Response::Error(err),
            };
            let rows = match &mut response {
                Response::Execute(ResultSet::Query { ref mut relation }) => relation.rows.take(),
                _ => None,
            };
            stream.send(response).await?;
            if let Some(rows) = rows {
                let mut errored = false;
                for r in rows {
                    stream
                        .send(match r {
                            Ok(row) => Response::Row(Some(row)),
                            Err(error) => {
                                errored = true;
                                Response::Error(error)
                            }
                        })
                        .await?;
                    if errored {
                        break;
                    }
                }
                if !errored {
                    stream.send(Response::Row(None)).await?;
                }
            }
        }
        Ok(())
    }

    /// Executes a simple request/response call.
    fn call(&mut self, req: Request) -> Result<Response, Error> {
        Ok(match req {
            Request::Execute(query) => Response::Execute(self.sql.execute(&query)?),
            Request::GetTable(table) => Response::GetTable(self.get_table(&table)?),
            Request::ListTables => Response::ListTables(self.list_tables()?),
            Request::Status => Response::Status(Status {
                id: self.server_id.clone(),
                version: env!("CARGO_PKG_VERSION").into(),
            }),
        })
    }

    /// Fetches a table.
    fn get_table(&mut self, name: &str) -> Result<Table, Error> {
        self.sql.with_txn(Mode::ReadOnly, |txn| match txn.read_table(name)? {
            Some(t) => Ok(t),
            None => Err(Error::Value(format!("Table {} does not exist", name))),
        })
    }

    /// Fetches a list of tables.
    fn list_tables(&mut self) -> Result<Vec<String>, Error> {
        self.sql.with_txn(Mode::ReadOnly, |txn| Ok(txn.scan_tables()?.map(|t| t.name).collect()))
    }
}
