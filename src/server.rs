use crate::encoding::bincode;
use crate::error::Result;
use crate::raft;
use crate::sql;
use crate::sql::engine::Engine as _;
use crate::sql::execution::ResultSet;
use crate::sql::schema::{Catalog as _, Table};
use crate::sql::types::Row;

use ::log::{debug, error, info};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// A toyDB server.
pub struct Server {
    raft: raft::Server,
}

impl Server {
    /// Creates a new toyDB server.
    pub fn new(
        id: raft::NodeID,
        peers: HashMap<raft::NodeID, String>,
        raft_log: raft::Log,
        raft_state: Box<dyn raft::State>,
    ) -> Result<Self> {
        Ok(Server { raft: raft::Server::new(id, peers, raft_log, raft_state)? })
    }

    /// Serves Raft and SQL requests until the returned future is dropped. Consumes the server.
    pub async fn serve(
        self,
        raft_listener: TcpListener,
        sql_listener: std::net::TcpListener,
    ) -> Result<()> {
        info!(
            "Listening on {} (SQL) and {} (Raft)",
            sql_listener.local_addr()?,
            raft_listener.local_addr()?
        );

        let (raft_tx, raft_rx) = mpsc::unbounded_channel();

        tokio::task::spawn_blocking(move || Self::serve_sql(sql_listener, raft_tx));

        tokio::try_join!(self.raft.serve(raft_listener, raft_rx))?;
        Ok(())
    }

    /// Serves SQL clients.
    fn serve_sql(listener: std::net::TcpListener, raft_tx: raft::ClientSender) {
        std::thread::scope(|s| loop {
            let (socket, peer) = match listener.accept() {
                Ok(r) => r,
                Err(err) => {
                    error!("Connection failed: {}", err);
                    continue;
                }
            };
            let raft_tx = raft_tx.clone();
            s.spawn(move || {
                let session = Session::new(sql::engine::Raft::new(raft_tx));
                info!("Client {} connected", peer);
                match session.handle(socket) {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(err) => error!("Client {} error: {}", peer, err),
                }
            });
        })
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
    Execute(ResultSet),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    Status(sql::engine::Status),
}

/// A client session coupled to a SQL session.
pub struct Session {
    engine: sql::engine::Raft,
    sql: sql::engine::Session<sql::engine::Raft>,
}

impl Session {
    /// Creates a new client session.
    fn new(engine: sql::engine::Raft) -> Self {
        Self { sql: engine.session(), engine }
    }

    /// Handles a client connection.
    fn handle(mut self, mut socket: std::net::TcpStream) -> Result<()> {
        while let Some(request) = bincode::maybe_deserialize_from(&mut socket)? {
            let mut response = self.request(request);
            let mut rows: Box<dyn Iterator<Item = Result<Response>> + Send> =
                Box::new(std::iter::empty());
            if let Ok(Response::Execute(ResultSet::Query { rows: ref mut resultrows, .. })) =
                &mut response
            {
                // TODO: don't stream results, for simplicity.
                rows = Box::new(
                    std::mem::replace(resultrows, Box::new(std::iter::empty()))
                        .map(|result| result.map(|row| Response::Row(Some(row))))
                        .chain(std::iter::once(Ok(Response::Row(None))))
                        .scan(false, |err_sent, response| match (&err_sent, &response) {
                            (true, _) => None,
                            (_, Err(error)) => {
                                *err_sent = true;
                                Some(Err(error.clone()))
                            }
                            _ => Some(response),
                        })
                        .fuse(),
                );
            }

            bincode::serialize_into(&mut socket, &response)?;

            for row in rows {
                bincode::serialize_into(&mut socket, &row)?;
            }
        }
        Ok(())
    }
    /// Executes a request.
    pub fn request(&mut self, request: Request) -> Result<Response> {
        debug!("Processing request {:?}", request);
        let response = match request {
            Request::Execute(query) => Response::Execute(self.sql.execute(&query)?),
            Request::GetTable(table) => {
                Response::GetTable(self.sql.read_with_txn(|txn| txn.must_read_table(&table))?)
            }
            Request::ListTables => Response::ListTables(
                self.sql.read_with_txn(|txn| Ok(txn.scan_tables()?.map(|t| t.name).collect()))?,
            ),
            Request::Status => Response::Status(self.engine.status()?),
        };
        debug!("Returning response {:?}", response);
        Ok(response)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.sql.execute("ROLLBACK").ok();
    }
}
