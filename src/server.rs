use crate::encoding::bincode;
use crate::error::Result;
use crate::raft;
use crate::sql;
use crate::sql::engine::Engine as _;
use crate::sql::execution::ResultSet;
use crate::sql::schema::{Catalog as _, Table};
use crate::sql::types::Row;

use log::{debug, error, info};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;

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

    /// Serves Raft and SQL requests indefinitely. Consumes the server.
    pub fn serve(
        self,
        raft_addr: impl std::net::ToSocketAddrs,
        sql_addr: impl std::net::ToSocketAddrs,
    ) -> Result<()> {
        let raft_listener = std::net::TcpListener::bind(raft_addr)?;
        let sql_listener = std::net::TcpListener::bind(sql_addr)?;
        info!(
            "Listening on {} (SQL) and {} (Raft)",
            sql_listener.local_addr()?,
            raft_listener.local_addr()?
        );

        std::thread::scope(move |s| {
            let (raft_tx, raft_rx) = crossbeam::channel::unbounded();

            s.spawn(move || self.raft.serve(raft_listener, raft_rx));
            s.spawn(move || Self::sql_accept(sql_listener, raft_tx));
        });

        Ok(())
    }

    /// Accepts new SQL client connections and spawns session threads for them.
    fn sql_accept(listener: std::net::TcpListener, raft_tx: raft::ClientSender) {
        std::thread::scope(|s| loop {
            let (socket, peer) = match listener.accept() {
                Ok(r) => r,
                Err(err) => {
                    error!("Accept failed: {err}");
                    continue;
                }
            };
            let raft_tx = raft_tx.clone();
            s.spawn(move || {
                debug!("Client {peer} connected");
                match Self::sql_session(socket, raft_tx) {
                    Ok(()) => debug!("Client {peer} disconnected"),
                    Err(err) => error!("Client {peer} error: {err}"),
                }
            });
        })
    }

    /// Processes a client SQL session, by executing SQL statements against the
    /// Raft node.
    fn sql_session(mut socket: std::net::TcpStream, raft_tx: raft::ClientSender) -> Result<()> {
        let mut session = sql::engine::Raft::new(raft_tx).session();
        while let Some(request) = bincode::maybe_deserialize_from(&mut socket)? {
            // Execute request.
            debug!("Received request {request:?}");
            let mut response = match request {
                Request::Execute(query) => session.execute(&query).map(Response::Execute),
                Request::Status => session.status().map(Response::Status),
                Request::GetTable(table) => session
                    .with_txn_read_only(|txn| txn.must_read_table(&table))
                    .map(Response::GetTable),
                Request::ListTables => session
                    .with_txn_read_only(|txn| Ok(txn.scan_tables()?.map(|t| t.name).collect()))
                    .map(Response::ListTables),
            };

            // Process response.
            debug!("Returning response {response:?}");
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
}

/// A SQL client request.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    /// Executes a SQL statement.
    Execute(String),
    /// Fetches the given table schema.
    GetTable(String),
    /// Lists all tables.
    ListTables,
    /// Returns server status.
    Status,
}

/// A SQL server response.
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Execute(ResultSet),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    Status(sql::engine::Status),
}
