use crate::encoding::bincode;
use crate::error::Error;
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
    node: raft::Node,
    node_rx: crossbeam::channel::Receiver<raft::Message>,
    peers: HashMap<raft::NodeID, String>,
}

impl Server {
    /// Creates a new toyDB server.
    pub fn new(
        id: raft::NodeID,
        peers: HashMap<raft::NodeID, String>,
        raft_log: raft::Log,
        raft_state: Box<dyn raft::State>,
    ) -> Result<Self> {
        let (node_tx, node_rx) = crossbeam::channel::unbounded();
        Ok(Self {
            node: raft::Node::new(
                id,
                peers.keys().copied().collect(),
                raft_log,
                raft_state,
                node_tx,
            )?,
            peers,
            node_rx,
        })
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
            let (raft_client_tx, raft_client_rx) = crossbeam::channel::unbounded();
            let (raft_step_tx, raft_step_rx) = crossbeam::channel::unbounded();

            // Serve inbound SQL connections.
            s.spawn(move || Self::sql_accept(sql_listener, raft_client_tx));

            // Serve inbound Raft connections.
            s.spawn(move || Self::raft_accept(raft_listener, raft_step_tx));

            // Establish outbound Raft connections.
            let mut raft_peers_tx =
                HashMap::<raft::NodeID, crossbeam::channel::Sender<raft::Message>>::new();

            for (id, addr) in self.peers.into_iter() {
                let (raft_peer_tx, raft_peer_rx) =
                    crossbeam::channel::bounded::<raft::Message>(1000); // TODO: const.
                raft_peers_tx.insert(id, raft_peer_tx);
                s.spawn(move || Self::raft_send_peer(addr, raft_peer_rx));
            }

            // Route Raft messages between the local node, peers, and clients.
            s.spawn(move || {
                Self::raft_route(
                    self.node,
                    self.node_rx,
                    raft_client_rx,
                    raft_step_rx,
                    raft_peers_tx,
                )
                .expect("event processing failed")
            });
        });

        Ok(())
    }

    /// Accepts new SQL client connections and spawns session threads for them.
    fn sql_accept(listener: std::net::TcpListener, raft_client_tx: raft::ClientSender) {
        std::thread::scope(|s| loop {
            let (socket, peer) = match listener.accept() {
                Ok(r) => r,
                Err(err) => {
                    error!("Accept failed: {err}");
                    continue;
                }
            };
            let raft_client_tx = raft_client_tx.clone();
            s.spawn(move || {
                debug!("Client {peer} connected");
                match Self::sql_session(socket, raft_client_tx) {
                    Ok(()) => debug!("Client {peer} disconnected"),
                    Err(err) => error!("Client {peer} error: {err}"),
                }
            });
        })
    }

    /// Processes a client SQL session, by executing SQL statements against the
    /// Raft node.
    fn sql_session(
        mut socket: std::net::TcpStream,
        raft_client_tx: raft::ClientSender,
    ) -> Result<()> {
        let mut session = sql::engine::Raft::new(raft_client_tx).session();
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

    /// Accepts new inbound Raft connections from peers and spawns threads
    /// routing inbound messages to the local Raft node.
    fn raft_accept(
        listener: std::net::TcpListener,
        raft_step_tx: crossbeam::channel::Sender<raft::Message>,
    ) {
        std::thread::scope(|s| loop {
            let (socket, peer) = match listener.accept() {
                Ok(r) => r,
                Err(err) => {
                    error!("Accept failed: {err}");
                    continue;
                }
            };
            let raft_step_tx = raft_step_tx.clone();
            s.spawn(move || {
                debug!("Raft peer {peer} connected");
                match Self::raft_receive_peer(socket, raft_step_tx) {
                    Ok(()) => debug!("Raft peer {peer} disconnected"),
                    Err(err) => error!("Raft peer {peer} error: {err}"),
                }
            });
        });
    }

    /// Receives inbound messages from a peer via TCP, and queues them for
    /// stepping into the Raft node.
    fn raft_receive_peer(
        mut socket: std::net::TcpStream,
        raft_step_tx: crossbeam::channel::Sender<raft::Message>,
    ) -> Result<()> {
        while let Some(message) = bincode::maybe_deserialize_from(&mut socket)? {
            raft_step_tx.send(message)?;
        }
        Ok(())
    }

    /// Sends outbound messages to a peer via TCP.
    fn raft_send_peer(addr: String, rx: crossbeam::channel::Receiver<raft::Message>) {
        loop {
            match std::net::TcpStream::connect(&addr) {
                Ok(mut socket) => {
                    debug!("Connected to Raft peer {addr}");
                    while let Ok(message) = rx.recv() {
                        if let Err(err) = bincode::serialize_into(&mut socket, &message) {
                            error!("Failed sending to Raft peer {addr}: {err}");
                            break;
                        }
                    }
                }
                Err(err) => error!("Failed connecting to Raft peer {addr}: {err}"),
            }
            std::thread::sleep(std::time::Duration::from_millis(1000)); // TODO: const.
            debug!("Disconnected from Raft peer {addr}");
        }
    }

    /// Routes Raft messages.
    fn raft_route(
        mut node: raft::Node,
        node_rx: crossbeam::channel::Receiver<raft::Message>,
        client_rx: raft::ClientReceiver,
        peers_rx: crossbeam::channel::Receiver<raft::Message>,
        mut peers_tx: HashMap<raft::NodeID, crossbeam::channel::Sender<raft::Message>>,
    ) -> Result<()> {
        let ticker = crossbeam::channel::tick(raft::TICK_INTERVAL);
        let mut requests =
            HashMap::<Vec<u8>, crossbeam::channel::Sender<Result<raft::Response>>>::new();
        loop {
            crossbeam::select! {
                recv(ticker) -> _ => node = node.tick()?,

                recv(peers_rx) -> msg => node = node.step(msg?)?,

                recv(node_rx) -> msg => {
                    let msg = msg?;
                    match msg {
                        // FIXME: move broadcast into raft, do request/response differently.
                        raft::Message{to: raft::Address::Node(to), ..} => {
                            peers_tx.get_mut(&to).expect("Unknown peer").send(msg)?;
                        },
                        raft::Message{to: raft::Address::Broadcast, ..} => {
                            for (to, peer_tx) in peers_tx.iter() {
                                let mut msg = msg.clone();
                                msg.to = raft::Address::Node(*to);
                                peer_tx.send(msg)?;
                            }
                        },
                        raft::Message{to: raft::Address::Client, event: raft::Event::ClientResponse{ id, response }, ..} => {
                            if let Some(response_tx) = requests.remove(&id) {
                                response_tx
                                    .send(response)
                                    .map_err(|e| Error::Internal(format!("Failed to send response {:?}", e)))?;
                            }
                        }
                        _ => return Err(Error::Internal(format!("Unexpected message {:?}", msg))),
                    }
                }

                recv(client_rx) -> r => {
                    let (request, response_tx) = r?;
                    let id = uuid::Uuid::new_v4().as_bytes().to_vec();
                    let msg = raft::Message{
                        from: raft::Address::Client,
                        to: raft::Address::Node(node.id()),
                        term: 0,
                        event: raft::Event::ClientRequest{id: id.clone(), request},
                    };
                    node = node.step(msg)?;
                    requests.insert(id, response_tx);
                }
            }
        }
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
