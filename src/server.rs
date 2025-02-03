use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Write as _};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::time::Duration;

use crossbeam::channel::{Receiver, Sender};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::encoding::{self, Value as _};
use crate::error::Result;
use crate::raft;
use crate::sql;
use crate::sql::engine::{Catalog as _, Engine as _, StatementResult};
use crate::sql::types::{Row, Table};
use crate::storage;

/// The outbound Raft peer channel capacity. This buffers messages when a Raft
/// peer is slow or unavailable. Beyond this, messages will be dropped.
const RAFT_PEER_CHANNEL_CAPACITY: usize = 1000;

/// The retry interval when connecting to a Raft peer.
const RAFT_PEER_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// A toyDB server. Routes messages to/from an inner Raft node.
///
/// * Listens for inbound SQL connections from clients via TCP and passes
///   requests to the local Raft node.
///
/// * Listens for inbound Raft connections from other toyDB nodes via TCP and
///   passes messages to the local Raft node.
///
/// * Connects to other toyDB nodes via TCP and sends outbound Raft messages
///   from the local Raft node.
pub struct Server {
    /// The inner Raft node.
    node: raft::Node,
    /// Outbound messages from the Raft node.
    node_rx: Receiver<raft::Envelope>,
    /// Raft peer IDs and addresses.
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
        let node = raft::Node::new(
            id,
            peers.keys().copied().collect(),
            raft_log,
            raft_state,
            node_tx,
            raft::Options::default(),
        )?;
        Ok(Self { node, peers, node_rx })
    }

    /// Serves Raft and SQL requests indefinitely. Consumes the server.
    pub fn serve(self, raft_addr: impl ToSocketAddrs, sql_addr: impl ToSocketAddrs) -> Result<()> {
        let raft_listener = TcpListener::bind(raft_addr)?;
        let sql_listener = TcpListener::bind(sql_addr)?;
        info!(
            "Listening on {} (SQL) and {} (Raft)",
            sql_listener.local_addr()?,
            raft_listener.local_addr()?
        );

        std::thread::scope(move |s| {
            let id = self.node.id();
            let (raft_request_tx, raft_request_rx) = crossbeam::channel::unbounded();
            let (raft_step_tx, raft_step_rx) = crossbeam::channel::unbounded();

            // Serve inbound Raft connections.
            s.spawn(move || Self::raft_accept(raft_listener, raft_step_tx));

            // Establish outbound Raft connections to peers.
            let mut raft_peers_tx = HashMap::new();
            for (id, addr) in self.peers.into_iter() {
                let (raft_peer_tx, raft_peer_rx) =
                    crossbeam::channel::bounded(RAFT_PEER_CHANNEL_CAPACITY);
                raft_peers_tx.insert(id, raft_peer_tx);
                s.spawn(move || Self::raft_send_peer(addr, raft_peer_rx));
            }

            // Route Raft messages between the local node, peers, and clients.
            s.spawn(move || {
                Self::raft_route(
                    self.node,
                    self.node_rx,
                    raft_step_rx,
                    raft_peers_tx,
                    raft_request_rx,
                )
            });

            // Serve inbound SQL connections.
            let sql_engine = sql::engine::Raft::new(raft_request_tx);
            s.spawn(move || Self::sql_accept(id, sql_listener, sql_engine));
        });

        Ok(())
    }

    /// Accepts new inbound Raft connections from peers and spawns threads
    /// routing inbound messages to the local Raft node.
    fn raft_accept(listener: TcpListener, raft_step_tx: Sender<raft::Envelope>) {
        std::thread::scope(|s| loop {
            let (socket, peer) = match listener.accept() {
                Ok((socket, peer)) => (socket, peer),
                Err(err) => {
                    error!("Raft peer accept failed: {err}");
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
    fn raft_receive_peer(socket: TcpStream, raft_step_tx: Sender<raft::Envelope>) -> Result<()> {
        let mut socket = BufReader::new(socket);
        while let Some(message) = raft::Envelope::maybe_decode_from(&mut socket)? {
            raft_step_tx.send(message)?;
        }
        Ok(())
    }

    /// Sends outbound messages to a peer via TCP. Retries indefinitely if the
    /// connection fails.
    fn raft_send_peer(addr: String, raft_node_rx: Receiver<raft::Envelope>) {
        loop {
            let mut socket = match TcpStream::connect(&addr) {
                Ok(socket) => BufWriter::new(socket),
                Err(err) => {
                    error!("Failed connecting to Raft peer {addr}: {err}");
                    std::thread::sleep(RAFT_PEER_RETRY_INTERVAL);
                    continue;
                }
            };
            while let Ok(message) = raft_node_rx.recv() {
                if let Err(err) = message.encode_into(&mut socket).and_then(|_| Ok(socket.flush()?))
                {
                    error!("Failed sending to Raft peer {addr}: {err}");
                    break;
                }
            }
            debug!("Disconnected from Raft peer {addr}");
        }
    }

    /// Routes Raft messages:
    ///
    /// * node_rx: outbound messages from the local Raft node. Routed to peers
    ///   via TCP, or to local clients via a response channel.
    ///
    /// * request_rx: inbound requests from local SQL clients. Stepped into
    ///   the local Raft node as ClientRequest messages. Responses are returned
    ///   via the provided response channel.
    ///
    /// * peers_rx: inbound messages from remote Raft peers. Stepped into the
    ///   local Raft node.
    ///
    /// * peers_tx: outbound per-peer channels sent via TCP connections.
    ///   Messages from the local node's node_rx are sent here.
    ///
    /// Panics on any errors, since the Raft node can't recover from failed
    /// state transitions.
    fn raft_route(
        mut node: raft::Node,
        node_rx: Receiver<raft::Envelope>,
        peers_rx: Receiver<raft::Envelope>,
        mut peers_tx: HashMap<raft::NodeID, Sender<raft::Envelope>>,
        request_rx: Receiver<(raft::Request, Sender<Result<raft::Response>>)>,
    ) {
        // Track response channels by request ID. The Raft node will emit
        // ClientResponse messages that we forward to the response channel.
        let mut response_txs = HashMap::<raft::RequestID, Sender<Result<raft::Response>>>::new();

        let ticker = crossbeam::channel::tick(raft::TICK_INTERVAL);
        loop {
            crossbeam::select! {
                // Periodically tick the node.
                recv(ticker) -> _ => node = node.tick().expect("tick failed"),

                // Step messages from peers into the node.
                recv(peers_rx) -> result => {
                    let msg = result.expect("peers_rx disconnected");
                    node = node.step(msg).expect("step failed");
                },

                // Send outbound messages from the node to the appropriate peer.
                // If we receive a client response addressed to the local node,
                // forward it to the waiting client via the response channel.
                recv(node_rx) -> result => {
                    let msg = result.expect("node_rx disconnected");
                    if msg.to == node.id() {
                        if let raft::Message::ClientResponse{ id, response } = msg.message {
                            if let Some(response_tx) = response_txs.remove(&id) {
                                response_tx.send(response).expect("response_tx disconnected");
                            }
                            continue
                        }
                    }
                    let peer_tx = peers_tx.get_mut(&msg.to).expect("unknown peer");
                    match peer_tx.try_send(msg) {
                        Ok(()) => {},
                        Err(crossbeam::channel::TrySendError::Full(_)) => {
                            error!("Raft peer channel full, dropping message");
                        },
                        Err(crossbeam::channel::TrySendError::Disconnected(_)) => {
                            panic!("peer_tx disconnected");
                        },
                    };
                }

                // Track inbound client requests and step them into the node.
                recv(request_rx) -> result => {
                    let (request, response_tx) = result.expect("request_rx disconnected");
                    let id = Uuid::new_v4();
                    let msg = raft::Envelope{
                        from: node.id(),
                        to: node.id(),
                        term: node.term(),
                        message: raft::Message::ClientRequest{id, request},
                    };
                    node = node.step(msg).expect("step failed");
                    response_txs.insert(id, response_tx);
                }
            }
        }
    }

    /// Accepts new SQL client connections and spawns session threads for them.
    fn sql_accept(id: raft::NodeID, listener: TcpListener, sql_engine: sql::engine::Raft) {
        std::thread::scope(|s| loop {
            let (socket, peer) = match listener.accept() {
                Ok((socket, peer)) => (socket, peer),
                Err(err) => {
                    error!("Client accept failed: {err}");
                    continue;
                }
            };
            let session = sql_engine.session();
            s.spawn(move || {
                debug!("Client {peer} connected");
                match Self::sql_session(id, socket, session) {
                    Ok(()) => debug!("Client {peer} disconnected"),
                    Err(err) => error!("Client {peer} error: {err}"),
                }
            });
        })
    }

    /// Processes a client SQL session, executing SQL statements against the
    /// Raft node.
    fn sql_session(
        id: raft::NodeID,
        socket: TcpStream,
        mut session: sql::engine::Session<sql::engine::Raft>,
    ) -> Result<()> {
        let mut reader = BufReader::new(socket.try_clone()?);
        let mut writer = BufWriter::new(socket);

        while let Some(request) = Request::maybe_decode_from(&mut reader)? {
            // Execute request.
            debug!("Received request {request:?}");
            let response = match request {
                Request::Execute(query) => session.execute(&query).map(Response::Execute),
                Request::GetTable(table) => {
                    session.with_txn(true, |txn| txn.must_get_table(&table)).map(Response::GetTable)
                }
                Request::ListTables => session
                    .with_txn(true, |txn| {
                        Ok(txn.list_tables()?.into_iter().map(|t| t.name).collect())
                    })
                    .map(Response::ListTables),
                Request::Status => session
                    .status()
                    .map(|s| Status { server: id, raft: s.raft, mvcc: s.mvcc })
                    .map(Response::Status),
            };

            // Process response.
            debug!("Returning response {response:?}");
            response.encode_into(&mut writer)?;
            writer.flush()?;
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

impl encoding::Value for Request {}

/// A SQL server response.
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Execute(StatementResult),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    Status(Status),
}

impl encoding::Value for Response {}

/// SQL server status.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub server: raft::NodeID,
    pub raft: raft::Status,
    pub mvcc: storage::mvcc::Status,
}
