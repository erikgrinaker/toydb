use crate::encoding::bincode;
use crate::error::Result;
use crate::raft;
use crate::sql;
use crate::sql::engine::Engine as _;
use crate::sql::execution::ResultSet;
use crate::sql::schema::{Catalog as _, Table};
use crate::sql::types::Row;

use crossbeam::channel::{Receiver, Sender};
use log::{debug, error, info};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

/// The outbound Raft peer channel capacity. This buffers messages when a Raft
/// peer is slow or unavailable. Beyond this, messages will be dropped.
const RAFT_PEER_CHANNEL_CAPACITY: usize = 1000;

/// The retry interval when connecting to a Raft peer.
const RAFT_PEER_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// A toyDB server. Routes messages to/from an inner Raft node.
///
/// - Listens for inbound Raft connections via TCP and passes messages to the
///   local Raft node.
///
/// - Connects to Raft peers via TCP and sends outbound messages from the
///   local Raft node.
///
/// - Listens for inbound SQL connections via TCP and passes requests to
///   the local Raft node.
pub struct Server {
    /// The inner Raft node.
    node: raft::Node,
    /// Outbound messages from the Raft node.
    node_rx: Receiver<raft::Message>,
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
    pub fn serve(self, raft_addr: impl ToSocketAddrs, sql_addr: impl ToSocketAddrs) -> Result<()> {
        let raft_listener = TcpListener::bind(raft_addr)?;
        let sql_listener = TcpListener::bind(sql_addr)?;
        info!(
            "Listening on {} (SQL) and {} (Raft)",
            sql_listener.local_addr()?,
            raft_listener.local_addr()?
        );

        std::thread::scope(move |s| {
            let (raft_request_tx, raft_request_rx) = crossbeam::channel::unbounded();
            let (raft_step_tx, raft_step_rx) = crossbeam::channel::unbounded();

            // Serve inbound Raft connections.
            s.spawn(move || Self::raft_accept(raft_listener, raft_step_tx));

            // Establish outbound Raft connections.
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
            s.spawn(move || Self::sql_accept(sql_listener, raft_request_tx));
        });

        Ok(())
    }

    /// Accepts new inbound Raft connections from peers and spawns threads
    /// routing inbound messages to the local Raft node.
    fn raft_accept(listener: TcpListener, raft_step_tx: Sender<raft::Message>) {
        std::thread::scope(|s| loop {
            let (socket, peer) = match listener.accept() {
                Ok(sp) => sp,
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
    fn raft_receive_peer(mut socket: TcpStream, raft_step_tx: Sender<raft::Message>) -> Result<()> {
        while let Some(message) = bincode::maybe_deserialize_from(&mut socket)? {
            raft_step_tx.send(message)?;
        }
        Ok(())
    }

    /// Sends outbound messages to a peer via TCP. Retries indefinitely if the
    /// connection fails.
    fn raft_send_peer(addr: String, raft_node_rx: Receiver<raft::Message>) {
        loop {
            let mut socket = match TcpStream::connect(&addr) {
                Ok(s) => s,
                Err(err) => {
                    error!("Failed connecting to Raft peer {addr}: {err}");
                    std::thread::sleep(RAFT_PEER_RETRY_INTERVAL);
                    continue;
                }
            };
            while let Ok(message) = raft_node_rx.recv() {
                if let Err(err) = bincode::serialize_into(&mut socket, &message) {
                    error!("Failed sending to Raft peer {addr}: {err}");
                    break;
                }
            }
            debug!("Disconnected from Raft peer {addr}");
        }
    }

    /// Routes Raft messages:
    ///
    /// - node_rx: outbound messages from the local Raft node. Routed to peers
    ///   via TCP, or to local clients via a response channel.
    ///
    /// - request_rx: inbound requests from local SQL clients. Stepped into
    ///   the local Raft node as ClientRequest messages. Responses are returned
    ///   via the provided response channel.
    ///
    /// - peers_rx: inbound messages from remote Raft peers. Stepped into the
    ///   local Raft node.
    ///
    /// - peers_tx: outbound per-peer channels sent via TCP connections.
    ///   Messages from the local node's node_rx are sent here.
    ///
    /// Panics on any errors, since the Raft node can't recover from failed
    /// state transitions.
    fn raft_route(
        mut node: raft::Node,
        node_rx: Receiver<raft::Message>,
        peers_rx: Receiver<raft::Message>,
        mut peers_tx: HashMap<raft::NodeID, Sender<raft::Message>>,
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
                        if let raft::Event::ClientResponse{ id, response } = msg.event {
                            if let Some(response_tx) = response_txs.remove(&id) {
                                response_tx.send(response).expect("response_tx disconnected");
                            }
                            continue
                        }
                    }
                    let peer_tx = peers_tx.get_mut(&msg.to).expect("unknown peer");
                    peer_tx.send(msg).expect("peer_tx disconnected");
                }

                // Track inbound client requests and step them into the node.
                recv(request_rx) -> result => {
                    let (request, response_tx) = result.expect("request_rx disconnected");
                    let id = uuid::Uuid::new_v4().into_bytes().to_vec();
                    let msg = raft::Message{
                        from: node.id(),
                        to: node.id(),
                        term: node.term(),
                        event: raft::Event::ClientRequest{id: id.clone(), request},
                    };
                    node = node.step(msg).expect("step failed");
                    response_txs.insert(id, response_tx);
                }
            }
        }
    }

    /// Accepts new SQL client connections and spawns session threads for them.
    fn sql_accept(
        listener: TcpListener,
        raft_request_tx: Sender<(raft::Request, Sender<Result<raft::Response>>)>,
    ) {
        std::thread::scope(|s| loop {
            let (socket, peer) = match listener.accept() {
                Ok(sp) => sp,
                Err(err) => {
                    error!("Client accept failed: {err}");
                    continue;
                }
            };
            let raft_request_tx = raft_request_tx.clone();
            s.spawn(move || {
                debug!("Client {peer} connected");
                match Self::sql_session(socket, raft_request_tx) {
                    Ok(()) => debug!("Client {peer} disconnected"),
                    Err(err) => error!("Client {peer} error: {err}"),
                }
            });
        })
    }

    /// Processes a client SQL session, by executing SQL statements against the
    /// Raft node.
    fn sql_session(
        mut socket: TcpStream,
        raft_request_tx: Sender<(raft::Request, Sender<Result<raft::Response>>)>,
    ) -> Result<()> {
        let mut session = sql::engine::Raft::new(raft_request_tx).session();
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
