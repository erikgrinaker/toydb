use super::{Address, Event, Log, Message, Node, NodeID, Request, Response, State};
use crate::encoding::bincode;
use crate::error::{Error, Result};

use ::log::{debug, error};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

// TODO: simplify these types.
pub type ClientSender =
    crossbeam::channel::Sender<(Request, crossbeam::channel::Sender<Result<Response>>)>;
pub type ClientReceiver =
    crossbeam::channel::Receiver<(Request, crossbeam::channel::Sender<Result<Response>>)>;

/// The interval between Raft ticks, the unit of time for e.g. heartbeats and
/// elections.
const TICK_INTERVAL: Duration = Duration::from_millis(100);

/// A Raft server.
pub struct Server {
    node: Node,
    peers: HashMap<NodeID, String>,
    node_rx: crossbeam::channel::Receiver<Message>,
}

impl Server {
    /// Creates a new Raft cluster
    pub fn new(
        id: NodeID,
        peers: HashMap<NodeID, String>,
        log: Log,
        state: Box<dyn State>,
    ) -> Result<Self> {
        let (node_tx, node_rx) = crossbeam::channel::unbounded();
        Ok(Self {
            node: Node::new(id, peers.keys().copied().collect(), log, state, node_tx)?,
            peers,
            node_rx,
        })
    }

    /// Connects to peers and serves requests.
    pub fn serve(self, listener: std::net::TcpListener, client_rx: ClientReceiver) -> Result<()> {
        std::thread::scope(|s| {
            let (tcp_in_tx, tcp_in_rx) = crossbeam::channel::unbounded::<Message>();
            let (tcp_out_tx, tcp_out_rx) = crossbeam::channel::unbounded::<Message>();

            s.spawn(move || Self::tcp_receive(listener, tcp_in_tx));
            s.spawn(move || Self::tcp_send(self.peers, tcp_out_rx));
            s.spawn(move || {
                Self::eventloop(self.node, self.node_rx, client_rx, tcp_in_rx, tcp_out_tx)
                    .expect("event processing failed")
            });
            Ok(())
        })
    }

    /// Runs the event loop.
    fn eventloop(
        mut node: Node,
        node_rx: crossbeam::channel::Receiver<Message>,
        client_rx: ClientReceiver,
        tcp_rx: crossbeam::channel::Receiver<Message>,
        tcp_tx: crossbeam::channel::Sender<Message>,
    ) -> Result<()> {
        let ticker = crossbeam::channel::tick(TICK_INTERVAL);
        let mut requests = HashMap::<Vec<u8>, crossbeam::channel::Sender<Result<Response>>>::new();
        loop {
            crossbeam::select! {
                recv(ticker) -> _ => node = node.tick()?,

                recv(tcp_rx) -> msg => node = node.step(msg?)?,

                recv(node_rx) -> msg => {
                    let msg = msg?;
                    match msg {
                        Message{to: Address::Node(_), ..} => tcp_tx.send(msg)?,
                        Message{to: Address::Broadcast, ..} => tcp_tx.send(msg)?,
                        Message{to: Address::Client, event: Event::ClientResponse{ id, response }, ..} => {
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
                    let id = Uuid::new_v4().as_bytes().to_vec();
                    let msg = Message{
                        from: Address::Client,
                        to: Address::Node(node.id()),
                        term: 0,
                        event: Event::ClientRequest{id: id.clone(), request},
                    };
                    node = node.step(msg)?;
                    requests.insert(id, response_tx);
                }
            }
        }
    }

    /// Receives inbound messages from peers via TCP.
    fn tcp_receive(listener: std::net::TcpListener, in_tx: crossbeam::channel::Sender<Message>) {
        std::thread::scope(|s| loop {
            let (socket, peer) = match listener.accept() {
                Ok(r) => r,
                Err(err) => {
                    error!("Connection failed: {}", err);
                    continue;
                }
            };
            let in_tx = in_tx.clone();
            s.spawn(move || {
                debug!("Raft peer {peer} connected");
                match Self::tcp_receive_peer(socket, in_tx) {
                    Ok(()) => debug!("Raft peer {peer} disconnected"),
                    Err(err) => error!("Raft peer {peer} error: {err}"),
                }
            });
        });
    }

    /// Receives inbound messages from a peer via TCP.
    fn tcp_receive_peer(
        mut socket: std::net::TcpStream,
        in_tx: crossbeam::channel::Sender<Message>,
    ) -> Result<()> {
        while let Some(message) = bincode::maybe_deserialize_from(&mut socket)? {
            in_tx.send(message)?;
        }
        Ok(())
    }

    /// Sends outbound messages to peers via TCP.
    fn tcp_send(peers: HashMap<NodeID, String>, out_rx: crossbeam::channel::Receiver<Message>) {
        std::thread::scope(move |s| {
            let mut peer_txs: HashMap<NodeID, crossbeam::channel::Sender<Message>> = HashMap::new();

            for (id, addr) in peers.into_iter() {
                let (tx, rx) = crossbeam::channel::bounded::<Message>(1000);
                peer_txs.insert(id, tx);
                s.spawn(move || Self::tcp_send_peer(addr, rx));
            }

            while let Ok(message) = out_rx.recv() {
                let to = match message.to {
                    Address::Broadcast => peer_txs.keys().copied().collect(),
                    Address::Node(peer) => vec![peer],
                    addr => {
                        error!("Received outbound message for non-TCP address {:?}", addr);
                        continue;
                    }
                };
                for id in to {
                    if let Some(tx) = peer_txs.get_mut(&id) {
                        if tx.try_send(message.clone()).is_err() {
                            error!("Full send buffer for peer {}, discarding message", id)
                        }
                    } else {
                        error!("Received outbound message for unknown peer {}", id)
                    }
                }
            }
        });
    }

    /// Sends outbound messages to a peer, continuously reconnecting.
    fn tcp_send_peer(addr: String, mut out_rx: crossbeam::channel::Receiver<Message>) {
        loop {
            match std::net::TcpStream::connect(&addr) {
                Ok(socket) => {
                    debug!("Connected to Raft peer {}", addr);
                    match Self::tcp_send_peer_session(socket, &mut out_rx) {
                        Ok(()) => break,
                        Err(err) => error!("Failed sending to Raft peer {}: {}", addr, err),
                    }
                }
                Err(err) => error!("Failed connecting to Raft peer {}: {}", addr, err),
            }
            std::thread::sleep(Duration::from_millis(1000));
        }
        debug!("Disconnected from Raft peer {}", addr);
    }

    /// Sends outbound messages to a peer via a TCP session.
    fn tcp_send_peer_session(
        mut socket: std::net::TcpStream,
        out_rx: &mut crossbeam::channel::Receiver<Message>,
    ) -> Result<()> {
        while let Ok(message) = out_rx.recv() {
            bincode::serialize_into(&mut socket, &message)?;
        }
        Ok(())
    }
}
