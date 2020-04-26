use super::{Client, Event, Log, Message, Node, Request, Response, State};
use crate::kv::storage::Storage;
use crate::Error;

use futures::{sink::SinkExt as _, FutureExt as _};
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt as _;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

/// The duration of a Raft tick, the unit of time for e.g. heartbeats and elections.
const TICK: Duration = Duration::from_millis(100);

/// A Raft server.
pub struct Server<L: Storage, S: State> {
    /// The local Raft node.
    node: Node<L, S>,
    /// Raft peers (node ID and network address).
    peers: HashMap<String, String>,
    /// Outbound messages from local node.
    node_rx: mpsc::UnboundedReceiver<Message>,
    /// Clients send requests via this endpoint.
    client_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Response>)>,
    /// Cluster receives client requests via this endpoint.
    client_rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Response>)>,
}

impl<L: Storage + Send + 'static, S: State + Send + 'static> Server<L, S> {
    /// Creates a new Raft cluster
    pub fn new(
        id: &str,
        peers: HashMap<String, String>,
        log: Log<L>,
        state: S,
    ) -> Result<Self, Error> {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        Ok(Self {
            node: Node::new(
                id,
                peers.iter().map(|(k, _)| k.to_string()).collect(),
                log,
                state,
                node_tx,
            )?,
            peers,
            node_rx,
            client_tx,
            client_rx,
        })
    }

    /// Returns a client for this Raft cluster.
    pub async fn client(&self) -> Client {
        Client::new(self.client_tx.clone()).await
    }

    /// Connects to peers and serves requests.
    pub async fn serve(self, listener: TcpListener) -> Result<(), Error> {
        let (tcp_in_tx, tcp_in_rx) = mpsc::channel::<Message>(100);
        let (tcp_out_tx, tcp_out_rx) = mpsc::channel::<Message>(100);
        let (task, tcp_receiver) = Self::tcp_receive(listener, tcp_in_tx).remote_handle();
        tokio::spawn(task);
        let (task, tcp_sender) = Self::tcp_send(self.peers, tcp_out_rx).remote_handle();
        tokio::spawn(task);
        let (task, eventloop) =
            Self::eventloop(self.node, self.node_rx, self.client_rx, tcp_in_rx, tcp_out_tx)
                .remote_handle();
        tokio::spawn(task);

        tokio::try_join!(tcp_receiver, tcp_sender, eventloop)?;
        Ok(())
    }

    /// Runs the event loop.
    async fn eventloop(
        mut node: Node<L, S>,
        mut node_rx: mpsc::UnboundedReceiver<Message>,
        mut client_rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Response>)>,
        mut tcp_rx: mpsc::Receiver<Message>,
        mut tcp_tx: mpsc::Sender<Message>,
    ) -> Result<(), Error> {
        let mut ticker = tokio::time::interval(TICK);
        let mut requests = HashMap::<Vec<u8>, oneshot::Sender<Response>>::new();
        loop {
            // FIXME Node.step() is blocking
            tokio::select! {
                _ = ticker.tick() => node = node.tick()?,

                Some(msg) = tcp_rx.next() => node = node.step(msg)?,

                Some(msg) = node_rx.next() => {
                    if msg.to.is_some() {
                        tcp_tx.send(msg).await?
                    } else if let Some(call_id) = msg.event.call_id() {
                        if let Some(response_tx) = requests.remove(&call_id) {
                            let response = match msg.event {
                                Event::RespondState{response, ..} => Response::State(response),
                                Event::RespondError{error, ..} => Response::Error(error),
                                e => return Err(Error::Internal(format!("Unexpected Raft response {:?}", e))),
                            };
                            response_tx
                                .send(response)
                                .map_err(|e| Error::Internal(format!("Failed to send response {:?}", e)))?;
                        }
                    }
                }

                Some((request, response_tx)) = client_rx.next() => {
                    if let Request::Status = request {
                        response_tx.send(Response::Status(node.status()?))
                            .map_err(|e| Error::Internal(format!("Failed to send response {:?}", e)))?;
                        continue
                    }
                    let call_id = Uuid::new_v4().as_bytes().to_vec();
                    requests.insert(call_id.clone(), response_tx);
                    let event = match request {
                        Request::Mutate(command) => Event::MutateState{call_id, command},
                        Request::Query(command) => Event::QueryState{call_id, command},
                        Request::Status => panic!("unexpected status request"),
                    };
                    node = node.step(Message{from: None, to: None, term: 0, event})?;
                }
            }
        }
    }

    /// Receives inbound messages from peers via TCP.
    async fn tcp_receive(
        mut listener: TcpListener,
        in_tx: mpsc::Sender<Message>,
    ) -> Result<(), Error> {
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let peer_in_tx = in_tx.clone();
            tokio::spawn(async move {
                debug!("Raft peer {} connected", peer);
                match Self::tcp_receive_peer(socket, peer_in_tx).await {
                    Ok(()) => debug!("Raft peer {} disconnected", peer),
                    Err(err) => error!("Raft peer {} error: {}", peer, err.to_string()),
                };
            });
        }
        Ok(())
    }

    /// Receives inbound messages from a peer via TCP.
    async fn tcp_receive_peer(
        socket: TcpStream,
        mut in_tx: mpsc::Sender<Message>,
    ) -> Result<(), Error> {
        let mut stream = tokio_serde::SymmetricallyFramed::<_, Message, _>::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::SymmetricalCbor::<Message>::default(),
        );
        while let Some(message) = stream.try_next().await? {
            in_tx.send(message).await?;
        }
        Ok(())
    }

    /// Sends outbound messages to peers via TCP.
    async fn tcp_send(
        peers: HashMap<String, String>,
        mut out_rx: mpsc::Receiver<Message>,
    ) -> Result<(), Error> {
        let mut peer_txs: HashMap<String, mpsc::Sender<Message>> = HashMap::new();
        for (id, addr) in peers.into_iter() {
            let (tx, rx) = mpsc::channel::<Message>(100);
            peer_txs.insert(id, tx);
            tokio::spawn(Self::tcp_send_peer(addr, rx));
        }
        while let Some(message) = out_rx.next().await {
            match message.to {
                None => error!("Received outbound message with no recipient"),
                Some(ref to) => match peer_txs.get_mut(to) {
                    Some(ref mut tx) => tx.send(message).await?,
                    None => error!("Received outbound message for unknown peer {}", to),
                },
            }
        }
        Ok(())
    }

    /// Sends outbound messages to a peer, continuously reconnecting.
    async fn tcp_send_peer(addr: String, mut out_rx: mpsc::Receiver<Message>) {
        loop {
            match TcpStream::connect(&addr).await {
                Ok(socket) => {
                    debug!("Connected to Raft peer {}", addr);
                    match Self::tcp_send_peer_session(socket, &mut out_rx).await {
                        Ok(()) => break,
                        Err(err) => error!("Failed sending to Raft peer {}: {}", addr, err),
                    }
                }
                Err(err) => error!("Failed to connect to Raft peer {}: {}", addr, err),
            }
            tokio::time::delay_for(Duration::from_millis(1000)).await;
        }
        debug!("Disconnected from Raft peer {}", addr);
    }

    /// Sends outbound messages to a peer via a TCP session.
    async fn tcp_send_peer_session(
        socket: TcpStream,
        out_rx: &mut mpsc::Receiver<Message>,
    ) -> Result<(), Error> {
        let mut stream = tokio_serde::SymmetricallyFramed::<_, Message, _>::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::SymmetricalCbor::<Message>::default(),
        );
        while let Some(message) = out_rx.next().await {
            stream.send(message).await?;
        }
        Ok(())
    }
}
