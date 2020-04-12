use crate::raft::{Message, Transport};
use crate::service;
use crate::service::Raft;
use crate::utility::{deserialize, serialize};
use crate::Error;
use crossbeam::channel::{Receiver, Sender};
use grpc::ClientStubExt;
use std::collections::HashMap;

/// A gRPC transport.
pub struct GRPC {
    /// The node channel receiver
    node_rx: Receiver<Message>,
    /// The node channel sender
    node_tx: Sender<Message>,
    /// A hash map of peer IDs and gRPC clients.
    peers: HashMap<String, service::RaftClient>,
}

impl Transport for GRPC {
    fn receiver(&self) -> Receiver<Message> {
        self.node_rx.clone()
    }

    fn send(&self, msg: Message) -> Result<(), Error> {
        if let Some(to) = &msg.to {
            if let Some(client) = self.peers.get(to) {
                // FIXME Needs to check the response.
                client.step(grpc::RequestOptions::new(), message_to_protobuf(msg));
                Ok(())
            } else {
                Err(Error::Internal(format!("Unknown Raft peer {}", to)))
            }
        } else {
            Err(Error::Internal("No receiver".into()))
        }
    }
}

impl GRPC {
    /// Creates a new GRPC transport
    pub fn new(peers: HashMap<String, std::net::SocketAddr>) -> Result<Self, Error> {
        let (node_tx, node_rx) = crossbeam::channel::unbounded();
        let mut t = GRPC { peers: HashMap::new(), node_tx, node_rx };
        for (id, addr) in peers.into_iter() {
            t.peers.insert(id, t.build_client(addr)?);
        }
        Ok(t)
    }

    /// Builds a gRPC client for a peer.
    pub fn build_client(&self, addr: std::net::SocketAddr) -> Result<service::RaftClient, Error> {
        Ok(service::RaftClient::new_plain(
            &addr.ip().to_string(),
            addr.port(),
            grpc::ClientConf::new(),
        )?)
    }

    /// Builds a gRPC service for a local server.
    pub fn build_service(&self) -> Result<impl service::Raft, Error> {
        Ok(GRPCService { local: self.node_tx.clone() })
    }
}

/// A gRPC service for a local server.
struct GRPCService {
    local: Sender<Message>,
}

impl service::Raft for GRPCService {
    fn step(
        &self,
        _: grpc::RequestOptions,
        pb: service::Message,
    ) -> grpc::SingleResponse<service::Empty> {
        self.local.send(message_from_protobuf(pb).unwrap()).unwrap();
        grpc::SingleResponse::completed(service::Empty::new())
    }
}

/// Converts a Protobuf message to a `Message`.
fn message_from_protobuf(pb: service::Message) -> Result<Message, Error> {
    Ok(Message {
        term: pb.term,
        from: Some(pb.from),
        to: Some(pb.to),
        event: deserialize(&pb.event)?,
    })
}

/// Converts a `Message` to its Protobuf representation.
fn message_to_protobuf(msg: Message) -> service::Message {
    service::Message {
        term: msg.term,
        from: msg.from.unwrap(),
        to: msg.to.unwrap(),
        event: serialize(&msg.event).unwrap(),
        ..Default::default()
    }
}
