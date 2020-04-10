use crate::raft::{Entry, Event, Message, Transport};
use crate::service;
use crate::service::Raft;
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
    ) -> grpc::SingleResponse<service::Success> {
        self.local.send(message_from_protobuf(pb).unwrap()).unwrap();
        grpc::SingleResponse::completed(service::Success::new())
    }
}

/// Converts a Protobuf message to a `Message`.
fn message_from_protobuf(pb: service::Message) -> Result<Message, Error> {
    Ok(Message {
        term: pb.term,
        from: Some(pb.from),
        to: Some(pb.to),
        event: match pb.event {
            Some(service::Message_oneof_event::heartbeat(e)) => {
                Event::Heartbeat { commit_index: e.commit_index, commit_term: e.commit_term }
            }
            Some(service::Message_oneof_event::confirm_leader(e)) => Event::ConfirmLeader {
                commit_index: e.commit_index,
                has_committed: e.has_committed,
            },
            Some(service::Message_oneof_event::solicit_vote(e)) => {
                Event::SolicitVote { last_index: e.last_index, last_term: e.last_term }
            }
            Some(service::Message_oneof_event::grant_vote(_)) => Event::GrantVote,
            Some(service::Message_oneof_event::query_state(e)) => {
                Event::QueryState { call_id: e.call_id, command: e.command }
            }
            Some(service::Message_oneof_event::mutate_state(e)) => {
                Event::MutateState { call_id: e.call_id, command: e.command }
            }
            Some(service::Message_oneof_event::respond_state(e)) => {
                Event::RespondState { call_id: e.call_id, response: e.response }
            }
            Some(service::Message_oneof_event::respond_error(e)) => {
                Event::RespondError { call_id: e.call_id, error: e.error }
            }
            Some(service::Message_oneof_event::replicate_entries(e)) => Event::ReplicateEntries {
                base_index: e.base_index,
                base_term: e.base_term,
                entries: e
                    .entries
                    .to_vec()
                    .into_iter()
                    .map(|entry| Entry {
                        term: entry.term,
                        command: if entry.command.is_empty() { None } else { Some(entry.command) },
                    })
                    .collect(),
            },
            Some(service::Message_oneof_event::accept_entries(e)) => {
                Event::AcceptEntries { last_index: e.last_index }
            }
            Some(service::Message_oneof_event::reject_entries(_)) => Event::RejectEntries,
            None => return Err(Error::Internal("No event found in protobuf message".into())),
        },
    })
}

/// Converts a `Message` to its Protobuf representation.
fn message_to_protobuf(msg: Message) -> service::Message {
    service::Message {
        term: msg.term,
        from: msg.from.unwrap(),
        to: msg.to.unwrap(),
        event: Some(match msg.event {
            Event::Heartbeat { commit_index, commit_term } => {
                service::Message_oneof_event::heartbeat(service::Heartbeat {
                    commit_index,
                    commit_term,
                    ..Default::default()
                })
            }
            Event::ConfirmLeader { commit_index, has_committed } => {
                service::Message_oneof_event::confirm_leader(service::ConfirmLeader {
                    commit_index,
                    has_committed,
                    ..Default::default()
                })
            }
            Event::SolicitVote { last_index, last_term } => {
                service::Message_oneof_event::solicit_vote(service::SolicitVote {
                    last_index,
                    last_term,
                    ..Default::default()
                })
            }
            Event::GrantVote => service::Message_oneof_event::grant_vote(service::GrantVote::new()),
            Event::QueryState { call_id, command } => {
                service::Message_oneof_event::query_state(service::QueryState {
                    call_id,
                    command,
                    ..Default::default()
                })
            }
            Event::MutateState { call_id, command } => {
                service::Message_oneof_event::mutate_state(service::MutateState {
                    call_id,
                    command,
                    ..Default::default()
                })
            }
            Event::RespondState { call_id, response } => {
                service::Message_oneof_event::respond_state(service::RespondState {
                    call_id,
                    response,
                    ..Default::default()
                })
            }
            Event::RespondError { call_id, error } => {
                service::Message_oneof_event::respond_error(service::RespondError {
                    call_id,
                    error,
                    ..Default::default()
                })
            }
            Event::ReplicateEntries { base_index, base_term, entries } => {
                service::Message_oneof_event::replicate_entries(service::ReplicateEntries {
                    base_index,
                    base_term,
                    entries: protobuf::RepeatedField::from_vec(
                        entries
                            .into_iter()
                            .map(|entry| service::Entry {
                                term: entry.term,
                                command: if let Some(bytes) = entry.command {
                                    bytes
                                } else {
                                    vec![]
                                },
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    ..Default::default()
                })
            }
            Event::AcceptEntries { last_index } => {
                service::Message_oneof_event::accept_entries(service::AcceptEntries {
                    last_index,
                    ..Default::default()
                })
            }
            Event::RejectEntries => {
                service::Message_oneof_event::reject_entries(service::RejectEntries::new())
            }
        }),
        ..Default::default()
    }
}
