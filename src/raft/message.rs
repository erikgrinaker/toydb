use super::{Entry, Status};
use crate::error::Result;

use serde_derive::{Deserialize, Serialize};

/// A message address.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Address {
    /// Broadcast to all peers.
    Peers,
    /// A remote peer.
    Peer(String),
    /// The local node.
    Local,
    /// A local client.
    Client,
}

/// A message passed between Raft nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// The current term of the sender.
    pub term: u64,
    /// The sender address.
    pub from: Address,
    /// The recipient address.
    pub to: Address,
    /// The message event.
    pub event: Event,
}

/// An event contained within messages.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Event {
    /// Leaders send periodic heartbeats to its followers.
    Heartbeat {
        /// The index of the leader's last committed log entry.
        commit_index: u64,
        /// The term of the leader's last committed log entry.
        commit_term: u64,
    },
    /// Followers confirm loyalty to leader after heartbeats.
    ConfirmLeader {
        /// The commit_index of the original leader heartbeat, to confirm
        /// read requests.
        commit_index: u64,
        /// If false, the follower does not have the entry at commit_index
        /// and would like the leader to replicate it.
        has_committed: bool,
    },
    /// Preventing disruptions when a server rejoins the cluster
    /// see: https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf [9.6]
    PreVote,
    /// Candidates solicit votes from all peers.
    SolicitVote {
        // The index of the candidate's last stored log entry
        last_index: u64,
        // The term of the candidate's last stored log entry
        last_term: u64,
    },
    /// Followers may grant votes to candidates.
    GrantVote {
        pre_vote: bool
    },
    /// Leaders replicate a set of log entries to followers.
    ReplicateEntries {
        /// The index of the log entry immediately preceding the submitted commands.
        base_index: u64,
        /// The term of the log entry immediately preceding the submitted commands.
        base_term: u64,
        /// Commands to replicate.
        entries: Vec<Entry>,
    },
    /// Followers may accept a set of log entries from a leader.
    AcceptEntries {
        /// The index of the last log entry.
        last_index: u64,
    },
    /// Followers may also reject a set of log entries from a leader.
    RejectEntries,
    /// A client request.
    ClientRequest {
        /// The request ID.
        id: Vec<u8>,
        /// The request.
        request: Request,
    },
    /// A client response.
    ClientResponse {
        /// The response ID.
        id: Vec<u8>,
        /// The response.
        response: Result<Response>,
    },
}

/// A client request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Request {
    Query(Vec<u8>),
    Mutate(Vec<u8>),
    Status,
}

/// A client response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    State(Vec<u8>),
    Status(Status),
}
