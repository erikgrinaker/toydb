use super::{Entry, Index, NodeID, Status, Term};
use crate::error::Result;

use serde_derive::{Deserialize, Serialize};

/// A message address.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Address {
    /// Broadcast to all peers. Only valid as an outbound recipient (to).
    Broadcast,
    /// A node with the specified node ID (local or remote). Valid both as
    /// sender and recipient.
    Node(NodeID),
    /// A local client. Can only send ClientRequest messages, and receive
    /// ClientResponse messages.
    Client,
}

impl Address {
    /// Unwraps the node ID, or panics if address is not of kind Node.
    pub fn unwrap(&self) -> NodeID {
        match self {
            Self::Node(id) => *id,
            _ => panic!("unwrap called on non-Node address {:?}", self),
        }
    }
}

/// A message passed between Raft nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// The current term of the sender. Must be set, unless the sender is
    /// Address::Client, in which case it must be 0.
    pub term: Term,
    /// The sender address.
    pub from: Address,
    /// The recipient address.
    pub to: Address,
    /// The message payload.
    pub event: Event,
}

/// An event contained within messages.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Event {
    /// Leaders send periodic heartbeats to its followers.
    Heartbeat {
        /// The index of the leader's last committed log entry.
        commit_index: Index,
        /// The term of the leader's last committed log entry.
        commit_term: Term,
    },
    /// Followers confirm loyalty to leader after heartbeats.
    ConfirmLeader {
        /// The commit_index of the original leader heartbeat, to confirm
        /// read requests.
        commit_index: Index,
        /// If false, the follower does not have the entry at commit_index
        /// and would like the leader to replicate it.
        has_committed: bool,
    },

    /// Candidates solicit votes from all peers when campaigning for leadership.
    SolicitVote {
        // The index of the candidate's last stored log entry
        last_index: Index,
        // The term of the candidate's last stored log entry
        last_term: Term,
    },

    /// Followers may grant a single vote to a candidate per term, on a
    /// first-come basis. Candidates implicitly vote for themselves.
    GrantVote,

    /// Leaders replicate log entries to followers by appending it to their log.
    AppendEntries {
        /// The index of the log entry immediately preceding the submitted commands.
        base_index: Index,
        /// The term of the log entry immediately preceding the submitted commands.
        base_term: Term,
        /// Commands to replicate.
        entries: Vec<Entry>,
    },
    /// Followers may accept a set of log entries from a leader.
    AcceptEntries {
        /// The index of the last log entry.
        last_index: Index,
    },
    /// Followers may also reject a set of log entries from a leader.
    RejectEntries,

    /// A client request. This can be submitted to the leader, or to a follower
    /// which will forward it to its leader. If there is no leader, or the
    /// leader or term changes, the request is aborted with an Error::Abort
    /// ClientResponse and the client must retry.
    ClientRequest {
        /// The request ID. This is arbitrary, but must be globally unique for
        /// the duration of the request.
        id: RequestID,
        /// The request.
        request: Request,
    },

    /// A client response.
    ClientResponse {
        /// The response ID. This matches the ID of the ClientRequest.
        id: RequestID,
        /// The response, or an error.
        response: Result<Response>,
    },
}

/// A client request ID.
pub type RequestID = Vec<u8>;

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
    Query(Vec<u8>),
    Mutate(Vec<u8>),
    Status(Status),
}
