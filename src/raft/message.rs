use super::{Entry, Index, NodeID, Term};
use crate::error::Result;
use crate::storage;

use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;

/// A message envelope sent between Raft nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    /// The sender.
    pub from: NodeID,
    /// The recipient.
    pub to: NodeID,
    /// The sender's current term.
    pub term: Term,
    /// The message.
    pub message: Message,
}

/// A message sent between Raft nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// Leaders send periodic heartbeats to its followers.
    Heartbeat {
        /// The index of the leader's last committed log entry.
        commit_index: Index,
        /// The term of the leader's last committed log entry.
        commit_term: Term,
        /// The latest read sequence number of the leader.
        read_seq: ReadSequence,
    },

    /// Followers confirm leader heartbeats.
    HeartbeatResponse {
        /// The index of the follower's last log entry.
        last_index: Index,
        /// The term of the follower's last log entry.
        last_term: Term,
        /// The read sequence number of the heartbeat we're responding to.
        read_seq: ReadSequence,
    },

    /// Candidates campaign for leadership by soliciting votes from peers.
    Campaign {
        /// The index of the candidate's last stored log entry
        last_index: Index,
        /// The term of the candidate's last stored log entry
        last_term: Term,
    },

    /// Followers may vote for a single candidate per term, on a first-come
    /// first-serve basis. Candidates implicitly vote for themselves.
    CampaignResponse {
        /// If true, the sender granted a vote for the candidate.
        vote: bool,
    },

    /// Leaders replicate log entries to followers by appending to their logs.
    Append {
        /// The index of the log entry immediately preceding the submitted commands.
        base_index: Index,
        /// The term of the log entry immediately preceding the submitted commands.
        base_term: Term,
        /// Commands to replicate.
        entries: Vec<Entry>,
    },

    /// Followers may accept or reject appending entries from the leader.
    AppendResponse {
        /// If true, the follower rejected the leader's entries.
        reject: bool,
        /// The index of the follower's last log entry.
        last_index: Index,
        /// The term of the follower's last log entry.
        last_term: Term,
    },

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

/// A read sequence number, used to confirm leadership for linearizable reads.
pub type ReadSequence = u64;

/// A client request, typically passed through to the state machine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Request {
    /// A state machine read command. This is not replicated, and only evaluted
    /// on the leader.
    Read(Vec<u8>),
    /// A state machine write command. This is replicated across all nodes, and
    /// must result in a deterministic response.
    Write(Vec<u8>),
    /// Requests Raft cluster status from the leader.
    Status,
}

/// A client response. This will be wrapped in a Result to handle errors.
///
/// TODO: consider a separate error kind here, or a wrapped Result, to separate
/// fallible state machine operations (returned to the caller) from apply errors
/// (fatal).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    /// A state machine read result.
    Read(Vec<u8>),
    /// A state machine write result.
    Write(Vec<u8>),
    /// The current Raft leader status.
    Status(Status),
}

/// Raft cluster status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// The current Raft leader, which generated this status.
    pub leader: NodeID,
    /// The current Raft term.
    pub term: Term,
    /// The last log indexes of all nodes.
    pub last_index: HashMap<NodeID, Index>,
    /// The current commit index.
    pub commit_index: Index,
    /// The current applied index.
    pub apply_index: Index,
    /// The log storage engine status.
    pub storage: storage::engine::Status,
}
