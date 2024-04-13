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
        /// If false, the follower does not have the entry at commit_index
        /// and would like the leader to replicate it.
        ///
        /// TODO: consider responding with last_index/term instead.
        has_committed: bool,
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
        ///
        /// TODO: should this include last_term as well?
        last_index: Index,
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
