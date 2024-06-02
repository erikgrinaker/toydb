use super::{Entry, Index, NodeID, Term};
use crate::encoding;
use crate::error::Result;
use crate::storage;

use serde_derive::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

impl encoding::Value for Envelope {}

/// A message sent between Raft nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// Leaders send heartbeats periodically, and on client read requests. This
    /// serves several purposes:
    ///
    /// * Inform nodes about the leader, and prevent elections.
    /// * Probe follower log progress.
    /// * Advance followers' commit indexes, so they can apply entries.
    /// * Confirm leadership for client reads, to avoid stale reads.
    ///
    /// While some of this could be split out to other messages, the heartbeat
    /// periodicity also implicitly provides retries, so it is convenient to
    /// piggyback on it.
    ///
    /// The Raft paper does not have a distinct heartbeat message, and instead
    /// uses an empty AppendEntries RPC, but we choose to add one for better
    /// separation of concerns.
    Heartbeat {
        /// The index of the leader's last committed log entry.
        commit_index: Index,
        /// The term of the leader's last committed log entry.
        ///
        /// The Raft paper does not propagate this, because it uses the
        /// AppendEntries RPC for heartbeats and commit index propagation, which
        /// includes a base index/term check guaranteeing that the commit index
        /// is consistent with the leader. We need it to ensure a divergent
        /// follower doesn't commit a stale entry from a different term.
        commit_term: Term,
        /// The leader's latest read sequence number in this term. Read requests
        /// are served once the sequence number has been confirmed by a quorum.
        read_seq: ReadSequence,
    },

    /// Followers respond to leader heartbeats if they still consider it leader.
    HeartbeatResponse {
        /// The index of the follower's last log entry.
        last_index: Index,
        /// The term of the follower's last log entry.
        last_term: Term,
        /// The heartbeat's read sequence number.
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

    /// Leaders replicate log entries to followers by appending to their logs
    /// after the given base entry. If the base entry matches the follower's log
    /// then their logs are identical up to it (see section 5.3 in the Raft
    /// paper), and the entries can be appended. Otherwise, the append is
    /// rejected and the leader must retry an earlier base index until a common
    /// base is found.
    Append {
        /// The index of the log entry to append after.
        base_index: Index,
        /// The term of the log entry to append after.
        base_term: Term,
        /// Log entries to append.
        entries: Vec<Entry>,
    },

    /// Followers accept or reject appends from the leader depending on whether
    /// the base entry matches in both logs.
    AppendResponse {
        /// If non-zero, the follower rejected an append at this base index
        /// because the base index/term did not match its log.
        reject_index: Index,
        /// The index of the follower's last log entry.
        ///
        /// NB: if the entries already existed in the follower's log, the append
        /// is a noop and the last_index may be greater than the latest entry in
        /// the append. That's fine -- if we're still the leader then our logs
        /// must be consistent, otherwise the follower will have stopped
        /// accepting our appends anyway.
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

impl encoding::Value for Request {}

/// A client response. This will be wrapped in a Result to handle errors.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    /// A state machine read result.
    Read(Vec<u8>),
    /// A state machine write result.
    Write(Vec<u8>),
    /// The current Raft leader status.
    Status(Status),
}

impl encoding::Value for Response {}

/// Raft cluster status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// The current Raft leader, which generated this status.
    pub leader: NodeID,
    /// The current Raft term.
    pub term: Term,
    /// The match indexes of all nodes. Use a BTreeMap for deterministic
    /// debug output.
    pub match_index: BTreeMap<NodeID, Index>,
    /// The current commit index.
    pub commit_index: Index,
    /// The current applied index.
    pub apply_index: Index,
    /// The log storage engine status.
    pub storage: storage::engine::Status,
}
