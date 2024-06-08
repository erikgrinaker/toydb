use super::{Entry, Index, NodeID, Term};
use crate::encoding;
use crate::error::Result;
use crate::storage;

use serde_derive::{Deserialize, Serialize};

/// A message envelope specifying the sender and receiver.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    /// The sender.
    pub from: NodeID,
    /// The sender's current term.
    pub term: Term,
    /// The recipient.
    pub to: NodeID,
    /// The message.
    pub message: Message,
}

impl encoding::Value for Envelope {}

/// A message sent between Raft nodes. Messages are sent asynchronously (i.e.
/// they are not request/response) and may be dropped or reordered.
///
/// In practice, they are sent across a TCP connection and crossbeam channels
/// ensuring messages are not dropped or reordered as long as the connection
/// remains intact. A message and its response are sent across separate TCP
/// connections (outbound from their respective senders).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// Candidates campaign for leadership by soliciting votes from peers.
    /// Votes will only be granted if the candidate's log is at least as
    /// up-to-date as the voter.
    Campaign {
        /// The index of the candidate's last log entry.
        last_index: Index,
        /// The term of the candidate's last log entry.
        last_term: Term,
    },

    /// Followers may vote for a single candidate per term, but only if the
    /// candidate's log is at least as up-to-date as the follower. Candidates
    /// implicitly vote for themselves.
    CampaignResponse {
        /// If true, the follower granted the candidate a vote. A false response
        /// isn't necessary, but is emitted for clarity.
        vote: bool,
    },

    /// Leaders send heartbeats periodically, and on client read requests. This
    /// serves several purposes:
    ///
    /// * Inform nodes about the leader, and prevent elections.
    /// * Probe follower log progress, and trigger append/ack retries.
    /// * Advance followers' commit indexes, so they can apply entries.
    /// * Confirm leadership for client reads, to avoid stale reads.
    ///
    /// While some of this could be split out to other messages, the heartbeat
    /// periodicity implicitly provides retries, so it is convenient to
    /// piggyback on it.
    ///
    /// The Raft paper does not have a distinct heartbeat message, and instead
    /// uses an empty AppendEntries RPC, but we choose to add one for better
    /// separation of concerns.
    Heartbeat {
        /// The index of the leader's last log entry. The term is the leader's
        /// current term, since it appends a noop entry on election win. The
        /// follower compares this to its own log to determine if it's
        /// up-to-date.
        last_index: Index,
        /// The index of the leader's last committed log entry. Followers use
        /// this to advance their commit index and apply entries. It's only safe
        /// to commit this if the local log matches last_index, such that the
        /// follower's log is identical to the leader at the commit index.
        commit_index: Index,
        /// The leader's latest read sequence number in this term. Read requests
        /// are served once the sequence number has been confirmed by a quorum.
        read_seq: ReadSequence,
    },

    /// Followers respond to leader heartbeats if they still consider it leader.
    HeartbeatResponse {
        /// If non-zero, the heartbeat's last_index which was matched in the
        /// follower's log. Otherwise, the follower is either divergent or
        /// lagging behind the leader.
        match_index: Index,
        /// The heartbeat's read sequence number.
        read_seq: ReadSequence,
    },

    /// Leaders replicate log entries to followers by appending to their logs
    /// after the given base entry.
    ///
    /// If the base entry matches the follower's log then their logs are
    /// identical up to it (see section 5.3 in the Raft paper), and the entries
    /// can be appended -- possibly replacing conflicting entries. Otherwise,
    /// the append is rejected and the leader must retry an earlier base index
    /// until a common base is found.
    ///
    /// Empty appends messages (no entries) are used to probe follower logs for
    /// a common match index in the case of divergent logs, restarted nodes, or
    /// dropped messages. This is typically done by sending probes with a
    /// decrementing base index until a match is found, at which point the
    /// subsequent entries can be sent.
    Append {
        /// The index of the log entry to append after.
        base_index: Index,
        /// The  term of the base entry.
        base_term: Term,
        /// Log entries to append. Must start at base_index + 1.
        entries: Vec<Entry>,
    },

    /// Followers accept or reject appends from the leader depending on whether
    /// the base entry matches their log.
    AppendResponse {
        /// If non-zero, the follower appended entries up to this index. The
        /// entire log up to this index is consistent with the leader. If no
        /// entries were sent (a probe), this will be the matching base index.
        match_index: Index,
        /// If non-zero, the follower rejected an append at this base index
        /// because the base index/term did not match its log. If the follower's
        /// log is shorter than the base index, the reject index will be lowered
        /// to the index after its last local index, to avoid probing each
        /// missing index.
        reject_index: Index,
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
    /// A state machine read command, executed via `State::read`. This is not
    /// replicated, and only evaluated on the leader.
    Read(Vec<u8>),
    /// A state machine write command, executed via `State::apply`. This is
    /// replicated across all nodes, and must produce a deterministic result.
    Write(Vec<u8>),
    /// Requests Raft cluster status from the leader.
    Status,
}

impl encoding::Value for Request {}

/// A client response. This will be wrapped in a Result for error handling.
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

/// Raft cluster status. Generated by the leader.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// The current Raft leader, which generated this status.
    pub leader: NodeID,
    /// The current Raft term.
    pub term: Term,
    /// The match indexes of all nodes, indicating replication progress. Uses a
    /// BTreeMap for test determinism.
    pub match_index: std::collections::BTreeMap<NodeID, Index>,
    /// The current commit index.
    pub commit_index: Index,
    /// The current applied index.
    pub apply_index: Index,
    /// The log storage engine status.
    pub storage: storage::Status,
}
