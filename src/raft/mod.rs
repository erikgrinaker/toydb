//! Implements the Raft distributed consensus protocol.
//!
//! For details, see Diego Ongaro's original writings:
//!
//! * Raft paper: <https://raft.github.io/raft.pdf>
//! * Raft thesis: <https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf>
//! * Raft website: <https://raft.github.io>
//!
//! Raft is a protocol for a group of computers to agree on some data -- or more
//! simply, to replicate the data. It is broadly equivalent to [Paxos] and
//! [Viewstamped Replication], but more prescriptive and simpler to understand.
//!
//! Raft has three main properties:
//!
//! * Fault tolerance: the system tolerates node failures as long as a majority
//!   of nodes (>50%) remain operational.
//!
//! * Linearizability (aka strong consistency): once a client write has been
//!   accepted, it is visible to all clients -- they never see outdated data.
//!
//! * Durability: a write is never lost as long as a majority of nodes remain.
//!
//! It does this by electing a single leader node which serves client requests
//! and replicates writes to other nodes. Requests are executed once they have
//! been confirmed by a strict majority of nodes (a quorum). If a leader fails,
//! a new leader is elected. Clusters have 3 or more nodes, since a two-node
//! cluster can't tolerate failures (1/2 is not a majority and would lead to
//! split brain).
//!
//! Notably, Raft does not provide horizontal scalability. Client requests are
//! processed by a single leader node which can quickly become a bottleneck, and
//! each node stores a complete copy of the entire dataset. Systems often handle
//! this by sharding the data into multiple Raft clusters and using a
//! distributed transaction protocol across them, but this is out of scope here.
//!
//! toyDB follows the Raft paper fairly closely, but, like most implementations,
//! takes some minor artistic liberties.
//!
//! [Paxos]: https://www.microsoft.com/en-us/research/uploads/prod/2016/12/paxos-simple-Copy.pdf
//! [Viewstamped Replication]: https://pmg.csail.mit.edu/papers/vr-revisited.pdf
//!
//! RAFT LOG AND STATE MACHINE
//! ==========================
//!
//! Raft maintains an ordered command log containing arbitrary write commands
//! submitted by clients. It attempts to reach consensus on this log by
//! replicating it to a majority of nodes. If successful, the log is considered
//! committed and immutable up to that point.
//!
//! Once committed, the commands in the log are applied sequentially to a local
//! state machine on each node. Raft itself doesn't care what the state machine
//! and commands are -- in toyDB's case it's a SQL database, but it could be
//! anything. Raft simply passes opaque commands to an opaque state machine.
//!
//! Each log entry contains an index, the leader's term (see next section), and
//! the command. For example, a naïve illustration of a toyDB Raft log might be:
//!
//! Index | Term | Command
//! ------|------|------------------------------------------------------
//!   1   |   1  | CREATE TABLE table (id INT PRIMARY KEY, value STRING)
//!   2   |   1  | INSERT INTO table VALUES (1, 'foo')
//!   3   |   2  | UPDATE table SET value = 'bar' WHERE id = 1
//!   4   |   2  | DELETE FROM table WHERE id = 1
//!
//! The state machine must be deterministic, such that all nodes will reach the
//! same identical state. Raft will apply the same commands in the same order
//! independently on all nodes, but if the commands have non-deterministic
//! behavior such as random number generation or communication with external
//! systems it can lead to state divergence causing different results.
//!
//! In toyDB, the Raft log is managed by `Log` and stored locally in a
//! `storage::Engine`. The state machine interface is the `State` trait. See
//! their documentation for more details.
//!
//! LEADER ELECTION
//! ===============
//!
//! Raft nodes can be in one of three states (or roles): follower, candidate,
//! and leader. toyDB models these as `Node::Follower`, `Node::Candidate`, and
//! `Node::Leader`.
//!
//! * Follower: replicates log entries from a leader. May not know a leader yet.
//! * Candidate: campaigns for leadership in an election.
//! * Leader: processes client requests and replicates writes to followers.
//!
//! Raft fundamentally relies on a single guarantee: there can be at most one
//! _valid_ leader at any point in time (old, since-replaced leaders may think
//! they're still a leader, e.g. during a network partition, but they won't be
//! able to do anything). It enforces this through the leader election protocol.
//!
//! Raft divides time into terms, which are monotonically increasing numbers.
//! Higher terms always take priority over lower terms. There can be at most one
//! leader in a term, and it can't change. Nodes keep track of their last known
//! term and store it on disk (see `Log.set_term()`). Messages between nodes are
//! tagged with the current term (as `Envelope.term`) -- old terms are ignored,
//! and future terms cause the node to become a follower in that term.
//!
//! Nodes start out as leaderless followers. If they receive a message from a
//! leader (in a current or future term), they follow it. Otherwise, they wait
//! out the election timeout (a few seconds), become candidates, and hold a
//! leader election.
//!
//! Candidates increase their term by 1 and send `Message::Campaign` to all
//! nodes, requesting their vote. Nodes respond with `Message::CampaignResponse`
//! saying whether a vote was granted. A node can only grant a single vote in a
//! term (stored to disk via `Log.set_term()`), on a first-come first-serve
//! basis, and candidates implicitly vote for themselves.
//!
//! When a candidate receives a majority of votes (>50%), it becomes leader. It
//! sends a `Message::Heartbeat` to all nodes asserting its leadership, and all
//! nodes become followers when they receive it (regardless of who they voted
//! for). Leaders continue to send periodic heartbeats every second or so. The
//! new leader also appends an empty entry to its log in order to safely commit
//! all entries from previous terms (Raft paper section 5.4.2).
//!
//! The new leader must have all committed entries in its log (or the cluster
//! would lose data). To ensure this, there is one additional condition for
//! granting a vote: the candidate's log must be at least as up-to-date as the
//! voter. Because an entry must be replicated to a majority before being
//! committed, this ensures a candidate can only win a majority of votes if its
//! log is up-to-date with all committed entries (Raft paper section 5.4.1).
//!
//! It's possible that no candidate wins an election, for example due to a tie
//! or a majority of nodes being offline. After an election timeout passes,
//! candidates will again bump their term and start a new election, until a
//! leader can be established. To avoid frequent ties, nodes use different,
//! randomized election timeouts (Raft paper section 5.2).
//!
//! Similarly, if a follower doesn't hear from a leader in an election timeout
//! interval, it will become candidate and hold another election. The periodic
//! leader heartbeats prevent this as long as the leader is running and
//! connected. A node that becomes disconnected from the leader will continually
//! hold new elections by itself until the network heals, at which point a new
//! election will be held in its term (disrupting the current leader).
//!
//! REPLICATION AND CONSENSUS
//! =========================
//!
//! When the leader receives a client write request, it appends the command to
//! its local log via `Log.append()`, and sends the log entry to all peers in
//! a `Message::Append`. Followers will attempt to durably append the entry to
//! their local logs and respond with `Message::AppendResponse`.
//!
//! Once a majority have acknowledged the append, the leader commits the entry
//! via `Log.commit()` and applies it to its local state machine, returning the
//! result to the client. It will inform followers about the commit in the next
//! heartbeat as `Message::Heartbeat.commit_index` so they can apply it too, but
//! this is not necessary for correctness (they will commit and apply it if they
//! become leader, otherwise they have no need for applying it).
//!
//! Followers may not be able to append the entry to their log -- they may be
//! unreachable, lag behind the leader, or have divergent logs (see Raft paper
//! section 5.3). The `Append` contains the index and term of the log entry
//! immediately before the replicated entry as `base_index` and `base_term`. An
//! index/term pair uniquely identifies a command, and if two logs have the same
//! index/term pair then the logs are identical up to and including that entry
//! (Raft paper section 5.3). If the base index/term matches the follower's log,
//! it appends the entry (potentially replacing any conflicting entries),
//! otherwise it rejects it.
//!
//! When a follower rejects an append, the leader must try to find a common log
//! entry that exists in both its and the follower's log where it can resume
//! replication. It does this by sending `Message::Append` probes only
//! containing a base index/term but no entries -- it will continue to probe
//! decreasing indexes one by one until the follower responds with a match, then
//! send an `Append` with the missing entries (Raft paper section 5.3). It keeps
//! track of each follower's `match_index` and `next_index` in a `Progress`
//! struct to manage this.
//!
//! In case `Append` messages or responses are lost, leaders also send their
//! `last_index` and term in each `Heartbeat`. If followers don't have that
//! index/term pair in their log, they'll say so in the `HeartbeatResponse` and
//! the leader can begin probing their logs as with append rejections.
//!
//! CLIENT REQUESTS
//! ===============
//!
//! Client requests are submitted as `Message::ClientRequest` to the local Raft
//! node. They are only processed on the leader, but followers will proxy them
//! to the leader (Raft thesis section 6.2). To avoid complications with message
//! replays (Raft thesis section 6.3), requests are not retried internally, and
//! are explicitly aborted with `Error::Abort` on leader/term changes as well as
//! elections.
//!
//! Write requests, `Request::Write`, are appended to the Raft log and
//! replicated. The leader keeps track of the request and its log index in a
//! `Write` struct. Once the command is committed and applied to the local state
//! machine, the leader looks up the write request by its log index and sends
//! the result to the client. Deterministic errors (e.g. foreign key violations)
//! are also returned to the client, but non-deterministic errors (e.g. IO
//! errors) must panic the node to avoid replica state divergence.
//!
//! Read requests, `Request::Read`, are only executed on the leader and don't
//! need to be replicated via the Raft log. However, to ensure linearizability,
//! the leader has to confirm with a quorum that it's actually still the leader.
//! Otherwise, it's possible that a new leader has been elected elsewhere and
//! executed writes without us knowing about it. It does this by assigning an
//! incrementing sequence number to each read, keeping track of the request in a
//! `Read` struct, and immediately sending a `Read` message with the latest
//! sequence number. Followers respond with the sequence number, and once a
//! quorum have confirmed a sequence number the read is executed and the result
//! returned to the client.
//!
//! IMPLEMENTATION CAVEATS
//! ======================
//!
//! For simplicity, toyDB implements the bare minimum for a functional and
//! correct Raft protocol, and omits several advanced mechanisms that would be
//! needed for a real production system. In particular:
//!
//! * No leases: for linearizability, every read request requires the leader to
//!   confirm with followers that it's still the leader. This could be avoided
//!   with a leader lease for a predefined time interval (Raft paper section 8,
//!   Raft thesis section 6.3).
//!
//! * No cluster membership changes: to add or remove nodes, the entire cluster
//!   must be stopped and restarted with the new configuration, otherwise it
//!   risks multiple leaders (Raft paper section 6).
//!
//! * No snapshots: new or lagging nodes must be caught up by replicating and
//!   replaying the entire log, instead of sending a state machine snapshot
//!   (Raft paper section 7).
//!
//! * No log truncation: because snapshots aren't supported, the entire Raft
//!   log must be retained forever in order to catch up new/lagging nodes,
//!   leading to excessive storage use (Raft paper section 7).
//!
//! * No pre-vote or check-quorum: a node that's partially partitioned (can
//!   reach some but not all nodes) can cause persistent unavailability with
//!   spurious elections or heartbeats. A node rejoining after a partition can
//!   also temporarily disrupt a leader. This requires additional pre-vote and
//!   check-quorum protocol extensions (Raft thesis section 4.2.3 and 9.6).
//!
//! * No request retries: client requests will not be retried on leader changes
//!   or message loss, and will be aggressively aborted, to ignore problems
//!   related to message replay (Raft thesis section 6.3).
//!
//! * No reject hints: if a follower has a divergent log, the leader will probe
//!   entries one by one until a match is found. The replication protocol could
//!   instead be extended with rejection hints (Raft paper section 5.3).

mod log;
mod message;
mod node;
mod state;

pub use log::{Entry, Index, Key, Log};
pub use message::{Envelope, Message, ReadSequence, Request, RequestID, Response, Status};
pub use node::{Node, NodeID, Options, Term, Ticks};
pub use state::State;

/// The interval between Raft ticks, the unit of time.
pub const TICK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// The interval between leader heartbeats in ticks.
const HEARTBEAT_INTERVAL: Ticks = 4;

/// The default election timeout range in ticks. This is randomized in this
/// interval, to avoid election ties.
const ELECTION_TIMEOUT_RANGE: std::ops::Range<Ticks> = 10..20;

/// The maximum number of entries to send in a single append message.
const MAX_APPEND_ENTRIES: usize = 100;
