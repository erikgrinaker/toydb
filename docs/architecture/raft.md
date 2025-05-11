# Raft Consensus

[Raft](https://raft.github.io) is a distributed consensus protocol which replicates data across a
cluster of nodes in a consistent and durable manner. It is described in the very readable
[Raft paper](https://raft.github.io/raft.pdf), and in the more comprehensive
[Raft thesis](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf).

The toyDB Raft implementation is in the [`raft`](https://github.com/erikgrinaker/toydb/tree/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/raft)
module, and is described in the module documentation:

https://github.com/erikgrinaker/toydb/blob/d96c6dd5ae7c0af55ee609760dcd958c289a44f2/src/raft/mod.rs#L1-L240

Raft is fundamentally the same protocol as [Paxos](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)
and [Viewstamped Replication](https://pmg.csail.mit.edu/papers/vr-revisited.pdf), but an
opinionated variant designed to be simple, understandable, and practical. It is widely used in the
industry: [CockroachDB](https://www.cockroachlabs.com), [TiDB](https://www.pingcap.com),
[etcd](https://etcd.io), [Consul](https://developer.hashicorp.com/consul), and many others.

Briefly, Raft elects a leader node which coordinates writes and replicates them to followers. Once a
majority (>50%) of nodes have acknowledged a write, it is considered durably committed. It is common
for the leader to also serve reads, since it always has the most recent data and is thus strongly
consistent.

A cluster must have a majority of nodes (known as a [quorum](https://en.wikipedia.org/wiki/Quorum_(distributed_computing)))
live and connected to remain available, otherwise it will not commit writes in order to guarantee
data consistency and durability. Since there can only be one majority in the cluster, this prevents
a [split brain](https://en.wikipedia.org/wiki/Split-brain_(computing)) scenario where two active
leaders can exist concurrently (e.g. during a [network partition](https://en.wikipedia.org/wiki/Network_partition))
and store conflicting values.

The Raft leader appends writes to an ordered command log, which is then replicated to followers.
Once a majority has replicated the log up to a given entry, that log prefix is committed and then
applied to a state machine. This ensures that all nodes will apply the same commands in the same
order and eventually reach the same state (assuming the commands are deterministic). Raft itself
doesn't care what the state machine and commands are, but in toyDB's case it's SQL tables and rows
stored in an MVCC key/value store.

This diagram from the Raft paper illustrates how a Raft node receives a command from a client (1),
adds it to its log and reaches consensus with other nodes (2), then applies it to its state machine
(3) before returning a result to the client (4):

<img src="./images/raft.svg" alt="Raft node" width="400" style="display: block; margin: 30px auto;">

You may notice that Raft is not very scalable, since all reads and writes go via the leader node,
and every node must store the entire dataset. Raft solves replication and availability, but not
scalability. Real-world systems typically provide horizontal scalability by splitting a large
dataset across many separate Raft clusters (i.e. sharding), but this is out of scope for toyDB.

For simplicitly, toyDB implements the bare minimum of Raft, and omits optimizations described in
the paper such as state snapshots, log truncation, leader leases, and more. The implementation is
in the [`raft`](https://github.com/erikgrinaker/toydb/blob/d96c6dd5ae7c0af55ee609760dcd958c289a44f2/src/raft/mod.rs)
module, and we'll walk through the main components next.

There is a comprehensive set of Raft test scripts in [`src/raft/testscripts/node`](https://github.com/erikgrinaker/toydb/blob/386153f5c00cb1a88b1ac8489ae132674d96f68a/src/raft/testscripts/node),
which illustrate the protocol in a wide variety of scenarios.

## Log Storage

Raft replicates an ordered command log consisting of `raft::Entry`:

https://github.com/erikgrinaker/toydb/blob/90a6cae47ac20481ac4eb2f20eea50f02e6c2b33/src/raft/log.rs#L10-L28

`index` specifies the position in the log, and `command` contains the binary command to apply to the
state machine. The `term` identifies the leadership term in which the command was proposed: a new
term begins when a new leader election is held (we'll get back to this later).

Entries are appended to the log by the leader and replicated to followers. Once acknowledged by a
quorum, the log up to that index is committed and will never change. Entries that are not yet
committed may be replaced or removed if the leader changes.

The Raft log enforces the following invariants:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/log.rs#L80-L91

`raft::Log` implements a Raft log, and stores log entries in a `storage::Engine` key/value store:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/log.rs#L43-L116

It also stores some additional metadata that we'll need later: the current term, vote, and commit
index. These are stored as separate keys:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/log.rs#L30-L39

Individual entries are appended to the log via `Log::append`, typically when the leader wants to
replicate a new write:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/log.rs#L190-L203

Entries can also be appended in bulk via `Log::splice`, typically when entries are replicated to
followers. This also allows replacing existing uncommitted entries, e.g. after a leader change:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/log.rs#L269-L343

Committed entries are marked by `Log::commit`, making them immutable and eligible for state machine
application:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/log.rs#L205-L222

The log also has methods to read entries from the log, either individually as `Log::get` or by
iterating over a range with `Log::scan`:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/log.rs#L224-L267

## State Machine Interface

Raft doesn't know or care what the log commands are, nor what the state machine does with them. It
simply takes `raft::Entry` from the log and gives them to the state machine.

The Raft state machine is represented by the `raft::State` trait. Raft will ask about the last
applied entry via `State::get_applied_index`, and feed it newly committed entries via
`State::apply`. It also allows reads via `State::read`, but we'll get back to that later.

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/state.rs#L4-L51

The state machine does not have to flush its state to durable storage after each transition; on node
crashes, the state machine is allowed to regress, and will be caught up by replaying the unapplied
log entries. It is also possible to implement a purely in-memory state machine (and in fact, toyDB
allows running the state machine with a `Memory` storage engine).

The state machine must take care to be deterministic: the same commands applied in the same order
must result in the same state across all nodes. This means that a command can't e.g. read the
current time or generate a random number -- these values must be included in the command. It also
means that non-deterministic errors, such as an IO error, must halt command application (in toyDB's
case, we just panic and crash the node).

In toyDB's, the state machine is an MVCC key/value store that stores SQL tables and rows, as we'll
see in the SQL Raft replication section.

## Node Roles

In Raft, a node can have one out of three roles:

* **Leader:** replicates writes to followers and serves client requests.
* **Follower:** replicates writes from a leader.
* **Candidate:** campaigns for leadership.

The Raft paper summarizes these roles and transitions in the following diagram (we'll discuss
leader election in detail below):

<img src="./images/raft-states.svg" alt="Raft states" width="400" style="display: block; margin: 30px auto;">

In toyDB, a node is represented by the `raft::Node` enum, with variants for each state:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L47-L66

This wraps the `raft::RawNode<Role>` type which contains the inner node state. It is generic over
the role, and uses the [typestate pattern](http://cliffle.com/blog/rust-typestate/) to provide
methods and transitions depending on the node's current role. This enforces state transitions and
invariants at compile time via Rust's type system -- for example, only `RawNode<Candidate>` has an
`into_leader()` method, since only candidates can transition to leaders (when they win an election).

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L156-L177

The `RawNode::role` field contains role-specific state as structs implementing the `Role` marker
trait:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L661-L680

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L242-L255

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L523-L531

We'll see what the various fields are used for in the following sections.

## Node Interface and Communication

The `raft::Node` enum has two main methods that drive the node: `tick()` and `step()`. These consume
the current node and return a new node, possibly with a different role.

`tick()` advances time by a logical tick. This is used to measure the passage of time, e.g. to
trigger election timeouts or periodic leader heartbeats. toyDB uses a tick interval of 100
milliseconds (see `raft::TICK_INTERVAL`), and will call `tick()` on the node at this rate.

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L125-L132

`step()` processes an inbound message from a different node or client:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L107-L123

Outbound messages to other nodes are sent via the `RawNode::tx` channel:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L171-L172

Nodes are identified by a unique node ID, which is given at node startup:

https://github.com/erikgrinaker/toydb/blob/90a6cae47ac20481ac4eb2f20eea50f02e6c2b33/src/raft/node.rs#L17-L18

Messages are wrapped in a `raft::Envelope` specifying the sender and recipient:

https://github.com/erikgrinaker/toydb/blob/d96c6dd5ae7c0af55ee609760dcd958c289a44f2/src/raft/message.rs#L10-L21

The envelope contains a `raft::Message`, an enum which encodes the Raft message protocol. We won't
dwell on the specific message types here, but discuss them invididually in the following sections.
Raft does not require reliable message delivery, so messages may be dropped or reordered at any
time, although toyDB's use of TCP provides stronger delivery guarantees.

https://github.com/erikgrinaker/toydb/blob/d96c6dd5ae7c0af55ee609760dcd958c289a44f2/src/raft/message.rs#L25-L152

This is an entirely synchronous and deterministic model -- the same sequence of calls on a given
node in a given initial state will always produce the same result. This is very convenient for
testing and understandability. We will see in the server section how toyDB drives the node on a
separate thread, provides a network transport for messages, and ticks it at regular intervals.

## Leader Election and Terms

In the steady state, Raft simply has a leader which replicates writes to followers. But to reach
this steady state, we must elect a leader, which is where much of the subtle complexity lies. See
the Raft paper for comprehensive details and safety arguments, we'll summarize it briefly below.

Raft divides time into _terms_. The term is a monotonically increasing number starting at 1. There
can only be one leader in a term (or none if an election fails), and the term can never regress.
Replicated commands belong to the specific term under which they were proposed.

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L20-L21

Let's walk through an election, where we bootstrap a brand new, empty toyDB cluster with 3 nodes.

Nodes are initialized by calling `Node::new()`. Since this is a new cluster, they are given an empty
`raft::Log` and `raft::State`, at term 0. Nodes start with role `Follower`, but without a leader.

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L68-L87

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L266-L290

Now, nothing really happens for a while, as the nodes are waiting to maybe hear from an existing
leader (there is none). Every 100 ms we call `tick()`, until we reach `election_timeout`:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L489-L497

Notice how `new()` set `election_timeout` to a random value (in the range `ELECTION_TIMEOUT_RANGE`
of 10-20 ticks, i.e. 1-2 seconds). If all nodes had the same timeout, they would likely campaign for
leadership simultaneously, resulting in an election tie -- Raft uses randomized election timeouts to
avoid such ties.

Once a node reaches `election_timeout` it transitions to role `Candidate`:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L292-L312

When it becomes a candidate it campaigns for leadership by increasing its term to 1, voting for
itself, and sending `Message::Campaign` to all peers asking for their vote:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L647-L658

In Raft, the term can't regress, and a node can only cast a single vote in each term (even across
restarts), so both of these are persisted to disk via `Log::set_term_vote()`.

When the two other nodes (still in state `Follower`) receive the `Message::Campaign` asking for a
vote, they will first increase their term to 1 (since this is a newer term than their local term 0):

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L347-L351

They then grant the vote since they haven't yet voted for anyone else in term 1. They persist the
vote to disk via `Log::set_term_vote()` and return a `Message::CampaignResponse { vote: true }` to
the candidate:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L424-L449

They also check that the candidate's log is at least as long as theirs, which is trivially true in
this case since the log is empty. This is necessary to ensure that a leader has all committed
entries (see section 5.4.1 in the Raft paper).

When the candidate receives the `Message::CampaignResponse` it records the vote from each node. Once
it has a quorum (in this case 2 out of 3 votes including its own vote) it becomes leader in term 1:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L599-L606

When it becomes leader, it sends a `Message::Heartbeat` to all peers to tell them it is now the
leader in term 1. It also appends an empty entry to its log and replicates it, but we will ignore
this for now (see section 5.4.2 in the Raft paper for why).

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L563-L583

When the other nodes receive the heartbeat, they become followers of the new leader in its term:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L359-L384

From now on, the leader will send periodic `Message::Heartbeat` every 4 ticks (see
`HEARTBEAT_INTERVAL`) to assert its leadership:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L945-L953

The followers record when they last received any message from the leader (including heartbeats), and
will hold a new election if they haven't heard from the leader in an election timeout (e.g. due to a
leader crash or network partition):

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L353-L356

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L489-L497

This entire process is illustrated in the test script [`election`](https://github.com/erikgrinaker/toydb/blob/cb234a0b776484608118fd9382869ee5bc30d4f0/src/raft/testscripts/node/election),
along with several other test scripts that show e.g. [election ties](https://github.com/erikgrinaker/toydb/blob/cb234a0b776484608118fd9382869ee5bc30d4f0/src/raft/testscripts/node/election_tie),
[contested elections](https://github.com/erikgrinaker/toydb/blob/cb234a0b776484608118fd9382869ee5bc30d4f0/src/raft/testscripts/node/election_contested),
and other scenarios:

https://github.com/erikgrinaker/toydb/blob/cb234a0b776484608118fd9382869ee5bc30d4f0/src/raft/testscripts/node/election#L1-L72

## Client Requests and Forwarding

Once a leader has been elected, we can submit read and write requests to it. This is done by
stepping a `Message::ClientRequest` into the node using the local node ID, with a unique request ID
(toyDB uses UUIDv4), and waiting for an outbound response message with the same ID:

https://github.com/erikgrinaker/toydb/blob/d96c6dd5ae7c0af55ee609760dcd958c289a44f2/src/raft/message.rs#L134-L151

https://github.com/erikgrinaker/toydb/blob/d96c6dd5ae7c0af55ee609760dcd958c289a44f2/src/raft/message.rs#L164-L188

The requests and responses themselves are arbitrary binary data which is interpreted by the state
machine. For our purposes here, let's pretend the requests are:

* `Request::Write("key=value")` → `Response::Write("ok")`
* `Request::Read("key")` → `Response::Read("value")`

The fundamental difference between read and write requests are that write requests are replicated
through Raft and executed on all nodes, while read requests are only executed on the leader without
being appended to the log. It would be possible to execute reads on followers too, for load
balancing, but these reads would be eventually consistent and thus violate linearizability, so toyDB
only executes reads on the leader.

If a request is submitted to a follower, it will be forwarded to the leader and the response
forwarded back to the client (distinguished by the sender/recipient node ID -- a local client always
uses the local node ID):

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L451-L474

For simplicity, we cancel the request with `Error::Abort` if a request is submitted to a candidate,
and similarly if a follower changes its role to candidate or discovers a new leader. We could have
held on to these and redirected them to a new leader, but we keep it simple and ask the client to
retry.

We'll look at the actual read and write request processing next.

## Write Replication and Application

When the leader receives a write request, it proposes the command for replication to followers. It
keeps track of the in-flight write and its log entry index in `writes`, such that it can respond to
the client with the command result once the entry has been committed and applied.

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L895-L904

To propose the command, the leader appends it to its log and sends a `Message::Append` to each
follower to replicate it to their logs:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L966-L980

In steady state, `Message::Append` just contains the single log entry we appended above:

https://github.com/erikgrinaker/toydb/blob/d96c6dd5ae7c0af55ee609760dcd958c289a44f2/src/raft/message.rs#L87-L108

However, sometimes followers may be lagging behind the leader (e.g. after a crash), or their log may
have diverged from the leader (e.g. unsuccessful proposals from a stale leader after a network
partition). To handle these cases, the leader tracks the replication progress of each follower as
`raft::Progress`:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L682-L698

We'll gloss over these cases here (see the Raft paper and the code in `raft::Progress` and
`maybe_send_append()` for details). In the steady state, where each entry is successfully appended
and replicated one at a time, `maybe_send_append()` will fall through to the bottom and send a
single entry:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L1068-L1128

The `Message::Append` contains the index/term of the entry immediately before the new entry as
`base_index` and `base_term`. If the follower's log also contains an entry with this index and term
then its log is guaranteed to match (be equal to) the leader's log up to this entry (see section 5.3
in the Raft paper). The follower can then append the new log entry and return a
`Message::AppendResponse` confirming that the entry was appended and that its log matches the
leader's log up to `match_index`:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L386-L410

When the leader receives the `Message::AppendResponse`, it will update its view of the follower's
`match_index`.

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L844-L858

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L701-L710

Once a quorum of nodes (in our case 2 out of 3 including the leader) have the entry in their log,
the leader can commit the entry and apply it to the state machine. It also looks up the in-flight
write request from `writes` and sends the command result back to the client as
`Message::ClientResponse`:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L982-L1032

The leader will also propagate the new commit index to followers via the next heartbeat, so that
they can also apply any pending log entries to their state machine. This isn't strictly necessary,
since reads are executed on the leader and nodes have to apply pending entries before becoming
leaders, but we do it anyway so that they don't fall too far behind on application.

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L359-L384

This process is illustrated in the test scripts [`append`](https://github.com/erikgrinaker/toydb/blob/cb234a0b776484608118fd9382869ee5bc30d4f0/src/raft/testscripts/node/append) and [`heartbeat_commits_follower`](https://github.com/erikgrinaker/toydb/blob/cb234a0b776484608118fd9382869ee5bc30d4f0/src/raft/testscripts/node/heartbeat_commits_follower)
(along with many other scenarios):

https://github.com/erikgrinaker/toydb/blob/cb234a0b776484608118fd9382869ee5bc30d4f0/src/raft/testscripts/node/append#L1-L43

https://github.com/erikgrinaker/toydb/blob/cb234a0b776484608118fd9382869ee5bc30d4f0/src/raft/testscripts/node/heartbeat_commits_follower#L1-L50

## Read Processing

For linearizable (aka strongly consistent) reads, we must execute read requests on the leader, as
mentioned above. However, this is not sufficient: under e.g. a network partition, a node may think
it's still the leader while in fact a different leader has been elected elsewhere (in a later term)
and executed writes there.

To handle this case, the leader must confirm that it is still the leader for each read, by sending a
`Message::Read` to its followers containing a read sequence number. Only if a quorum confirms that
it is still the leader can the read be executed. This incurs an additional network roundtrip, which
is clearly inefficient, so real-world systems often use leader leases instead (see section 6.4.1 of
the Raft _thesis_, not the paper) -- but it's fine for toyDB.

https://github.com/erikgrinaker/toydb/blob/d96c6dd5ae7c0af55ee609760dcd958c289a44f2/src/raft/message.rs#L125-L132

When the leader receives the read request, it increments the read sequence number, stores the
pending read request in `reads`, and sends a `Message::Read` to all followers:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L906-L917

When the followers receive the `Message::Read`, they simply respond with a `Message::ReadResponse`
if it's from their current leader (messages from stale terms are ignored):

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L342-L346

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L412-L422

When the leader receives the `Message::ReadResponse` it records it in the peer's `Progress`, and
executes the read once a quorum have confirmed the sequence number:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L860-L866

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/node.rs#L1034-L1066

We now have a Raft-managed state machine with replicated writes and linearizable reads.

---

<p align="center">
← <a href="mvcc.md">MVCC Transactions</a> &nbsp; | &nbsp; <a href="sql.md">SQL Engine</a> →
</p>