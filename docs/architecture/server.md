# Server and Client

Now that we've gone over the individual components, we'll tie them all together with the server and
client.

## Server

in the toyDB
server `toydb::Server`, located in the [`server`](https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs) module.

The server wraps an inner Raft node `raft::Node`, which manages the SQL state machine, and is
responsible for routing network traffic between the Raft node, its Raft peers, and SQL clients.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L27-L44

For network protocol, the server uses the Bincode encoding that we've discussed in the encoding
section, sent over a TCP connection. There's no need for any further framing, since Bincode knows
how many bytes to expect for each message depending on the type it's decoding into.

The server does not use async Rust and e.g. [Tokio](https://tokio.rs), instead opting for regular OS
threads. Async Rust can significantly complicate the code, which would obscure the main concepts,
and any efficiency gains would be entirely irrelevant for toyDB.

Internally in the server, messages are passed around between threads using
[Crossbeam channels](https://docs.rs/crossbeam/latest/crossbeam/channel/index.html).

The main server loop `Server::serve` listens for inbound TCP connections on port 9705 for Raft peers
and 9605 for SQL clients, and spawns threads to process them. We'll look at Raft and SQL services
separately.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L66-L110

## Raft Routing

The heart of the server is the Raft processing thread `Server::raft_route`. This is responsible
for periodically ticking the Raft node via `raft::Node::tick`, stepping inbound messages from
Raft peers into the node via `raft::Node::step`, and sending outbound messages to peers.

It also takes inbound Raft client requests from the `sql::engine::Raft` SQL engine, steps them
into the Raft node via `raft::Node::step`, and passes responses back to the appropriate client
as the node emits them.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L169-L249

When the node starts up, it spawns a `Server::raft_send_peer` thread for each Raft peer to send
outbound messages to them.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L84-L91

These threads continually attempt to connect to the peer via TCP, and then read any outbound
`raft::Envelope(raft::Message)` messages from `Server::raft_route` via a channel and writes the
messages into the TCP connection using Bincode:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L146-L167

The server also continually listens for inbound Raft TCP connections from peers in
`Server::raft_accept`:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L112-L134

When an inbound connection is accepted, a `Server::raft_receive_peer` thread is spawned that reads
Bincode-encoded `raft::Envelope(raft::Message)` messages from the TCP connection and sends them to
`Server::raft_route` via a channel.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L136-L144

The Raft cluster is now fully connected, and the nodes can all talk to each other.

## SQL Service

Next, let's serve some SQL clients. The SQL service uses the enums `toydb::Request` and
`toydb::Response` as a client protocol, again Bincode-encoded over TCP.

The primary request type is `Request::Execute` which executes a SQL statement against a
`sql::execution::Session` and returns a `sql::execution::StatementResult`, as we've seen previously.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L312-L337

The server sets up a `sql::engine::Raft` SQL engine, with a Crossbeam channel that's used to send
`raft::Request` Raft client requests to `Server::raft_route` and onwards to the local `raft::Node`.
It then spawns a `Server::sql_accept` thread to listen for inbound SQL client connections:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L104-L106

When a SQL client connection is accepted, a new client session `sql::execution::Session` is set up
for the client, and we spawn a `Server::sql_session` thread to serve the connection:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L251-L272

These session threads continually read `Request` messages from the client, execute them against the
SQL session (and ultimately the Raft node), before sending a `Response` back to the client.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/server.rs#L274-L309

## `toydb` Binary

The `toydb` binary in `src/bin/toydb.rs` launches the server, and is is a thin wrapper around
`toydb::Server`. It is a tiny [`clap`](https://docs.rs/clap/latest/clap/) command:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/bin/toydb.rs#L82-L89

It first parses a server configuration from the `toydb.yaml` file:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/bin/toydb.rs#L30-L59

Then it initializes the Raft log storage and SQL state machine:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/bin/toydb.rs#L105-L133

And finally it launches the `toydb::Server`:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/bin/toydb.rs#L135-L137

toyDB is now up and running!

## Client Library

The main client library `toydb::Client` in the [`client`](https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/client.rs)
module is used to connect to a toyDB server:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/client.rs#L15-L24

When initialized, it connects to a toyDB server over TCP:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/client.rs#L27-L33

It can then send Bincode-encoded `toydb::Request` to the server, and receives `toydb::Response`
back.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/client.rs#L35-L40


In particular, `Client::execute` can be used to execute arbitrary SQL statements in the client's
session:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/client.rs#L42-L56

## `toysql` Binary

However, `toydb::Client` is a programmatic API, and we want a more convenient user interface.
The `toysql` client in [`src/bin/toysql.rs`](https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs)
provides a typical [REPL](https://en.wikipedia.org/wiki/Read–eval–print_loop) (read-evaluate-print loop) where users can enter SQL statements and view the results.

Like `toydb`, `toysql` is a tiny [`clap`](https://docs.rs/clap/latest/clap/) command that takes a
toyDB server address to connect to and starts an interactive shell:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs#L29-L53

This first attempts to connect to the toyDB server using the `toydb::Client` client, and then starts
an interactive shell using the [Rustyline](https://docs.rs/rustyline/latest/rustyline/) library.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs#L55-L81

The shell is simply a loop that prompts the user to input a SQL statement:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs#L216-L250

Each statement is the executed against the server via `toydb::Client::execute`, and the response
is formatted and printed as output:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs#L83-L92

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs#L175-L204

And with that, we have a fully functional SQL database system and can run queries to our heart's
content. Have fun!

---

<p align="center">
← <a href="sql-execution.md">SQL Execution</a>
</p>