# SQL Raft Replication

toyDB uses Raft to replicate SQL storage across a cluster of nodes (see the Raft section above for
details). All nodes will store a copy of the SQL database, and the Raft leader will replicate writes
across nodes and execute reads.

Recall the Raft state machine interface `raft::State`:

https://github.com/erikgrinaker/toydb/blob/8782c2b05f11333c1586ef248f1a13dc1c8dec4a/src/raft/state.rs#L4-L51

In toyDB, the state machine is just a `sql::engine::Local` storage engine with a thin wrapper:

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/engine/raft.rs#L278-L291

Raft will submit read and write commands to this state machine as binary `Vec<u8>` data, so we have
to represent the methods of `sql::engine::Engine` as binary Raft commands. We do this as two
enums, `sql::engine::raft::Read` and `sql::engine::raft::Write`, which we'll Bincode-encode:

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/engine/raft.rs#L16-L71

Notice that almost all requests include a `mvcc::TransactionState`. Most of the useful methods of
`sql::engine::Engine` are on the `sql::engine::Transaction`, but unlike the `Local` engine, below
Raft we can't hold on to a `Transaction` object in memory between each command -- nodes may restart
and leadership may move, and we want client transactions to keep working despite this. Instead, we
will use the client-supplied `mvcc::TransactionState` to reconstruct a `Transaction` for every
command via `mvcc::Transaction::resume()` and call methods on it.

When the state machine receives a write command, it decodes it as a `Write` and calls the
appropriate `Local` method. The result is Bincode-encoded and returned to the caller, who knows what
return type to expect for a given command. The state machine also keeps track of the Raft applied
index of each command as a separate key in the key/value store.

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/engine/raft.rs#L346-L367

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/engine/raft.rs#L306-L338

Similarly, read commands are decoded as a `Read` and the appropriate `Local` method is called:

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/engine/raft.rs#L369-L404

That's the state machine running below Raft. But how do we actually send these commands to Raft and
receive results? That's handled by the `sql::engine::Raft` implementation, which uses a channel to
send requests to the local Raft node (we'll see how this plumbing works in the server section):

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/engine/raft.rs#L80-L95

The channel takes a `raft::Request` containing binary Raft client requests, and also a return
channel where the Raft node can send back a `raft::Response`. The Raft engine has a few convenience
methods to send requests and receive responses, for both read and write requests:

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/engine/raft.rs#L114-L135

And the implementation of the `Engine` and `Transaction` traits simply send requests via Raft:

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/engine/raft.rs#L194-L276

One thing to note here is that we don't support streaming data via Raft, so e.g. the
`Transaction::scan` method will buffer the entire result in a `Vec`. With a full table scan, this
will load the entire table into memory -- that's unfortunate, but we keep it simple.

---

<p align="center">
← <a href="sql-storage.md">SQL Storage</a> &nbsp; | &nbsp; <a href="sql-parser.md">SQL Parsing</a> →
</p>