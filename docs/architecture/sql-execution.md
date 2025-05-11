# SQL Execution

Now that the planner and optimizer have done all the hard work of figuring out how to execute a
query, it's time to actually execute it.

## Plan Executor

Plan execution is done by `sql::execution::Executor` in the
[`sql::execution`](https://github.com/erikgrinaker/toydb/tree/9419bcf6aededf0e20b4e7485e2a5fa3e975d79f/src/sql/execution)
module, using a `sql::engine::Transaction` to access the SQL storage engine.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/execution/executor.rs#L14-L49

The executor takes a `sql::planner::Plan` as input, and will return an `ExecutionResult` depending
on the statement type.

https://github.com/erikgrinaker/toydb/blob/686d3971a253bfc9facc2ba1b0e716cff5c109fb/src/sql/execution/executor.rs#L331-L339

When executing the plan, the executor will branch off depending on the statement type:

https://github.com/erikgrinaker/toydb/blob/686d3971a253bfc9facc2ba1b0e716cff5c109fb/src/sql/execution/executor.rs#L57-L101

We'll focus on `SELECT` queries here, which are the most interesting.

toyDB uses the iterator model (also known as the volcano model) for query execution. In the case of
a `SELECT` query, the result is a row iterator, and pulling from this iterator by calling `next()`
will drive the entire execution pipeline by recursively calling `next()` on the child nodes' row
iterators. This maps very naturally onto Rust's iterators, and we leverage these to construct the
execution pipeline as nested iterators.

Execution itself is fairly straightforward, since we're just doing exactly what the planner tells us
to do in the plan. We call `Executor::execute_node` recursively on each `sql::planner:Node`,
starting with the root node. Each node returns a result row iterator that the parent node can pull
its input rows from, process them, and output the resulting rows via its own row iterator (with the
root node's iterator being returned to the caller):

https://github.com/erikgrinaker/toydb/blob/686d3971a253bfc9facc2ba1b0e716cff5c109fb/src/sql/execution/executor.rs#L103-L104

`Executor::execute_node()` will simply look at the type of `Node`, recursively call
`Executor::execute_node()` on any child nodes, and then process the rows accordingly.

https://github.com/erikgrinaker/toydb/blob/686d3971a253bfc9facc2ba1b0e716cff5c109fb/src/sql/execution/executor.rs#L103-L212

We won't discuss every plan node in detail, but let's consider the movie plan we've looked at
previously:

```
Select
└─ Order: movies.released desc
   └─ Projection: movies.title, movies.released, genres.name as genre
      └─ HashJoin: inner on movies.genre_id = genres.id
         ├─ Scan: movies (released >= 2000)
         └─ Scan: genres
```

We'll recursively call `execute_node()` until we end up in the two `Scan` nodes. These simply
call through to the SQL engine (either using Raft or local disk) via `Transaction::scan()`, passing
in the scan predicate if any, and return the resulting row iterator:

https://github.com/erikgrinaker/toydb/blob/686d3971a253bfc9facc2ba1b0e716cff5c109fb/src/sql/execution/executor.rs#L203-L204

`HashJoin` will then join the output rows from the `movies` and `genres` iterators by using a
hash join. This builds an in-memory table for `genres` and then iterates over `movies`, joining
the rows:

https://github.com/erikgrinaker/toydb/blob/686d3971a253bfc9facc2ba1b0e716cff5c109fb/src/sql/execution/executor.rs#L128-L141

https://github.com/erikgrinaker/toydb/blob/889aef9f24c0fa4d58e314877fa17559a9f3d5d2/src/sql/execution/join.rs#L103-L183

The `Projection` node will simply evaluate the (trivial) column expressions using each joined
row as input:

https://github.com/erikgrinaker/toydb/blob/686d3971a253bfc9facc2ba1b0e716cff5c109fb/src/sql/execution/executor.rs#L179-L186

And finally the `Order` node will sort the results (which requires buffering them all in memory):

https://github.com/erikgrinaker/toydb/blob/686d3971a253bfc9facc2ba1b0e716cff5c109fb/src/sql/execution/executor.rs#L173-L177

https://github.com/erikgrinaker/toydb/blob/686d3971a253bfc9facc2ba1b0e716cff5c109fb/src/sql/execution/executor.rs#L298-L328

The output row iterator of `Order` is returned via `ExecutionResult::Select`, and the caller can now
go ahead and pull the resulting rows from it.

## Session Management

The entry point to the SQL engine is the `sql::execution::Session`, which represents a single user
session. It is obtained via `sql::engine::Engine::session()`.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/sql/execution/session.rs#L14-L21

The session takes a series of raw SQL statement strings as input and parses them:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/sql/execution/session.rs#L29-L33

For each statement, it returns a result depending on the kind of statement:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/sql/execution/session.rs#L132-L148

The session itself performs transaction control. It handles `BEGIN`, `COMMIT`, and `ROLLBACK`
statements, and modifies the transaction accordingly.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/sql/execution/session.rs#L34-L70

Any other statements are processed by the SQL planner, optimizer, and executor as we've seen in
previous sections.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/sql/execution/session.rs#L77-L83

These statements are always executed using the session's current transaction. If there is no active
transaction, the session will create a new, implicit transaction for each statement.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/sql/execution/session.rs#L87-L112

And with that, we have a fully functional SQL engine!

---

<p align="center">
← <a href="sql-optimizer.md">SQL Optimization</a> &nbsp; | &nbsp; <a href="server.md">Server</a> →
</p>