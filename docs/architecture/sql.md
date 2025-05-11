# SQL Engine

The SQL engine provides support for the SQL query language, and is the main database interface. It
uses the key/value store for data storage, MVCC for transactions, and Raft for replication. The SQL
engine itself consists of several distinct components that form a pipeline:

> Query → Session → Lexer → Parser → Planner → Optimizer → Executor → Storage

The SQL engine is located in the [`sql`](https://github.com/erikgrinaker/toydb/tree/b2fe7b76ee634ca6ad31616becabfddb1c03d34b/src/sql)
module. We'll discuss each of the components in a bottom-up manner.

The SQL engine is tested as a whole by test scripts under
[`src/sql/testscripts`](https://github.com/erikgrinaker/toydb/tree/9419bcf6aededf0e20b4e7485e2a5fa3e975d79f/src/sql/testscripts).
These typically take a raw SQL string as input, execute them against an in-memory storage engine,
and output the result along with intermediate state such as the query plan, storage operations,
and stored binary key/value data.

---

<p align="center">
← <a href="raft.md">Raft Consensus</a> &nbsp; | &nbsp; <a href="sql-data.md">SQL Data Model</a> →
</p>