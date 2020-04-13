# toyDB

[![Build Status](https://cloud.drone.io/api/badges/erikgrinaker/toydb/status.svg)](https://cloud.drone.io/erikgrinaker/toydb)

Distributed SQL database in Rust, written as a learning project. Most components have been built from scratch, including:

* Raft-based distributed consensus engine for linearizable state machine replication.

* ACID-compliant transaction engine with MVCC-based snapshot isolation.

* Iterator-based query engine with heuristic optimization, secondary indexes, and time-travel support.

* SQL interface including projections, filters, joins, and aggregates.

For details, see the [reference documentation](REFERENCE.md).

## Usage

A local five-node cluster can be started on `localhost` ports `9601` to `9605` by running:

```
# Local processes
$ (cd clusters/local && ./run.sh)

# Docker containers 
$ (cd clusters/docker && docker-compose up --build)
```

A command-line REPL client can be built and used with the node on `localhost` port `9605`
by running:

```
$ cargo run --bin toysql
Connected to node "toydb-e" (version 0.1.0). Enter !help for instructions.
toydb>
```

A basic subset of SQL has been partially implemented, e.g.:

```
toydb> CREATE TABLE movies (id INTEGER PRIMARY KEY, title VARCHAR NOT NULL)
toydb> INSERT INTO movies VALUES (1, 'Sicario'), (2, 'Stalker'), (3, 'Her')
toydb> SELECT * FROM movies
1|Sicario
2|Stalker
3|Her
```

ACID transactions are supported via snapshot isolation:

```
toydb> BEGIN
Began transaction 3
toydb:3> INSERT INTO movies VALUES (4, 'Alien vs. Predator')
toydb:3> ROLLBACK
Rolled back transaction 3

toydb> BEGIN
Began transaction 4
toydb:4> INSERT INTO movies VALUES (4, 'Alien'), (5, 'Predator')
toydb:4> COMMIT
Committed transaction 4

toydb> SELECT * FROM movies
1|Sicario
2|Stalker
3|Her
4|Alien
5|Predator
```

Time-travel queries are also supported:

```
toydb> BEGIN TRANSACTION READ ONLY AS OF SYSTEM TIME 2
Began read-only transaction in snapshot of version 2
toydb@1> SELECT * FROM movies
1|Sicario
2|Stalker
3|Her
```

## Known Issues

The primary goal is to build a minimally functional yet correct distributed database. Performance, security, reliability, and convenience are non-goals. Below is an incomplete list of known issues preventing this from being a "real" database.

### Networking

* **No security:** all network traffic is unauthenticated and in plaintext; any request from any source is accepted.

### Raft

* **Cluster reconfiguration:** the Raft cluster must consist of a static set of nodes available via static IP addresses. It is not possible to resize the cluster without a full cluster restart.

* **Single node processing:** all operations (both reads and writes) are processed by a single Raft thread on a single node (the master), and the system consists of a single Raft cluster, preventing horizontal scalability and efficient resource utilization.

* **Client call retries:** there is currently no retries of client-submitted operations, and if a node processing or proxying an operation changes role then the call is dropped.

* **Log replication optimization:** currently only the simplest version of the Raft log replication protocol is implemented, without snapshots or rapid log replay (i.e. replication of old log entries is retried one by one until a common base entry is found).

### Storage

* **Recovery:** there is no WAL or other form of crash recovery - unexpected termination is likely to corrupt data.

* **Garbage collection:** old Raft log entries or MVCC versions are never removed, giving unbounded disk usage but also unbounded data history.

### Transactions

* **Abortion:** client disconnects or system crashes do not abort transactions, which may cause serialization failures for other transactions due to uncommitted writes.

* **Transient reads:** all statements, including trivial `SELECT`s, will be wrapped in a transaction with a globally allocated, persistent transaction ID.

* **Serializability:** transactions use MVCC without serializable snapshot isolation, and are thus vulnerable to write skew anomalies.

* **ID overflow:** transaction IDs are stored as unsigned 64-bit integers without wraparound, thus no new transactions can be started beyond transaction number 2⁶⁴-1.

### Schema

* **Single database:** only a single, unnamed database is supported per ToyDB cluster.

* **Schema changes:** schema changes other than creating or dropping tables and indexes is not supported, i.e. there is no `ALTER TABLE`.

### Query Engine

* **Type checking:** query type checking (e.g. `SELECT a + b` must receive two numbers) is done at query evaluation time, not at query compile time.

* **Streamed evaluation:** it's not possible to operate on fields that were not output by a preceding plan node - e.g. it's only possible to order on `SELECT` output columns.
