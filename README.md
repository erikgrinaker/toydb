# toyDB

Distributed SQL database in Rust, written as a learning project.

The primary goal is to build a minimally functional yet correct distributed database. Performance, security, reliability, and convenience are non-goals.

## Usage

A local five-node cluster can be started by running:

```sh
$ (cd sandbox && docker-compose up --build)
```

The nodes can be contacted on `localhost` ports `9601` to `9605`, e.g.:

```sh
$ grpcurl -plaintext -proto protobuf/toydb.proto localhost:9605 ToyDB/Status
```

## Project Outline

- [x] **Networking:** gRPC, no security.

- [x] **Consensus:** Self-written Raft implementation with strictly serializable reads and writes.

- [ ] **Storage:** Self-written key-value engine using B+-trees (and possibly LSM-trees), with secondary indexes. MessagePack for serialization. No log compaction or write-ahead log.

- [ ] **Data Types:** Support for nulls, booleans, signed 64-bit doubles, and short UTF-8 strings.

- [ ] **Constraints:** Compulsory singluar primary keys, unique indexes, and foreign keys.

- [ ] **Transactions:** MVCC-based serializable snapshot isolation.

- [ ] **Query Engine:** Simple heuristic-based planner and optimizer supporting expressions, functions, and inner joins.

- [ ] **Language:** Basic SQL support:

  * `[CREATE|DROP] TABLE ...` and `[CREATE|DROP] INDEX ...`
  * `BEGIN`, `COMMIT`, and `ROLLBACK`
  * `INSERT INTO ... (...) VALUES (...)`
  * `UPDATE ... SET ... WHERE ...`
  * `DELETE FROM ... WHERE ...`
  * `SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY ...`
  * `EXPLAIN SELECT ...`

- [ ] **Client:** Simple interactive REPL client over gRPC.

- [ ] **Verification:** [Jepsen](https://github.com/jepsen-io/jepsen) test suite.

## Known Issues

Below is an incomplete list of known issues preventing this from being a "real" database.

### Networking

* **No security:** all network traffic is unauthenticated and in plaintext; any request from any source is accepted.

### Raft

* **Cluster reconfiguration:** the Raft cluster must consist of a static set of nodes available via static IP addresses. It is not possible to resize the cluster without a full cluster restart.

* **Single node processing:** all operations (both reads and writes) are processed by a single Raft thread on a single node (the master), and the system consists of a single Raft cluster, preventing horizontal scalability and efficient resource utilization.

* **Client call retries:** there is currently no retries of client-submitted operations, and if a node processing or proxying an operations changes role then the call is dropped.

* **State machine errors:** errors during state machine mutations currently crash the node - it may be beneficial to support user errors which simply skip the erroring log entry.

* **Log replication optimization:** currently only the simplest version of the Raft log replication protocol is implemented, without snapshots or rapid log replay (i.e. replication of old log entries is retried one by one until a common base entry is found).