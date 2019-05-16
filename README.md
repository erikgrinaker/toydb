# toyDB

Distributed SQL database in Rust, written as a learning project.

## Project Outline

- [x] **Networking:** gRPC, no security.

- [ ] **Storage:** Self-written key-value engine using LSM-trees and/or B+-trees, with secondary indexes. MessagePack for serialization. No log compaction or write-ahead log.

- [ ] **Consensus:** Self-written Raft implementation, handling all writes and reads. Node time is (naively) considered accurate.

- [ ] **Data Types:** Support for nulls, booleans, signed 64-bit doubles, and short UTF-8 strings.

- [ ] **Constraints:** Singular required primary keys, unique indexes, and foreign keys.

- [ ] **Transactions:** MVCC-based serializable snapshot isolation.

- [ ] **Query Engine:** Simple heuristic-based planner and optimizer supporting expressions, functions, and inner joins.

- [ ] **Language:** Basic SQL support

  * `[CREATE|DROP] TABLE ...` and `[CREATE|DROP] INDEX ...`
  * `BEGIN`, `COMMIT`, and `ROLLBACK`
  * `INSERT INTO [TABLE] (...) VALUES (...)`
  * `UPDATE [TABLE] SET ... WHERE ...`
  * `DELETE FROM [TABLE] WHERE ...`
  * `SELECT ... FROM ... WHERE ... ORDER BY ...`
  * `EXPLAIN SELECT ...`

- [ ] **Client:** Simple interactive REPL client over gRPC.
