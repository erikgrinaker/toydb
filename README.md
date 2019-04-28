# toyDB

Distributed SQL database in Rust, written as a learning project.

## Project Goals

* **Storage:** Self-written engine using LSM-trees with B-trees for primary and secondary indexes (to learn both data structures). No log compaction or write-ahead log, unless necessary.

* **Networking:** gRPC over TCP/IP, using third-party library.

* **Consensus:** Self-written Raft implementation, handling all writes and reads.

* **Data Types:** Support for nulls, booleans, signed 64-bit doubles, and short UTF-8 strings.

* **Constraints:** Singular required primary keys, unique indexes, and foreign keys.

* **Transactions:** Serializable isolation with MVCC-based snapshot isolation.

* **Query Engine:** Simple heuristic-based planner and optimizer supporting expressions, functions, and inner joins.

* **Language:** Basic SQL support

  * `[CREATE|DROP] TABLE ...` and `[CREATE|DROP] INDEX ...`
  * `INSERT INTO [TABLE] (...) VALUES (...)`
  * `UPDATE [TABLE] SET ... WHERE ...`
  * `DELETE FROM [TABLE] WHERE ...`
  * `SELECT ... FROM ... WHERE ... ORDER BY ...`
  * `EXPLAIN SELECT ...`
