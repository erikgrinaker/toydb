# toyDB

toyDB is a distributed SQL database in Rust, built from scratch as an educational project. Main
features:

* Raft distributed consensus for linearizable state machine replication.

* ACID transactions with MVCC-based snapshot isolation.

* Pluggable storage engine with BitCask and in-memory backends.

* Iterator-based query engine with heuristic optimization and time-travel  support.

* SQL interface including joins, aggregates, and transactions.

toyDB is not distributed as a crate, see <https://github.com/erikgrinaker/toydb> for more.

This crate used to contain the [joydb](https://crates.io/crates/joydb) database. Thanks to Serhii
Potapov for donating the crate name.