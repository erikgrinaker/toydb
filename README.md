# <a><img src="./docs/images/toydb.svg" height="40" valign="top" /></a> toyDB

[![CI](https://github.com/erikgrinaker/toydb/actions/workflows/ci.yml/badge.svg)](https://github.com/erikgrinaker/toydb/actions/workflows/ci.yml)

Distributed SQL database in Rust, written as an educational project. Built from scratch, including:

* [Raft distributed consensus engine][raft] for linearizable state machine replication.

* [ACID transaction engine][txn] with MVCC-based snapshot isolation.

* [Pluggable storage engine][storage] with [BitCask][bitcask] and [in-memory][memory] backends.

* [Iterator-based query engine][query] with [heuristic optimization][optimizer] and time-travel 
  support.

* [SQL interface][sql] including joins, aggregates, and transactions.

toyDB is intended to illustrate the overall architecture and concepts of distributed SQL databases.
It should be functional and correct, but focuses on simplicity and understandability. In particular,
performance, scalability, and availability are explicit non-goals -- these are major sources of
complexity in production-grade databases, which obscur the basic underlying concepts. Shortcuts have
been taken wherever possible.

toyDB is not suitable for real-world use.

[raft]: https://github.com/erikgrinaker/toydb/blob/master/src/raft/mod.rs
[txn]: https://github.com/erikgrinaker/toydb/blob/master/src/storage/mvcc.rs
[storage]: https://github.com/erikgrinaker/toydb/blob/master/src/storage/engine.rs
[bitcask]: https://github.com/erikgrinaker/toydb/blob/master/src/storage/bitcask.rs
[memory]: https://github.com/erikgrinaker/toydb/blob/master/src/storage/memory.rs
[query]: https://github.com/erikgrinaker/toydb/blob/master/src/sql/planner/plan.rs
[optimizer]: https://github.com/erikgrinaker/toydb/blob/master/src/sql/planner/optimizer.rs
[sql]: https://github.com/erikgrinaker/toydb/blob/master/src/sql/mod.rs

## Documentation

* [Architecture guide](docs/architecture.md): overview of toyDB's architecture and implementation.

* [SQL examples](docs/examples.md): walkthrough of toyDB's SQL features.

* [SQL reference](docs/sql.md): toyDB SQL reference documentation.

* [References](docs/references.md): books and other material used while building toyDB.

## Usage

With a [Rust compiler](https://www.rust-lang.org/tools/install) installed, a local five-node 
cluster can be started on `localhost` ports `9601` to `9605`, with data under `cluster/*/data`:

```
$ ./cluster/run.sh
```

A command-line client can be built and used with node 5 on `localhost:9605`:

```
$ cargo run --release --bin toysql
Connected to toyDB node n5. Enter !help for instructions.
toydb> CREATE TABLE movies (id INTEGER PRIMARY KEY, title VARCHAR NOT NULL);
toydb> INSERT INTO movies VALUES (1, 'Sicario'), (2, 'Stalker'), (3, 'Her');
toydb> SELECT * FROM movies;
1, 'Sicario'
2, 'Stalker'
3, 'Her'
```

toyDB supports most common SQL features, including joins, aggregates, and ACID transactions.

## Architecture

[![toyDB architecture](./docs/images/architecture.svg)](./docs/architecture.md)

toyDB's architecture is fairly typical for a distributed SQL database: a transactional
key/value store managed by a Raft cluster with a SQL query engine on top. See the
[architecture guide](./docs/architecture.md) for more details.

## Tests

toyDB mostly uses [Goldenscripts](https://github.com/erikgrinaker/goldenscript) for tests. These 
are used to script various scenarios, capture events and output, and later assert that the
behavior remains the same. See e.g.:

* [Raft cluster tests](https://github.com/erikgrinaker/toydb/tree/master/src/raft/testscripts/node)
* [MVCC transaction tests](https://github.com/erikgrinaker/toydb/tree/master/src/storage/testscripts/mvcc)
* [SQL execution tests](https://github.com/erikgrinaker/toydb/tree/master/src/sql/testscripts)
* [End-to-end tests](https://github.com/erikgrinaker/toydb/tree/master/tests/scripts)

Run tests with `cargo test`, or have a look at the latest 
[CI run](https://github.com/erikgrinaker/toydb/actions/workflows/ci.yml).

## Benchmarks

toyDB is not optimized for performance, but comes with a `workload` benchmark tool that can run 
various workloads against a toyDB cluster. For example:

```sh
# Start a 5-node toyDB cluster.
$ ./cluster/run.sh
[...]

# Run a read-only benchmark via all 5 nodes.
$ cargo run --release --bin workload read
Preparing initial dataset... done (0.179s)
Spawning 16 workers... done (0.006s)
Running workload read (rows=1000 size=64 batch=1)...

Time   Progress     Txns      Rate       p50       p90       p99      pMax
1.0s      13.1%    13085   13020/s     1.3ms     1.5ms     1.9ms     8.4ms
2.0s      27.2%    27183   13524/s     1.3ms     1.5ms     1.8ms     8.4ms
3.0s      41.3%    41301   13702/s     1.2ms     1.5ms     1.8ms     8.4ms
4.0s      55.3%    55340   13769/s     1.2ms     1.5ms     1.8ms     8.4ms
5.0s      70.0%    70015   13936/s     1.2ms     1.5ms     1.8ms     8.4ms
6.0s      84.7%    84663   14047/s     1.2ms     1.4ms     1.8ms     8.4ms
7.0s      99.6%    99571   14166/s     1.2ms     1.4ms     1.7ms     8.4ms
7.1s     100.0%   100000   14163/s     1.2ms     1.4ms     1.7ms     8.4ms

Verifying dataset... done (0.002s)
```

The available workloads are:

* `read`: single-row primary key lookups.
* `write`: single-row inserts to sequential primary keys.
* `bank`: makes bank transfers between various customers and accounts. To make things interesting,
  this includes joins, secondary indexes, sorting, and conflicts.

For more information about workloads and parameters, run `cargo run --bin workload -- --help`.

Example workload results:

```
Workload   Time       Txns      Rate       p50       p90       p99      pMax
read       7.1s     100000   14163/s     1.2ms     1.4ms     1.7ms     8.4ms
write      22.2s    100000    4502/s     3.9ms     4.5ms     4.9ms    15.7ms
bank       155.0s   100000     645/s    16.9ms    41.7ms    95.0ms  1044.4ms
```

## Debugging

[VSCode](https://code.visualstudio.com) provides an intuitive environment for debugging toyDB.
The debug configuration is included under `.vscode/launch.json`, to use it:

1. Install the [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb)
   extension.

2. Go to the "Run and Debug" tab and select e.g. "Debug unit tests in library 'toydb'".

3. To debug the binary, select "Debug executable 'toydb'" under "Run and Debug".

## Credits

toyDB logo is courtesy of [@jonasmerlin](https://github.com/jonasmerlin).