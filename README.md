# <a><img src="./docs/images/toydb.svg" height="40" valign="top" /></a> toyDB

[![CI](https://github.com/erikgrinaker/toydb/actions/workflows/ci.yml/badge.svg)](https://github.com/erikgrinaker/toydb/actions/workflows/ci.yml)

Distributed SQL database in Rust, written as a learning project. Most components are built from
scratch, including:

* Raft-based distributed consensus engine for linearizable state machine replication.

* ACID-compliant transaction engine with MVCC-based snapshot isolation.

* Pluggable storage engine with BitCask and in-memory backends.

* Iterator-based query engine with heuristic optimization and time-travel support.

* SQL interface including projections, filters, joins, aggregates, and transactions.

toyDB is not suitable for real-world use, but may be of interest to others learning about
database internals.

## Documentation

* [Architecture guide](docs/architecture.md): a guide to toyDB's architecture and implementation.

* [SQL examples](docs/examples.md): comprehensive examples of toyDB's SQL features.

* [SQL reference](docs/sql.md): detailed reference documentation for toyDB's SQL dialect.

* [References](docs/references.md): books and other research material used while building toyDB.

## Usage

With a [Rust compiler](https://www.rust-lang.org/tools/install) installed, a local five-node 
cluster can be started on `localhost` ports `9601` to `9605`, with data under `cluster/*/data`:

```
$ ./cluster/run.sh
```

A command-line client can be built and used with node 5 on `localhost:9605`:

```
$ cargo run --release --bin toysql
Connected to toyDB node "toydb-e". Enter !help for instructions.
toydb> CREATE TABLE movies (id INTEGER PRIMARY KEY, title VARCHAR NOT NULL);
toydb> INSERT INTO movies VALUES (1, 'Sicario'), (2, 'Stalker'), (3, 'Her');
toydb> SELECT * FROM movies;
1|Sicario
2|Stalker
3|Her
```

toyDB supports most common SQL features, including joins, aggregates, and ACID transactions.

## Architecture

[![toyDB architecture](./docs/images/architecture.svg)](./docs/architecture.md)

toyDB's architecture is fairly typical for distributed SQL databases: a transactional
key/value store managed by a Raft cluster with a SQL query engine on top. See the
[architecture guide](./docs/architecture.md) for more details.

## Tests

toyDB has decent test coverage, with about a thousand tests of core functionality. These consist
of in-code unit-tests for many low-level components, golden master integration tests of the SQL
engine under [`tests/sql`](https://github.com/erikgrinaker/toydb/tree/master/tests/sql), and a
basic set of end-to-end cluster tests under
[`tests/`](https://github.com/erikgrinaker/toydb/tree/master/tests).
[Jepsen tests](https://jepsen.io), or similar system-wide correctness and reliability tests, are 
desirable but not yet implemented.

Execute `cargo test` to run all tests, or check out the latest
[CI run](https://github.com/erikgrinaker/toydb/actions/workflows/ci.yml).

## Benchmarks

toyDB is not optimized for performance, but it comes with a `workload` benchmarking tool that can
run various workloads against a toyDB cluster. For example:

```sh
# Start a 5-node toyDB cluster.
$ ./cluster/run.sh
[...]

# Run a read-only benchmark via all 5 nodes.
$ cargo run --release --bin workload read
Preparing initial dataset... done (0.096s)
Spawning 16 workers... done (0.003s)
Running workload read (rows=1000 size=64 batch=1)...

Time   Progress     Txns      Rate       p50       p90       p99      pMax
1.0s       7.2%     7186    7181/s     2.3ms     3.1ms     4.0ms     9.6ms
2.0s      14.4%    14416    7205/s     2.3ms     3.1ms     4.2ms     9.6ms
3.0s      22.5%    22518    7504/s     2.2ms     2.9ms     4.0ms     9.6ms
4.0s      30.3%    30303    7574/s     2.2ms     2.9ms     3.8ms     9.6ms
5.0s      38.2%    38200    7639/s     2.2ms     2.8ms     3.7ms     9.6ms
6.0s      46.0%    45961    7659/s     2.2ms     2.8ms     3.7ms     9.6ms
7.0s      53.3%    53343    7620/s     2.2ms     2.8ms     3.7ms     9.6ms
8.0s      61.2%    61220    7651/s     2.2ms     2.8ms     3.6ms     9.6ms
9.0s      68.2%    68194    7576/s     2.2ms     2.8ms     3.7ms     9.6ms
10.0s     75.8%    75800    7579/s     2.2ms     2.8ms     3.7ms     9.6ms
11.0s     82.9%    82864    7533/s     2.2ms     2.9ms     3.7ms    18.2ms
12.0s     90.6%    90583    7548/s     2.2ms     2.9ms     3.7ms    18.2ms
13.0s     98.3%    98311    7562/s     2.2ms     2.9ms     3.7ms    18.2ms
13.2s    100.0%   100000    7569/s     2.2ms     2.9ms     3.7ms    18.2ms

Verifying dataset... done (0.001s)
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
read       13.2s    100000    7569/s     2.2ms     2.9ms     3.7ms    18.2ms
write      22.2s    100000    4502/s     3.9ms     4.5ms     4.9ms    15.7ms
bank       155.0s   100000     645/s    16.9ms    41.7ms    95.0ms  1044.4ms
```

## Debugging

[VSCode](https://code.visualstudio.com) provides a very intuitive environment for debugging toyDB.
The debug configuration is included under `.vscode/launch.json`. Follow these steps to set it up:

1. Install the [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb)
   extension.

2. Go to "Run and Debug" tab and select e.g. "Debug unit tests in library 'toydb'".

3. To debug the binary, select "Debug executable 'toydb'" under "Run and Debug".

## Credits

toyDB logo is courtesy of [@jonasmerlin](https://github.com/jonasmerlin).