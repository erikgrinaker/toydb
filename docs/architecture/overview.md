# Overview

toyDB consists of a cluster of nodes that execute [SQL](https://en.wikipedia.org/wiki/SQL)
transactions against a replicated state machine. Clients can connect to any node in the cluster and
submit SQL statements. The cluster remains available if a minority of nodes crash or disconnect,
but halts if a majority of nodes fail.

## Properties

* **Distributed:** runs across a cluster of nodes.
* **Highly available:** tolerates failure of a minority of nodes.
* **SQL compliant:** correctly supports most common [SQL](https://en.wikipedia.org/wiki/SQL)
  features.
* **Strongly consistent:** committed writes are immediately visible to all readers ([linearizability](https://en.wikipedia.org/wiki/Linearizability)).
* **Transactional:** provides [ACID](https://en.wikipedia.org/wiki/ACID) transactions
  * **Atomic:** groups of writes are applied as a single, atomic unit.
  * **Consistent:** database constraints and referential integrity are always enforced.
  * **Isolated:** concurrent transactions don't affect each other ([snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation)).
  * **Durable:** committed writes are never lost.

For simplicity, toyDB is:

* **Not scalable:** every node stores the full dataset, and reads/writes execute on one node.
* **Not reliable:** only handles crash failures, not e.g. partial network partitions or node stalls.
* **Not performant:** data processing is slow, and not optimized at all.
* **Not efficient:** loads entire tables into memory, no compression or garbage collection, etc.
* **Not full-featured:** only basic SQL functionality is implemented.
* **Not backwards compatible:** changes to data formats and protocols will break databases.
* **Not flexible:** nodes can't be added or removed while running, and take a long time to join.
* **Not secure:** there is no authentication, authorization, nor encryption.

## Components

Internally, toyDB is made up of a few main components:

* **Storage engine:** stores data on disk and manages transactions.
* **Raft consensus engine:** replicates data and coordinates cluster nodes.
* **SQL engine:** organizes SQL data, manages SQL sessions, and executes SQL statements.
* **Server:** manages network communication, both with SQL clients and Raft nodes.
* **Client:** provides a SQL user interface and communicates with the server.

This diagram illustrates the internal structure of a single toyDB node:

![toyDB architecture](./images/architecture.svg)

We will go through each of these components from the bottom up.

---

<p align="center">
← <a href="index.md">toyDB Architecture</a> &nbsp; | &nbsp; <a href="storage.md">Storage Engine</a> →
</p>