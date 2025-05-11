# toyDB Architecture

toyDB is a simple distributed SQL database, intended to illustrate how such systems are built. The
overall structure is similar to real-world distributed databases, but the design and implementation
has been kept as simple as possible for understandability. Performance and scalability are explicit
non-goals, as these are major sources of complexity in real-world systems.

This guide will walk through toyDB's architecture and code from the bottom up, with plenty of links
to the actual source code.

> ℹ️ View on GitHub with a desktop browser for inline code listings.

* [Overview](overview.md)
  * [Properties](overview.md#properties)
  * [Components](overview.md#components)
* [Storage Engine](storage.md)
  * [`Memory` Storage Engine](storage.md#memory-storage-engine)
  * [`BitCask` Storage Engine](storage.md#bitcask-storage-engine)
* [Key/Value Encoding](encoding.md)
  * [`Bincode` Value Encoding](encoding.md#bincode-value-encoding)
  * [`Keycode` Key Encoding](encoding.md#keycode-key-encoding)
* [MVCC Transactions](mvcc.md)
* [Raft Consensus](raft.md)
  * [Log Storage](raft.md#log-storage)
  * [State Machine Interface](raft.md#state-machine-interface)
  * [Node Roles](raft.md#node-roles)
  * [Node Interface and Communication](raft.md#node-interface-and-communication)
  * [Leader Election and Terms](raft.md#leader-election-and-terms)
  * [Client Requests and Forwarding](raft.md#client-requests-and-forwarding)
  * [Write Replication and Application](raft.md#write-replication-and-application)
  * [Read Processing](raft.md#read-processing)
* [SQL Engine](sql.md)
  * [Data Model](sql-data.md)
    * [Data Types](sql-data.md#data-types)
    * [Schemas](sql-data.md#schemas)
    * [Expressions](sql-data.md#expressions)
  * [Storage](sql-storage.md)
    * [Key/Value Representation](sql-storage.md#keyvalue-representation)
    * [Schema Catalog](sql-storage.md#schema-catalog)
    * [Row Storage and Transactions](sql-storage.md#row-storage-and-transactions)
  * [Raft Replication](sql-raft.md)
  * [Parsing](sql-parser.md)
    * [Lexer](sql-parser.md#lexer)
    * [Abstract Syntax Tree](sql-parser.md#abstract-syntax-tree)
    * [Parser](sql-parser.md#parser)
  * [Planning](sql-planner.md)
    * [Execution Plan](sql-planner.md#execution-plan)
    * [Scope and Name Resolution](sql-planner.md#scope-and-name-resolution)
    * [Planner](sql-planner.md#planner)
  * [Optimization](sql-optimizer.md)
    * [Constant Folding](sql-optimizer.md#constant-folding)
    * [Filter Pushdown](sql-optimizer.md#filter-pushdown)
    * [Index Lookups](sql-optimizer.md#index-lookups)
    * [Hash Join](sql-optimizer.md#hash-join)
    * [Short Circuiting](sql-optimizer.md#short-circuiting)
  * [Execution](sql-execution.md)
    * [Plan Executor](sql-execution.md#plan-executor)
    * [Session Management](sql-execution.md#session-management)
* [Server and Client](server.md)
  * [Server](server.md#server)
  * [Raft Routing](server.md#raft-routing)
  * [SQL Service](server.md#sql-service)
  * [`toydb` Binary](server.md#toydb-binary)
  * [Client Library](server.md#client-library)
  * [`toysql` Binary](server.md#toysql-binary)

---

<p align="center">
<a href="overview.md">Overview</a> →
</p>