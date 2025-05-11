# Storage Engine

toyDB uses an embedded [key/value store](https://en.wikipedia.org/wiki/Key–value_database) for data
storage, located in the [`storage`](https://github.com/erikgrinaker/toydb/tree/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/storage)
module. This stores arbitrary keys and values as binary byte strings. The storage engine doesn't
know or care what the keys and values contain -- we'll see later how the SQL data model, with tables
and rows, is mapped onto this key/value structure.

The storage engine supports simple set/get/delete operations on individual keys. It does not itself
support transactions -- this is built on top, and we'll get back to it shortly.

Keys are stored in sorted order. This allows range scans, where we can iterate over all key/value
pairs between two specific keys, or with a specific key prefix. This will be needed by other
components in the system, e.g. to scan all rows in a specific SQL table, to scan all versions of an
MVCC key, to scan the tail of the Raft log, etc.

The storage engine is pluggable: there are multiple implementations, and the user can choose which
one to use in the config file. These implement the `storage::Engine` trait:

https://github.com/erikgrinaker/toydb/blob/4804df254034c51f367d1380d389d80695cd7054/src/storage/engine.rs#L8-L58

Let's look at the existing storage engine implementations.

## `Memory` Storage Engine

The simplest storage engine is the `storage::Memory` engine. This is a trivial implementation which
stores data in memory using the Rust standard library's
[`BTreeMap`](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html), without persisting
it to disk. It is primarily used for testing.

Since this is just a wrapper around the `BTreeMap` we can include it in its entirety here:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/memory.rs#L8-L77

## `BitCask` Storage Engine

The main storage engine is `storage::BitCask`. This is a very simple variant of
[BitCask](https://riak.com/assets/bitcask-intro.pdf), used in the [Riak](https://riak.com/)
database. It is kind of like the [LSM-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)'s
baby cousin.

https://github.com/erikgrinaker/toydb/blob/3e467512dca55843f0b071b3e239f14724f59a41/src/storage/bitcask.rs#L15-L55

toyDB's BitCask implementation uses a single append-only log file for storage. To write a key/value
pair, we simply append it to the file. To delete a key, we append a special tombstone value. When
reading a key, the last entry for that key in the file is used.

The file format for a key/value pair is simply:

1. The key length, as a big-endian `u32` (4 bytes).
2. The value length, as a big-endian `i32` (4 bytes). -1 if tombstone.
3. The binary key (n bytes).
4. The binary value (n bytes).

For example, the key/value pair `foo=bar` would be written as follows (in hexadecimal):

```
keylen   valuelen key    value
00000003 00000003 666f6f 626172
```

Because the data file is a simple log, we don't need a separate [write-ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging)
for crash recovery -- the data file _is_ the write-ahead log.

To quickly look up key/value pairs when reading, we maintain an in-memory `KeyDir` index which maps
a key to the latest value's position in the file. All keys must therefore fit in memory.

https://github.com/erikgrinaker/toydb/blob/3e467512dca55843f0b071b3e239f14724f59a41/src/storage/bitcask.rs#L57-L65

We initially generate this index by scanning through the entire file when it is opened:

https://github.com/erikgrinaker/toydb/blob/3e467512dca55843f0b071b3e239f14724f59a41/src/storage/bitcask.rs#L267-L332

To write a key, we append it to the file and update the `KeyDir`:

https://github.com/erikgrinaker/toydb/blob/3e467512dca55843f0b071b3e239f14724f59a41/src/storage/bitcask.rs#L155-L159

https://github.com/erikgrinaker/toydb/blob/3e467512dca55843f0b071b3e239f14724f59a41/src/storage/bitcask.rs#L342-L366

To delete a key, we append a tombstone value instead:

https://github.com/erikgrinaker/toydb/blob/3e467512dca55843f0b071b3e239f14724f59a41/src/storage/bitcask.rs#L122-L126

To read a value for a key, we look up the key's file location in the `KeyDir` index (if the key
exists), and then read it from the file:

https://github.com/erikgrinaker/toydb/blob/3e467512dca55843f0b071b3e239f14724f59a41/src/storage/bitcask.rs#L334-L340

The `KeyDir` uses an inner stdlib `BTreeMap` to keep track of keys. This allows range scans, where
we iterate over a sorted set of keys between the range bounds, loading each key from the file:

https://github.com/erikgrinaker/toydb/blob/3e467512dca55843f0b071b3e239f14724f59a41/src/storage/bitcask.rs#L144-L146

https://github.com/erikgrinaker/toydb/blob/3e467512dca55843f0b071b3e239f14724f59a41/src/storage/bitcask.rs#L207-L225

As keys are updated and deleted, we'll keep accumulating old versions in the log file. To remove
these, the log file is compacted on startup. This writes out the latest value of every live
key/value pair to a new file, and replaces the old file. The keys are written in sorted order, to
make later scans faster.

https://github.com/erikgrinaker/toydb/blob/3e467512dca55843f0b071b3e239f14724f59a41/src/storage/bitcask.rs#L172-L195

---

<p align="center">
← <a href="overview.md">Overview</a> &nbsp; | &nbsp; <a href="encoding.md">Key/Value Encoding</a> →
</p>