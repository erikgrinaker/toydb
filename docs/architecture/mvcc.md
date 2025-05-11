# MVCC Transactions

Transactions are groups of reads and writes (e.g. to different keys) that are submitted together as
a single unit. For example, a bank transaction that transfers $100 from account A to account B might
consist of this group of reads and writes:

```
a = get(A)
b = get(B)
if a < 100:
    error("insufficient balance")
set(A, a - 100)
set(B, b + 100)
```

toyDB provides [ACID](https://en.wikipedia.org/wiki/ACID) transactions, a set of very strong
guarantees:

* **Atomicity:** all of the writes take effect as an single, atomic unit, at the same instant, when
  they are _committed_. Other users will never see some of the writes without the others.

* **Consistency:** database constraints are never violated (e.g. referential integrity or uniqueness
  contraints). We'll see how this is implemented later in the SQL execution layer.

* **Isolation:** users should appear to have the entire database to themselves, unaffected by other
  simultaneous users. Two transactions may conflict, in which case one has to retry, but if a
  transaction succeeds then the user knows with certainty that the operations were executed without
  interference by anyone else. This eliminates the risk of [race conditions](https://en.wikipedia.org/wiki/Race_condition).
  
* **Durability:** committed writes are never lost (even if the system crashes).

To illustrate how transactions work, here's an example MVCC test script where two concurrent users
modify a set of bank accounts (there's many [other test scripts](https://github.com/erikgrinaker/toydb/tree/aa14deb71f650249ce1cab8828ed7bcae2c9206e/src/storage/testscripts/mvcc)
there too):

https://github.com/erikgrinaker/toydb/blob/a73e24b7e77671b9f466e0146323cd69c3e27bdf/src/storage/testscripts/mvcc/bank#L1-L69

To provide these guarantees, toyDB uses a common technique called
[Multi-Version Concurrency Control](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
(MVCC). It is implemented at the key/value storage level, in the [`storage::mvcc`](https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs)
module. It uses a `storage::Engine` for actual data storage.

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L220-L231

MVCC provides an [isolation level](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Isolation_levels)
called [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation): a transaction sees a
snapshot of the database as it was when the transaction began. Any later changes are invisible to
it.

It does this by storing historical versions of key/value pairs. The version number is simply a
number that's incremented for every new transaction:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L155-L158

Each transaction has its own unique version number. When it writes a key/value pair it appends its
version number to the key as `Key::Version(&[u8], Version)` (using the Keycode encoding we've seen
previously). If an old version of the key already exists, it will have a different version number
suffix and therefore be stored as a separate key in the storage engine. Deleted keys are versions
with a special tombstone value.

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L183-L189

Here's a simple diagram of what a history of versions 1 to 5 of keys `a` to `d` might look like:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L11-L26

Additionally, we need to keep track of the currently ongoing (uncommitted) transaction versions,
known as the "active set".

With versioning and the active set, we can summarize the MVCC protocol with a few simple rules:

1. When a new transaction begins, it:
    * Obtains the next available version number.
    * Takes a snapshot of the active set (other uncommitted transactions).
    * Adds its version number to the active set.

2. When the transaction reads a key, it:
    * Returns the latest version of the key at or below its own version.
    * Ignores versions above its own version.
    * Ignores versions in its active set snapshot.

3. When the transaction writes a key, it:
    * Looks for a key version above its own version; errors if found.
    * Looks for a key version in its active set snapshot; errors if found.
    * Writes a key/value pair with its own version.

4. When the transaction commits, it:
    * Flushes all writes to disk.
    * Removes itself from the active set.

The magic happens when the transaction removes itself from the active set. This is a single, atomic
operation, and when it completes all of its writes immediately become visible to _new_ transactions.
However, ongoing transactions still won't see these writes, because the version is still in their
active set snapshot or at a later version (hence they are isolated from this transaction).

Furthermore, the transaction could see its own uncommitted writes even though noone else could, and
if any writes conflicted with another transaction it would error out and have to retry.

Not only that, this also allows us to do time-travel queries, where we can query the database as it
was at any time in the past: we simply pick a version number to read at.

There are a few more details that we've left out here: transaction rollbacks need to keep track of
the writes and undo them, and read-only queries can avoid allocating new version numbers. We also
don't garbage collect old version, for simplicity. See the module documentation for more details:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L1-L140

Let's walk through a simple example with code pointers to get a feel for how this is implemented.
Notice how we don't have to deal with any version numbers when we're using the MVCC API -- this is
an internal MVCC implementation detail.

```rust
// Open a BitCask database in the file "toy.db" with MVCC support.
let path = PathBuf::from("toy.db");
let db = MVCC::new(BitCask::new(path)?);

// Begin a new transaction.
let txn = db.begin()?;

// Read the key "foo", and decode the binary value as a u64 with bincode.
let bytes = txn.get(b"foo")?.expect("foo not found");
let mut value: u64 = bincode::deserialize(&bytes)?;

// Delete "foo".
txn.delete(b"foo")?;

// Add 1 to the value, and write it back to the key "bar".
value += 1;
let bytes = bincode::serialize(&value);
txn.set(b"bar", bytes)?;

// Commit the transaction.
txn.commit()?;
```

First, we begin a new transaction with `MVCC::begin()`, which calls through to
`Transaction::begin()`. This obtains a version number stored in `Key::NextVersion` and increments
it, then takes a snapshot of the active set in `Key::ActiveSet` and adds itself to it:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L368-L391

This returns a `Transaction` object which provides the main key/value API, with get/set/delete
methods. It keeps track of the main state of the transaction: it's version number and active set.

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L294-L327

Next, we call `Transaction::get(b"foo")` to read the value of the key `foo`. This finds the latest
version that's visible to us (ignoring future versions and the active set). Recall that we store
multiple version of each key as `Key::Version(key, version)`. The Keycode encoding ensures that all
versions are stored in sorted order, so we can do a reverse range scan from `Key::Version(b"foo",
self.version)` to  `Key::Version(b"foo", 0)` and return the latest version that's visible to us:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L564-L581

We then call `Transaction::delete(b"foo")` and `Transaction::set(b"bar", value)`. Both of these just
call through to the same `Transaction::write_version()` method, but use `Some(value)` for a regular
key/value pair and `None` as a deletion tombstone:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L514-L522

To write a new version of a key, we first have to check for conflicts by seeing if there's a
version of the key that's invisible to us -- if it is, we conflicted with a concurrent transaction.
We use a range scan for this, like we did in `Transaction::get()`.

If there are no conflicts, we go on to write `Key::Version(b"foo", self.version)` and encode the
value as an `Option<value>` to accomodate the `None` tombstone marker. We also write a
`Key::TxnWrite(version, key)` to keep track of the keys we've written in case we have to roll back.

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L524-L562

Finally, `Transaction::commit()` will make our transaction take effect and become visible. It does
this simply by removing itself from the active set in `Key::ActiveSet`, and also cleaning up its
`Key::TxnWrite` write tracking. As the comment says, we don't actually have to flush to durable
storage here, because the Raft log will provide durability for us -- we'll get back to this later.

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L466-L485

---

<p align="center">
← <a href="encoding.md">Key/Value Encoding</a> &nbsp; | &nbsp; <a href="raft.md">Raft Consensus</a> →
</p>