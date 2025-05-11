# MVCC Transactions

Transactions provide _atomicity_: a user can submit multiple writes which will take effect as a
single group, at the same instant, when they are _committed_. Other users should never see some of
the writes without the others. And they provide _durability_: committed writes should never be lost
(even if the system crashes), and should remain visible.

Transactions also provide _isolation_: they should appear to have the entire database to themselves,
unaffected by what other users may be doing at the same time. Two transactions may conflict, in
which case one has to retry, but if a transaction succeeds then the user can rest easy that the
operations were executed correctly without interference. This is a very powerful guarantee, since
it basically eliminates the risk of [race conditions](https://en.wikipedia.org/wiki/Race_condition)
(a class of bugs that are notoriously hard to fix). 

To illustrate how transactions work, here's an example test script for the MVCC code (there's a
bunch of [other test scripts](https://github.com/erikgrinaker/toydb/tree/aa14deb71f650249ce1cab8828ed7bcae2c9206e/src/storage/testscripts/mvcc)
there too):

https://github.com/erikgrinaker/toydb/blob/a73e24b7e77671b9f466e0146323cd69c3e27bdf/src/storage/testscripts/mvcc/bank#L1-L69

To provide such [ACID transactions](https://en.wikipedia.org/wiki/ACID), toyDB uses a common
technique called [Multi-Version Concurrency Control](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
(MVCC). It is implemented at the key/value storage level, in the
[`storage::mvcc`](https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs)
module. It sits on top of any `storage::Engine` implementation, which it uses for actual data
storage.

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L220-L231

MVCC provides a guarantee called [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation):
a transaction sees a snapshot of the database as it was when the transaction began. Any later
changes will be invisible to it.

It does this by storing several historical versions of key/value pairs. The version number is simply
a number that's incremented for every new transaction:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L155-L158

Each transaction has its own unique version number. When it writes a key/value pair it appends its
version number to the key as `Key::Version(&[u8], Version)`, via the Keycode encoding we saw above.
If an old version of the key already exists, it will have a different version number and therefore
be stored as a separate key/value in the storage engine, so it will be left intact. To delete a key,
it writes a special tombstone value.

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L183-L189

Here's a simple diagram of what a history of versions 1 to 5 of keys `a` to `d` might look like:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L11-L26

Given this versioning scheme, we can summarize the MVCC protocol with a few simple rules:

1. When a new transaction begins, it:
    * Obtains the next available version number.
    * Adds its version number to the set of active transactions (the "active set").
    * Takes a snapshot of other uncommitted transaction versions from the active set.

2. When the transaction reads a key, it:
    * Ignores versions above its own version.
    * Ignores versions in its active set (uncommitted transactions).
    * Returns the latest version of the key at or below its own version.

3. When the transaction writes a key, it:
    * Looks for a key version above its own version; errors if found.
    * Looks for a key version in its active set (uncommitted transactions); errors if found.
    * Writes a key/value pair with its own version.

4. When the transaction commits, it:
    * Flushes all writes to disk.
    * Removes itself from the active set.

And that's basically it! The transaction's writes all become visible atomically at the instant it
commits and removes itself from the active set, since new transactions no longer ignore its version.
The transaction saw a stable snapshot of the database, since it ignored newer versions and versions
that were uncommitted when it began. The transaction can read its own writes, even though noone else
can. And if any of its writes conflict with another transaction it would get an error and have to
retry.

Not only that, this also allows us to do time-travel queries, where we can query the database as it
was at any time in the past: we simply pick a version number to read at.

There are a few more details that we've left out here. To roll back the transaction, it needs to
keep track of its writes to undo them. To delete keys we write tombstone versions. For read-only
queries we can avoid assigning new version numbers. And we don't garbage collect old versions. See
the module documentation for more details:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L1-L140

Let's walk through a simple example with code pointers to get a feel for how this works. Notice how
we don't have to mess with any version numbers here -- this is an internal MVCC implementation
detail.

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

First, we begin a new transaction with `MVCC::begin()`. This calls through to
`Transaction::begin()`, which obtains a version number stored in `Key::NextVersion` and increments
it, then takes a snapshot of the active set in `Key::ActiveSet`, adds itself to it, and writes it
back:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L368-L391

This returns a `Transaction` object which provides the primary key/value API with get/set/delete
methods. It contains the main state of the transaction: it's version number and active set.

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L294-L327

We then call `Transaction::get(b"foo")` to read the value of the key `foo`. We want to find the
latest version that's visible to us (ignoring future versions and the active set). Recall that
we store multiple version of each key as `Key::Version(key, version)`. The Keycode encoding ensures
that all versions are stored in sorted order, so we can do a reverse range scan from
`Key::Version(b"foo", 0)` to `Key::Version(b"foo", self.version)` and return the latest version
that's visible to us:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L564-L581

When we call `Transaction::delete(b"foo")` and `Transaction::set(b"bar", value)`, we're just calling
through to the same `Transaction::write_version()` method, but use `None` as a deletion tombstone
and `Some(value)` for a regular key/value pair:

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L514-L522

To write a new version of a key, we first have to check for conflicts by seeing if there's a
version of the key that's invisible to us -- if it is, we conflicted with a concurrent transaction.
Like with `Transaction::get()`, we do this with a range scan.

We then just go on to write the `Key::Version(b"foo", self.version)` and encode the value as an
`Option<value>` to accomodate the `None` tombstone marker. We also write a
`Key::TxnWrite(version, key)` to keep a version-indexed list of our writes in case we have to roll
back.

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L524-L562

Finally, `Transaction::commit()` will make our transaction take effect and become visible. It does
this simply by removing itself from the active set in `Key::ActiveSet`, and also cleaning up its
`Key::TxnWrite` write index. As the comment says, we don't actually have to flush to durable storage
here, because the Raft log will provide durability for us -- we'll get back to this later.

https://github.com/erikgrinaker/toydb/blob/8f8eae0dcf70b1a0df2e853b1f6600e0c7075340/src/storage/mvcc.rs#L466-L485

---

<p align="center">
← <a href="encoding.md">Key/Value Encoding</a> &nbsp; | &nbsp; <a href="raft.md">Raft Consensus</a> →
</p>