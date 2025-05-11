# SQL Storage

The SQL storage engine, in the [`sql::engine`](https://github.com/erikgrinaker/toydb/tree/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/engine)
module, stores tables and rows. toyDB has two SQL storage implementations:

* `sql::engine::Local`: local storage using a `storage::Engine` key/value store.
* `sql::engine::Raft`: Raft-replicated storage, using `Local` on each node below Raft.

These implement the `sql::engine::Engine` trait, which specifies the SQL storage API. SQL execution
can use either simple local storage or Raft-replicated storage -- toyDB itself always uses the
Raft-replicated engine, but many tests use a local in-memory engine.

The `sql::engine::Engine` trait is fully transactional, based on the `storage::MVCC` transaction
engine discussed previously. As such, the trait just has a few methods that begin transactions --
the storage logic itself is implemented in the transaction, which we'll cover in next. The trait
also has a `session()` method to start SQL sessions for query execution, which we'll revisit in the
execution section.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/sql/engine/engine.rs#L9-L29

Here, we'll only look at the `Local` engine, and we'll discuss Raft replication afterwards. `Local`
itself is just a thin wrapper around a `storage::MVCC<storage::Engine>` to create transactions:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L50-L97

## Key/Value Representation

`Local` uses a `storage::Engine` key/value store to store SQL table schemas, table rows, and
secondary index entries. But how do we represent these as keys and values?

The keys are represented by the `sql::engine::Key` enum, and encoded using the Keycode encoding
that we've discussed in the encoding section:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L15-L31

The values are encoded using the Bincode encoding, where the value type is given by the key:

* `Key::Table` → `sql::types::Table` (table schemas)
* `Key::Index` → `BTreeSet<sql::types::Value>` (indexed primary keys)
* `Key::Row` → `sql::types::Row` (table rows)

Recall that the Keycode encoding will store keys in sorted order. This means that all `Key::Table`
entries come first, then all `Key::Index`, then all `Key::Row`. These are further grouped and
sorted by their fields.

For example, consider these SQL tables containing movies and genres, with a secondary index on
`movies.genre_id` for fast lookups of movies with a given genre:

```sql
CREATE TABLE genres (
    id INTEGER PRIMARY KEY,
    name STRING NOT NULL
);

CREATE TABLE movies (
    id INTEGER PRIMARY KEY,
    title STRING NOT NULL,
    released INTEGER NOT NULL,
    genre_id INTEGER NOT NULL INDEX REFERENCES genres
);

INSERT INTO genres VALUES (1, 'Drama'), (2, 'Action');

INSERT INTO movies VALUES
    (1, 'Sicario', 2015, 2),
    (2, '21 Grams', 2003, 1),
    (3, 'Heat', 1995, 2);
```

This would result in the following illustrated keys and values, in the given order:

```
/Table/genres → Table { name: "genres", primary_key: 0, columns: ... }
/Table/movies → Table { name: "movies", primary_key: 0, columns: ... }
/Index/movies/genre_id/Integer(1) → BTreeSet { Integer(2) }
/Index/movies/genre_id/Integer(2) → BTreeSet { Integer(1), Integer(3) }
/Row/genres/Integer(1) → Row { Integer(1), String("Action") }
/Row/genres/Integer(2) → Row { Integer(2), String("Drama") }
/Row/movies/Integer(1) → Row { Integer(1), String("Sicario"), Integer(2015), Integer(2) }
/Row/movies/Integer(2) → Row { Integer(2), String("21 Grams"), Integer(2003), Integer(1) }
/Row/movies/Integer(3) → Row { Integer(3), String("Heat"), Integer(1995), Integer(2) }
```

Thus, if we want to do a full table scan of the `movies` table, we just do a prefix scan of
`/Row/movies/`. If we want to do a secondary index lookup of all movies with `genre_id = 2`, we
fetch `/Index/movies/genre_id/Integer(2)` and find that movies with `id = {1,3}` have this genre.

To help with prefix scans, the valid key prefixes are represented as `sql::engine::KeyPrefix`:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L35-L48

For a look at the actual on-disk binary storage format, see the test scripts under
[`src/sql/testscripts/writes`](https://github.com/erikgrinaker/toydb/tree/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/testscripts/writes),
which output the logical and raw binary representation of write operations.

## Schema Catalog

The `sql::engine::Catalog` trait is used to store table schemas, i.e. `sql::types::Table`. It has a
handful of methods for creating, dropping and fetching tables (recall that toyDB does not support
schema changes). The `Table::name` field is used as a unique table identifier throughout.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/sql/engine/engine.rs#L60-L79

The `Catalog` trait is also fully transactional, as it must be implemented on a transaction via the
`type Transaction: Transaction + Catalog` trait bound on `sql::engine::Engine`.

Creating a table is straightforward: insert a key/value pair with a Keycode-encoded `Key::Table`
for the key and a Bincode-encoded `sql::types::Table` for the value. We first check that the
table doesn't already exist, and validate the table schema using `Table::validate()`.

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L340-L347

Similarly, fetching and listing tables is straightforward: just key/value gets or scans using the
appropriate keys.

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L390-L399

Dropping tables is a bit more involved, since we have to perform some validation and also delete the
actual table rows and any secondary index entries, but it's not terribly complicated:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L349-L388

## Row Storage and Transactions

The workhorse of the SQL storage engine is the `Transaction` trait, which provides
[CRUD](https://en.wikipedia.org/wiki/Create,_read,_update_and_delete) operations (create, read,
update, delete) on table rows and secondary index entries. For performance (especially with Raft),
it operates on row batches rather than individual rows.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/sql/engine/engine.rs#L31-L58

The `Local::Transaction` implementation is just a wrapper around an MVCC transaction, and the
commit/rollback methods just call straight through to it:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L99-L102

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L182-L192

To insert new rows into a table, we first have to perform some validation: check that the table
exists and validate the rows against the table schema (including checking for e.g. primary key
conflicts and foreign key references). We then store the rows as a key/value pairs, using a
`Key::Row` with the table name and primary key value. And finally, we update secondary index entries
(if any).

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L252-L268

Row updates are similar to inserts, but in the case of a primary key change we instead delete the
old row and insert a new one, for simplicity. Secondary index updates also have to update both the
old and new entries.

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L296-L337

Row deletions are also similar: validate that the deletion is safe (e.g. check that there are no
foreign key references to it), then delete the `Key::Row` keys and any secondary index entries:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L194-L246

To fetch rows by primary key, we simply call through to key/value gets using the appropriate
`Key::Row`:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L248-L250

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L127-L133

Similarly, index lookups fetch a `Key::Index` for the indexed value, returning matching primary
keys:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L270-L273

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L115-L125

Scanning table rows just performs a prefix scan with the appropriate `KeyPrefix::Row`, returning a
row iterator. This can optionally also do row filtering via filter pushdowns, which we'll revisit
when we look at the SQL optimizer.

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/engine/local.rs#L275-L294

And with that, we can now store and retrieve SQL tables and rows on disk. Let's see how to replicate
it across nodes via Raft.

---

<p align="center">
← <a href="sql-data.md">SQL Data Model</a> &nbsp; | &nbsp; <a href="sql-raft.md">SQL Raft Replication</a> →
</p>