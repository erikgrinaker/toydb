use super::Catalog;
use crate::encoding::{self, Key as _, Value as _};
use crate::error::Result;
use crate::sql::types::schema::Table;
use crate::sql::types::{Expression, Row, Rows, Value};
use crate::storage::{self, mvcc};
use crate::{errdata, errinput};

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

/// A SQL engine using local storage. This provides the main SQL storage logic,
/// including with the Raft SQL engine which dispatches to this engine for
/// node-local SQL storage.
pub struct Local<E: storage::Engine> {
    /// The local MVCC storage engine.
    pub(super) mvcc: mvcc::MVCC<E>,
}

impl<E: storage::Engine> Local<E> {
    /// Creates a new local SQL engine using the given storage engine.
    pub fn new(engine: E) -> Self {
        Self { mvcc: mvcc::MVCC::new(engine) }
    }

    /// Resumes a transaction from the given state. This is usually encapsulated
    /// in `mvcc::Transaction`, but the Raft-based engine can't retain the MVCC
    /// transaction between each request since it may be executed across
    /// multiple leader nodes, so it instead keeps the state in the session.
    pub fn resume(&self, state: mvcc::TransactionState) -> Result<Transaction<E>> {
        Ok(Transaction::new(self.mvcc.resume(state)?))
    }

    /// Fetches an unversioned key, or None if it doesn't exist.
    pub fn get_unversioned(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.mvcc.get_unversioned(key)
    }

    /// Sets an unversioned key.
    pub fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.mvcc.set_unversioned(key, value)
    }
}

impl<'a, E: storage::Engine + 'a> super::Engine<'a> for Local<E> {
    type Transaction = Transaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.mvcc.begin()?))
    }

    fn begin_read_only(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.mvcc.begin_read_only()?))
    }

    fn begin_as_of(&self, version: mvcc::Version) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.mvcc.begin_as_of(version)?))
    }
}

/// A SQL transaction, wrapping an MVCC transaction.
pub struct Transaction<E: storage::Engine> {
    txn: mvcc::Transaction<E>,
}

impl<E: storage::Engine> Transaction<E> {
    /// Creates a new SQL transaction using the given MVCC transaction.
    fn new(txn: mvcc::Transaction<E>) -> Self {
        Self { txn }
    }

    /// Returns the transaction's internal state.
    pub(super) fn state(&self) -> &mvcc::TransactionState {
        self.txn.state()
    }

    /// Fetches the matching primary keys for the given secondary index value,
    /// or an empty set if there is none.
    fn get_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        debug_assert!(self.has_index(table, column)?, "no index on {table}.{column}");
        Ok(self
            .txn
            .get(&Key::Index(table.into(), column.into(), value.into()).encode())?
            .map(|v| HashSet::decode(&v))
            .transpose()?
            .unwrap_or_default())
    }

    /// Fetches a single row by primary key, or None if it doesn't exist.
    fn get_row(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        self.txn
            .get(&Key::Row(table.into(), id.into()).encode())?
            .map(|v| Row::decode(&v))
            .transpose()
    }

    /// Returns true if the given secondary index exists.
    fn has_index(&self, table: &str, column: &str) -> Result<bool> {
        Ok(self.must_get_table(table)?.get_column(column)?.index)
    }

    /// Stores a secondary index entry for the given column value, replacing the
    /// existing entry.
    fn set_index(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        ids: HashSet<Value>,
    ) -> Result<()> {
        debug_assert!(self.has_index(table, column)?, "no index on {table}.{column}");
        let key = Key::Index(table.into(), column.into(), value.into()).encode();
        if ids.is_empty() {
            self.txn.delete(&key)
        } else {
            self.txn.set(&key, ids.encode())
        }
    }

    /// Returns all tables referencing a table, as (table, column index) pairs.
    /// This includes references from the table itself.
    fn table_references(&self, table: &str) -> Result<Vec<(Table, Vec<usize>)>> {
        Ok(self
            .list_tables()?
            .into_iter()
            .map(|t| {
                let references: Vec<usize> = t
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.references.as_deref() == Some(table))
                    .map(|(i, _)| i)
                    .collect();
                (t, references)
            })
            .filter(|(_, references)| !references.is_empty())
            .collect())
    }
}

impl<E: storage::Engine> super::Transaction for Transaction<E> {
    fn version(&self) -> u64 {
        self.txn.version()
    }

    fn read_only(&self) -> bool {
        self.txn.read_only()
    }

    fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<()> {
        self.txn.rollback()
    }

    fn delete(&self, table: &str, ids: &[Value]) -> Result<()> {
        let table = self.must_get_table(table)?;
        let indexes: Vec<_> = table.columns.iter().enumerate().filter(|(_, c)| c.index).collect();

        // Check for foreign key references to the deleted rows.
        for (source, refs) in self.table_references(&table.name)? {
            let self_reference = source.name == table.name;
            for i in refs {
                let column = &source.columns[i];
                let mut source_ids = if i == source.primary_key {
                    // If the reference is from a primary key column, do a lookup.
                    self.get(&source.name, ids)?
                        .into_iter()
                        .map(|row| row.into_iter().nth(i).expect("short row"))
                        .collect()
                } else {
                    // Otherwise (commonly), do a secondary index lookup.
                    // All foreign keys have a secondary index.
                    self.lookup_index(&source.name, &column.name, ids)?
                };
                // We can ignore any references between the deleted rows,
                // including a row referring to itself.
                if self_reference {
                    for id in ids {
                        source_ids.remove(id);
                    }
                }
                if let Some(source_id) = source_ids.into_iter().next() {
                    return errinput!(
                        "row referenced by {}.{} for {}.{}={source_id}",
                        source.name,
                        column.name,
                        source.name,
                        source.columns[source.primary_key].name
                    );
                }
            }
        }

        for id in ids {
            // Remove the primary key from any index entries. There must be
            // an index entry for each row.
            //
            // TODO: NULL entries shouldn't be indexed, we can skip
            // those. But they're currently indexed.
            if !indexes.is_empty() {
                if let Some(row) = self.get_row(&table.name, id)? {
                    for (i, column) in indexes.iter().copied() {
                        let mut index = self.get_index(&table.name, &column.name, &row[i])?;
                        index.remove(id);
                        self.set_index(&table.name, &column.name, &row[i], index)?;
                    }
                }
            }

            // Delete the row.
            self.txn.delete(&Key::Row((&table.name).into(), id.into()).encode())?;
        }
        Ok(())
    }

    fn get(&self, table: &str, ids: &[Value]) -> Result<Vec<Row>> {
        ids.iter().filter_map(|id| self.get_row(table, id).transpose()).collect()
    }

    fn insert(&self, table: &str, rows: Vec<Row>) -> Result<()> {
        let table = self.must_get_table(table)?;
        for row in rows {
            // Insert the row.
            table.validate_row(&row, false, self)?;
            let id = &row[table.primary_key];
            self.txn.set(&Key::Row((&table.name).into(), id.into()).encode(), row.encode())?;

            // Update any secondary indexes.
            for (i, column) in table.columns.iter().enumerate().filter(|(_, c)| c.index) {
                let mut index = self.get_index(&table.name, &column.name, &row[i])?;
                index.insert(id.clone());
                self.set_index(&table.name, &column.name, &row[i], index)?;
            }
        }
        Ok(())
    }

    fn lookup_index(&self, table: &str, column: &str, values: &[Value]) -> Result<HashSet<Value>> {
        debug_assert!(self.has_index(table, column)?, "index lookup without index");
        let mut pks = HashSet::new();
        for v in values {
            pks.extend(self.get_index(table, column, v)?)
        }
        Ok(pks)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Rows> {
        Ok(Box::new(
            self.txn
                .scan_prefix(&KeyPrefix::Row(table.into()).encode())
                .iter()
                .map(|r| r.and_then(|(_, v)| Row::decode(&v)))
                .filter_map(move |r| match r {
                    Ok(row) => match &filter {
                        Some(filter) => match filter.evaluate(Some(&row)) {
                            Ok(Value::Boolean(b)) if b => Some(Ok(row)),
                            Ok(Value::Boolean(_)) | Ok(Value::Null) => None,
                            Ok(v) => Some(errinput!("filter returned {v}, expected boolean")),
                            Err(err) => Some(Err(err)),
                        },
                        None => Some(Ok(row)),
                    },
                    err => Some(err),
                })
                // TODO: don't collect.
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<super::IndexScan> {
        debug_assert!(self.has_index(table, column)?, "index scan without index");
        Ok(Box::new(
            self.txn
                .scan_prefix(&KeyPrefix::Index(table.into(), column.into()).encode())
                .iter()
                .map(|r| {
                    r.and_then(|(k, v)| {
                        let Key::Index(_, _, value) = Key::decode(&k)? else {
                            return errdata!("invalid index key");
                        };
                        Ok((value.into_owned(), HashSet::decode(&v)?))
                    })
                })
                // TODO: don't collect.
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }

    fn update(&self, table: &str, rows: HashMap<Value, Row>) -> Result<()> {
        let table = self.must_get_table(table)?;

        for (id, row) in rows {
            // If the primary key changes, we simply do a delete and insert.
            // This simplifies constraint validation.
            if id != row[table.primary_key] {
                self.delete(&table.name, &[id])?;
                self.insert(&table.name, vec![row])?;
                return Ok(());
            }

            // Validate the row, but don't write it yet since we may need to
            // read the existing value to update secondary indexes.
            table.validate_row(&row, true, self)?;

            // Update indexes, knowing that the primary key has not changed.
            let indexes: Vec<_> =
                table.columns.iter().enumerate().filter(|(_, c)| c.index).collect();
            if !indexes.is_empty() {
                let old = self.get(&table.name, &[id.clone()])?.remove(0);
                for (i, column) in indexes {
                    // If the value didn't change, we don't have to do anything.
                    if old[i] == row[i] {
                        continue;
                    }

                    // Remove the old value from the index entry.
                    let mut index = self.get_index(&table.name, &column.name, &old[i])?;
                    index.remove(&id);
                    self.set_index(&table.name, &column.name, &old[i], index)?;

                    // Insert the new value into the index entry.
                    // TODO: consider whether Null values should be indexed.
                    let mut index = self.get_index(&table.name, &column.name, &row[i])?;
                    index.insert(id.clone());
                    self.set_index(&table.name, &column.name, &row[i], index)?;
                }
            }

            // Update the row.
            self.txn.set(&Key::Row((&table.name).into(), (&id).into()).encode(), row.encode())?;
        }
        Ok(())
    }
}

impl<E: storage::Engine> Catalog for Transaction<E> {
    fn create_table(&self, table: Table) -> Result<()> {
        if self.get_table(&table.name)?.is_some() {
            return errinput!("table {} already exists", table.name);
        }
        table.validate(self)?;
        self.txn.set(&Key::Table((&table.name).into()).encode(), table.encode())
    }

    fn drop_table(&self, table: &str, if_exists: bool) -> Result<bool> {
        let table = match self.get_table(table)? {
            Some(table) => table,
            None if if_exists => return Ok(false),
            None => return errinput!("table {table} does not exist"),
        };

        // Check for foreign key references.
        if let Some((source, refs)) =
            self.table_references(&table.name)?.iter().find(|(t, _)| t.name != table.name)
        {
            return errinput!(
                "table {} is referenced from {}.{}",
                table.name,
                source.name,
                source.columns[refs[0]].name
            );
        }

        // Delete the table schema entry.
        self.txn.delete(&Key::Table((&table.name).into()).encode())?;

        // Delete the table rows. storage::Engine doesn't support writing while
        // scanning, so we buffer all keys in a vector. We could also do this in
        // batches, although we'd want to do the batching above Raft to avoid
        // blocking Raft processing for the duration of the drop.
        let keys: Vec<Vec<u8>> = self
            .txn
            .scan_prefix(&KeyPrefix::Row((&table.name).into()).encode())
            .iter()
            .map(|r| r.map(|(key, _)| key))
            .collect::<Result<_>>()?;
        for key in keys {
            self.txn.delete(&key)?;
        }

        // Delete any secondary indexes.
        for column in table.columns.iter().filter(|c| c.index) {
            let keys: Vec<_> = self
                .txn
                .scan_prefix(
                    &KeyPrefix::Index((&table.name).into(), (&column.name).into()).encode(),
                )
                .iter()
                .map(|r| r.map(|(key, _)| key))
                .collect::<Result<_>>()?;
            for key in keys {
                self.txn.delete(&key)?;
            }
        }

        Ok(true)
    }

    fn get_table(&self, table: &str) -> Result<Option<Table>> {
        self.txn.get(&Key::Table(table.into()).encode())?.map(|v| Table::decode(&v)).transpose()
    }

    fn list_tables(&self) -> Result<Vec<Table>> {
        self.txn
            .scan_prefix(&KeyPrefix::Table.encode())
            .iter()
            .map(|r| r.and_then(|(_, v)| Table::decode(&v)))
            .collect()
    }
}

/// SQL engine keys, using the KeyCode order-preserving encoding. For
/// simplicity, table and column names are used directly as identifiers in
/// keys, instead of e.g. numberic IDs. It is not possible to change
/// table/column names, so this is fine.
///
/// Uses Cow to allow encoding borrowed values but decoding owned values.
///
/// TODO: add helper methods here to encode borrowed keys. This should also
/// encode into a reused byte buffer, see keycode::serialize() comment.
#[derive(Deserialize, Serialize)]
enum Key<'a> {
    /// A table schema by table name.
    Table(Cow<'a, str>),
    /// An index entry, by table name, index name, and index value.
    Index(Cow<'a, str>, Cow<'a, str>, Cow<'a, Value>),
    /// A table row, by table name and primary key value.
    Row(Cow<'a, str>, Cow<'a, Value>),
}

impl<'a> encoding::Key<'a> for Key<'a> {}

/// Key prefixes, allowing prefix scans of specific parts of the keyspace. These
/// must match the keys -- in particular, the enum variant indexes must match.
#[derive(Deserialize, Serialize)]
enum KeyPrefix<'a> {
    /// All table schemas.
    Table,
    /// An entire table index, by table and index name.
    Index(Cow<'a, str>, Cow<'a, str>),
    /// An entire table's rows, by table name.
    Row(Cow<'a, str>),
}

impl<'a> encoding::Key<'a> for KeyPrefix<'a> {}
