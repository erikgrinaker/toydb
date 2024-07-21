use super::Catalog;
use crate::encoding::{self, Key as _, Value as _};
use crate::errinput;
use crate::error::Result;
use crate::sql::types::{Expression, Row, Rows, Table, Value};
use crate::storage::{self, mvcc};

use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

/// A SQL engine using local storage. This provides the main SQL storage logic,
/// and the Raft SQL engine just dispatches to this for node-local SQL storage.
pub struct Local<E: storage::Engine + 'static> {
    /// The local MVCC storage engine.
    pub mvcc: mvcc::MVCC<E>,
}

impl<E: storage::Engine> Local<E> {
    /// Creates a new local SQL engine using the given storage engine.
    pub fn new(engine: E) -> Self {
        Self { mvcc: mvcc::MVCC::new(engine) }
    }

    /// Resumes a transaction from the given state. This is usually encapsulated
    /// in `mvcc::Transaction`, but the Raft-based engine can't retain the MVCC
    /// transaction between each request since it may be executed across
    /// different leader nodes, so it instead keeps the state in the session.
    pub fn resume(&self, state: mvcc::TransactionState) -> Result<Transaction<E>> {
        Ok(Transaction::new(self.mvcc.resume(state)?))
    }

    /// Gets an unversioned key, or None if it doesn't exist.
    pub fn get_unversioned(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.mvcc.get_unversioned(key)
    }

    /// Sets an unversioned key.
    pub fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.mvcc.set_unversioned(key, value)
    }
}

impl<'a, E: storage::Engine> super::Engine<'a> for Local<E> {
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
pub struct Transaction<E: storage::Engine + 'static> {
    txn: mvcc::Transaction<E>,
}

impl<E: storage::Engine> Transaction<E> {
    /// Creates a new SQL transaction using the given MVCC transaction.
    fn new(txn: mvcc::Transaction<E>) -> Self {
        Self { txn }
    }

    /// Returns the transaction's internal state.
    pub fn state(&self) -> &mvcc::TransactionState {
        self.txn.state()
    }

    /// Fetches the matching primary keys for the given secondary index value,
    /// or an empty set if there is none. The value must already be normalized.
    fn get_index(&self, table: &str, column: &str, value: &Value) -> Result<BTreeSet<Value>> {
        debug_assert!(self.has_index(table, column)?, "no index on {table}.{column}");
        debug_assert!(value.is_normalized(), "value not normalized");
        Ok(self
            .txn
            .get(&Key::Index(table.into(), column.into(), value.into()).encode())?
            .map(|v| BTreeSet::decode(&v))
            .transpose()?
            .unwrap_or_default())
    }

    /// Fetches a single row by primary key, or None if it doesn't exist. The key
    /// must already be normalized.
    fn get_row(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        debug_assert!(id.is_normalized(), "value not normalized");
        self.txn
            .get(&Key::Row(table.into(), id.into()).encode())?
            .map(|v| Row::decode(&v))
            .transpose()
    }

    /// Returns true if the given secondary index exists.
    fn has_index(&self, table: &str, column: &str) -> Result<bool> {
        let table = self.must_get_table(table)?;
        Ok(table.columns.iter().find(|c| c.name == column).map(|c| c.index).unwrap_or(false))
    }

    /// Stores a secondary index entry for the given column value, replacing the
    /// existing entry. The value and ids must already be normalized.
    fn set_index(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        ids: BTreeSet<Value>,
    ) -> Result<()> {
        debug_assert!(self.has_index(table, column)?, "no index on {table}.{column}");
        debug_assert!(value.is_normalized(), "value not normalized");
        debug_assert!(ids.iter().all(|v| v.is_normalized()), "value not normalized");
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
        let indexes = table.columns.iter().enumerate().filter(|(_, c)| c.index).collect_vec();

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
                // Error if the delete would violate referential integrity.
                if let Some(source_id) = source_ids.first() {
                    let table = source.name;
                    let column = &source.columns[source.primary_key].name;
                    return errinput!("row referenced by {table}.{column}={source_id}");
                }
            }
        }

        // Delete the rows.
        for id in ids {
            let id = id.normalize_ref();

            // Update any index entries.
            if !indexes.is_empty() {
                if let Some(row) = self.get_row(&table.name, &id)? {
                    for (i, column) in indexes.iter().copied() {
                        let mut index = self.get_index(&table.name, &column.name, &row[i])?;
                        index.remove(&id);
                        self.set_index(&table.name, &column.name, &row[i], index)?;
                    }
                }
            }

            self.txn.delete(&Key::Row((&table.name).into(), id).encode())?;
        }
        Ok(())
    }

    fn get(&self, table: &str, ids: &[Value]) -> Result<Vec<Row>> {
        ids.iter().filter_map(|id| self.get_row(table, &id.normalize_ref()).transpose()).collect()
    }

    fn insert(&self, table: &str, rows: Vec<Row>) -> Result<()> {
        let table = self.must_get_table(table)?;
        for mut row in rows {
            // Normalize the row.
            row.iter_mut().for_each(|v| v.normalize());

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

    fn lookup_index(&self, table: &str, column: &str, values: &[Value]) -> Result<BTreeSet<Value>> {
        debug_assert!(self.has_index(table, column)?, "no index on {table}.{column}");
        values
            .iter()
            .map(|v| self.get_index(table, column, &v.normalize_ref()))
            .flatten_ok()
            .collect()
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Rows> {
        // TODO: this could be simpler if process_results() implemented Clone.
        let rows = self
            .txn
            .scan_prefix(&KeyPrefix::Row(table.into()).encode())
            .map(|result| result.and_then(|(_, value)| Row::decode(&value)));
        let Some(filter) = filter else {
            return Ok(Box::new(rows));
        };
        let rows = rows.filter_map(move |result| {
            result
                .and_then(|row| match filter.evaluate(Some(&row))? {
                    Value::Boolean(true) => Ok(Some(row)),
                    Value::Boolean(false) | Value::Null => Ok(None),
                    value => errinput!("filter returned {value}, expected boolean"),
                })
                .transpose()
        });
        Ok(Box::new(rows))
    }

    fn update(&self, table: &str, rows: BTreeMap<Value, Row>) -> Result<()> {
        let table = self.must_get_table(table)?;
        for (mut id, mut row) in rows {
            // Normalize the ID and row.
            id.normalize();
            row.iter_mut().for_each(|v| v.normalize());

            // If the primary key changes, we simply do a delete and insert.
            // This simplifies constraint validation.
            if id != row[table.primary_key] {
                self.delete(&table.name, &[id])?;
                self.insert(&table.name, vec![row])?;
                continue;
            }

            // Validate the row, but don't write it yet since we may need to
            // read the existing value to update secondary indexes.
            table.validate_row(&row, true, self)?;

            // Update indexes, knowing that the primary key has not changed.
            let indexes = table.columns.iter().enumerate().filter(|(_, c)| c.index).collect_vec();
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
        let prefix = &KeyPrefix::Row((&table.name).into()).encode();
        let keys: Vec<Vec<u8>> =
            self.txn.scan_prefix(prefix).map_ok(|(key, _)| key).try_collect()?;
        for key in keys {
            self.txn.delete(&key)?;
        }

        // Delete any secondary indexes.
        for column in table.columns.iter().filter(|c| c.index) {
            let prefix = &KeyPrefix::Index((&table.name).into(), (&column.name).into()).encode();
            let keys: Vec<_> = self.txn.scan_prefix(prefix).map_ok(|(key, _)| key).try_collect()?;
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
#[derive(Debug, Deserialize, Serialize)]
pub enum Key<'a> {
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
