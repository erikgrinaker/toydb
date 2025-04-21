use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::slice;

use itertools::Itertools as _;
use serde::{Deserialize, Serialize};

use super::Catalog;
use crate::encoding::{self, Key as _, Value as _};
use crate::errinput;
use crate::error::Result;
use crate::sql::types::{Expression, Row, Rows, Table, Value};
use crate::storage::{self, mvcc};

/// SQL engine keys, using the Keycode order-preserving encoding. For
/// simplicity, table and column names are used directly as identifiers, instead
/// of e.g. numeric IDs. It is not possible to change table/column names, so
/// this is fine, if somewhat inefficient.
///
/// Uses Cow to allow encoding borrowed values but decoding owned values.
#[derive(Debug, Deserialize, Serialize)]
pub enum Key<'a> {
    /// A table schema, keyed by table name.
    Table(Cow<'a, str>),
    /// A column index entry, keyed by table name, column name, and index value.
    Index(Cow<'a, str>, Cow<'a, str>, Cow<'a, Value>),
    /// A table row, keyed by table name and primary key value.
    Row(Cow<'a, str>, Cow<'a, Value>),
}

impl<'a> encoding::Key<'a> for Key<'a> {}

/// Key prefixes, allowing prefix scans of specific parts of the keyspace. These
/// must match the keys -- in particular, the enum variant indexes must match,
/// since it's part of the encoded key.
#[derive(Deserialize, Serialize)]
enum KeyPrefix<'a> {
    /// All table schemas.
    Table,
    /// All column index entries, keyed by table and column name.
    Index(Cow<'a, str>, Cow<'a, str>),
    /// All table rows, keyed by table name.
    Row(Cow<'a, str>),
}

impl<'a> encoding::Key<'a> for KeyPrefix<'a> {}

/// A SQL engine using local storage. This provides the main SQL storage logic.
/// The Raft SQL engine dispatches to this for node-local SQL storage, executing
/// the same writes across each nodes' instance of `Local`.
pub struct Local<E: storage::Engine + 'static> {
    /// The local MVCC storage engine.
    pub mvcc: mvcc::MVCC<E>,
}

impl<E: storage::Engine> Local<E> {
    /// Creates a new local SQL engine using the given storage engine.
    pub fn new(engine: E) -> Self {
        Self { mvcc: mvcc::MVCC::new(engine) }
    }

    /// Resumes a transaction from the given state. This is usually kept within
    /// `mvcc::Transaction`, but the Raft-based engine can't retain the MVCC
    /// transaction across requests since it may be executed on different leader
    /// nodes, so it instead keeps the state client-side in the session.
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

impl<E: storage::Engine> super::Engine<'_> for Local<E> {
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
    /// or an empty set if there is none.
    fn get_index(&self, table: &str, column: &str, value: &Value) -> Result<BTreeSet<Value>> {
        debug_assert!(self.has_index(table, column)?, "no index on {table}.{column}");
        Ok(self
            .txn
            .get(&Key::Index(table.into(), column.into(), value.into()).encode())?
            .map(|v| BTreeSet::decode(&v))
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

    /// Returns true if a secondary index exists for the given column.
    fn has_index(&self, table: &str, column: &str) -> Result<bool> {
        let table = self.must_get_table(table)?;
        Ok(table.columns.iter().find(|c| c.name == column).map(|c| c.index).unwrap_or(false))
    }

    /// Stores a secondary index entry for the given column value, replacing the
    /// existing entry if any.
    fn set_index(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        ids: BTreeSet<Value>,
    ) -> Result<()> {
        debug_assert!(self.has_index(table, column)?, "no index on {table}.{column}");
        let key = Key::Index(table.into(), column.into(), value.into()).encode();
        if ids.is_empty() {
            self.txn.delete(&key)?;
        } else {
            self.txn.set(&key, ids.encode())?;
        }
        Ok(())
    }

    /// Returns all tables referencing a table, as (table, column index) pairs.
    /// This includes any references from the table itself.
    fn table_references(&self, table: &str) -> Result<Vec<(Table, Vec<usize>)>> {
        Ok(self
            .list_tables()?
            .into_iter()
            .map(|t| {
                let references = t
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.references.as_deref() == Some(table))
                    .map(|(i, _)| i)
                    .collect_vec();
                (t, references)
            })
            .filter(|(_, references)| !references.is_empty())
            .collect())
    }
}

impl<E: storage::Engine> super::Transaction for Transaction<E> {
    fn state(&self) -> &mvcc::TransactionState {
        self.txn.state()
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
                // including a row referencing itself.
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

        for id in ids {
            // Update any secondary index entries.
            if !indexes.is_empty() {
                if let Some(row) = self.get_row(&table.name, id)? {
                    for (i, column) in indexes.iter().copied() {
                        let mut ids = self.get_index(&table.name, &column.name, &row[i])?;
                        ids.remove(id);
                        self.set_index(&table.name, &column.name, &row[i], ids)?;
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

            // Update any secondary index entries.
            for (i, column) in table.columns.iter().enumerate().filter(|(_, c)| c.index) {
                let mut ids = self.get_index(&table.name, &column.name, &row[i])?;
                ids.insert(id.clone());
                self.set_index(&table.name, &column.name, &row[i], ids)?;
            }
        }
        Ok(())
    }

    fn lookup_index(&self, table: &str, column: &str, values: &[Value]) -> Result<BTreeSet<Value>> {
        debug_assert!(self.has_index(table, column)?, "no index on {table}.{column}");
        values.iter().map(|v| self.get_index(table, column, v)).flatten_ok().collect()
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
        for (id, row) in rows {
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
                let old = self.get(&table.name, slice::from_ref(&id))?.remove(0);
                for (i, column) in indexes {
                    // If the value didn't change, we don't have to do anything.
                    if old[i] == row[i] {
                        continue;
                    }

                    // Remove the old value from the index entry.
                    let mut ids = self.get_index(&table.name, &column.name, &old[i])?;
                    ids.remove(&id);
                    self.set_index(&table.name, &column.name, &old[i], ids)?;

                    // Insert the new value into the index entry.
                    let mut ids = self.get_index(&table.name, &column.name, &row[i])?;
                    ids.insert(id.clone());
                    self.set_index(&table.name, &column.name, &row[i], ids)?;
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
        let Some(table) = self.get_table(table)? else {
            if if_exists {
                return Ok(false);
            }
            return errinput!("table {table} does not exist");
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

        // Delete the table rows.
        let prefix = &KeyPrefix::Row((&table.name).into()).encode();
        let mut keys = self.txn.scan_prefix(prefix).map_ok(|(key, _)| key);
        while let Some(key) = keys.next().transpose()? {
            self.txn.delete(&key)?;
        }

        // Delete any secondary index entries.
        for column in table.columns.iter().filter(|c| c.index) {
            let prefix = &KeyPrefix::Index((&table.name).into(), (&column.name).into()).encode();
            let mut keys = self.txn.scan_prefix(prefix).map_ok(|(key, _)| key);
            while let Some(key) = keys.next().transpose()? {
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
