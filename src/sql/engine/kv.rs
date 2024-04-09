use super::super::schema::{Catalog, Table, Tables};
use super::super::types::{Expression, Row, Value};
use super::Transaction as _;
use crate::encoding::{bincode, keycode};
use crate::error::{Error, Result};
use crate::storage;

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::clone::Clone;
use std::collections::HashSet;

/// A SQL engine based on an underlying MVCC key/value store.
pub struct KV<E: storage::Engine> {
    /// The underlying key/value store.
    pub(super) kv: storage::mvcc::MVCC<E>,
}

// FIXME Implement Clone manually due to https://github.com/rust-lang/rust/issues/26925
impl<E: storage::Engine> Clone for KV<E> {
    fn clone(&self) -> Self {
        KV { kv: self.kv.clone() }
    }
}

impl<E: storage::Engine> KV<E> {
    /// Creates a new key/value-based SQL engine
    pub fn new(engine: E) -> Self {
        Self { kv: storage::mvcc::MVCC::new(engine) }
    }

    /// Resumes a transaction from the given state
    pub fn resume(
        &self,
        state: storage::mvcc::TransactionState,
    ) -> Result<<Self as super::Engine>::Transaction> {
        Ok(<Self as super::Engine>::Transaction::new(self.kv.resume(state)?))
    }

    /// Fetches an unversioned metadata value
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.get_unversioned(key)
    }

    /// Sets an unversioned metadata value
    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.set_unversioned(key, value)
    }
}

impl<E: storage::Engine> super::Engine for KV<E> {
    type Transaction = Transaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }

    fn begin_read_only(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin_read_only()?))
    }

    fn begin_as_of(&self, version: u64) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin_as_of(version)?))
    }
}

/// Serializes SQL metadata.
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    bincode::serialize(value)
}

/// Deserializes SQL metadata.
fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    bincode::deserialize(bytes)
}

/// An SQL transaction based on an MVCC key/value transaction
pub struct Transaction<E: storage::Engine> {
    txn: storage::mvcc::Transaction<E>,
}

impl<E: storage::Engine> Transaction<E> {
    /// Creates a new SQL transaction from an MVCC transaction
    fn new(txn: storage::mvcc::Transaction<E>) -> Self {
        Self { txn }
    }

    /// Returns the transaction's serialized state.
    pub(super) fn state(&self) -> &storage::mvcc::TransactionState {
        self.txn.state()
    }

    /// Loads an index entry
    fn index_load(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        Ok(self
            .txn
            .get(&Key::Index(table.into(), column.into(), value.into()).encode()?)?
            .map(|v| deserialize(&v))
            .transpose()?
            .unwrap_or_default())
    }

    /// Saves an index entry.
    fn index_save(
        &mut self,
        table: &str,
        column: &str,
        value: &Value,
        index: HashSet<Value>,
    ) -> Result<()> {
        let key = Key::Index(table.into(), column.into(), value.into()).encode()?;
        if index.is_empty() {
            self.txn.delete(&key)
        } else {
            self.txn.set(&key, serialize(&index)?)
        }
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

    fn create(&mut self, table: &str, row: Row) -> Result<()> {
        let table = self.must_read_table(table)?;
        table.validate_row(&row, self)?;
        let id = table.get_row_key(&row)?;
        if self.read(&table.name, &id)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} already exists for table {}",
                id, table.name
            )));
        }
        self.txn.set(&Key::Row((&table.name).into(), (&id).into()).encode()?, serialize(&row)?)?;

        // Update indexes
        for (i, column) in table.columns.iter().enumerate().filter(|(_, c)| c.index) {
            let mut index = self.index_load(&table.name, &column.name, &row[i])?;
            index.insert(id.clone());
            self.index_save(&table.name, &column.name, &row[i], index)?;
        }
        Ok(())
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<()> {
        let table = self.must_read_table(table)?;
        for (t, cs) in self.table_references(&table.name, true)? {
            let t = self.must_read_table(&t)?;
            let cs = cs
                .into_iter()
                .map(|c| Ok((t.get_column_index(&c)?, c)))
                .collect::<Result<Vec<_>>>()?;
            let mut scan = self.scan(&t.name, None)?;
            while let Some(row) = scan.next().transpose()? {
                for (i, c) in &cs {
                    if &row[*i] == id && (table.name != t.name || id != &table.get_row_key(&row)?) {
                        return Err(Error::Value(format!(
                            "Primary key {} is referenced by table {} column {}",
                            id, t.name, c
                        )));
                    }
                }
            }
        }

        let indexes: Vec<_> = table.columns.iter().enumerate().filter(|(_, c)| c.index).collect();
        if !indexes.is_empty() {
            if let Some(row) = self.read(&table.name, id)? {
                for (i, column) in indexes {
                    let mut index = self.index_load(&table.name, &column.name, &row[i])?;
                    index.remove(id);
                    self.index_save(&table.name, &column.name, &row[i], index)?;
                }
            }
        }
        self.txn.delete(&Key::Row(table.name.into(), id.into()).encode()?)
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        self.txn
            .get(&Key::Row(table.into(), id.into()).encode()?)?
            .map(|v| deserialize(&v))
            .transpose()
    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        if !self.must_read_table(table)?.get_column(column)?.index {
            return Err(Error::Value(format!("No index on {}.{}", table, column)));
        }
        self.index_load(table, column, value)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<super::Scan> {
        let table = self.must_read_table(table)?;
        Ok(Box::new(
            self.txn
                .scan_prefix(&KeyPrefix::Row((&table.name).into()).encode()?)?
                .iter()
                .map(|r| r.and_then(|(_, v)| deserialize(&v)))
                .filter_map(move |r| match r {
                    Ok(row) => match &filter {
                        Some(filter) => match filter.evaluate(Some(&row)) {
                            Ok(Value::Boolean(b)) if b => Some(Ok(row)),
                            Ok(Value::Boolean(_)) | Ok(Value::Null) => None,
                            Ok(v) => Some(Err(Error::Value(format!(
                                "Filter returned {}, expected boolean",
                                v
                            )))),
                            Err(err) => Some(Err(err)),
                        },
                        None => Some(Ok(row)),
                    },
                    err => Some(err),
                })
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<super::IndexScan> {
        let table = self.must_read_table(table)?;
        let column = table.get_column(column)?;
        if !column.index {
            return Err(Error::Value(format!("No index for {}.{}", table.name, column.name)));
        }
        Ok(Box::new(
            self.txn
                .scan_prefix(
                    &KeyPrefix::Index((&table.name).into(), (&column.name).into()).encode()?,
                )?
                .iter()
                .map(|r| -> Result<(Value, HashSet<Value>)> {
                    let (k, v) = r?;
                    let value = match Key::decode(&k)? {
                        Key::Index(_, _, pk) => pk.into_owned(),
                        _ => return Err(Error::Internal("Invalid index key".into())),
                    };
                    Ok((value, deserialize(&v)?))
                })
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()> {
        let table = self.must_read_table(table)?;
        // If the primary key changes we do a delete and create, otherwise we replace the row
        if id != &table.get_row_key(&row)? {
            self.delete(&table.name, id)?;
            self.create(&table.name, row)?;
            return Ok(());
        }

        // Update indexes, knowing that the primary key has not changed
        let indexes: Vec<_> = table.columns.iter().enumerate().filter(|(_, c)| c.index).collect();
        if !indexes.is_empty() {
            let old = self.read(&table.name, id)?.unwrap();
            for (i, column) in indexes {
                if old[i] == row[i] {
                    continue;
                }
                let mut index = self.index_load(&table.name, &column.name, &old[i])?;
                index.remove(id);
                self.index_save(&table.name, &column.name, &old[i], index)?;

                let mut index = self.index_load(&table.name, &column.name, &row[i])?;
                index.insert(id.clone());
                self.index_save(&table.name, &column.name, &row[i], index)?;
            }
        }

        table.validate_row(&row, self)?;
        self.txn.set(&Key::Row(table.name.into(), id.into()).encode()?, serialize(&row)?)
    }
}

impl<E: storage::Engine> Catalog for Transaction<E> {
    fn create_table(&mut self, table: Table) -> Result<()> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }
        table.validate(self)?;
        self.txn.set(&Key::Table((&table.name).into()).encode()?, serialize(&table)?)
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        let table = self.must_read_table(table)?;
        if let Some((t, cs)) = self.table_references(&table.name, false)?.first() {
            return Err(Error::Value(format!(
                "Table {} is referenced by table {} column {}",
                table.name, t, cs[0]
            )));
        }
        let mut scan = self.scan(&table.name, None)?;
        while let Some(row) = scan.next().transpose()? {
            self.delete(&table.name, &table.get_row_key(&row)?)?
        }
        self.txn.delete(&Key::Table(table.name.into()).encode()?)
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        self.txn.get(&Key::Table(table.into()).encode()?)?.map(|v| deserialize(&v)).transpose()
    }

    fn scan_tables(&self) -> Result<Tables> {
        Ok(Box::new(
            self.txn
                .scan_prefix(&KeyPrefix::Table.encode()?)?
                .iter()
                .map(|r| r.and_then(|(_, v)| deserialize(&v)))
                .collect::<Result<Vec<_>>>()?
                .into_iter(),
        ))
    }
}

/// SQL keys, using the KeyCode order-preserving encoding. Uses table and column
/// names directly as identifiers, to avoid additional indirection. It is not
/// possible to change names, so this is ok. Cow strings allow encoding borrowed
/// values and decoding into owned values.
#[derive(Debug, Deserialize, Serialize)]
enum Key<'a> {
    /// A table schema by table name.
    Table(Cow<'a, str>),
    /// An index entry, by table name, index name, and index value.
    Index(Cow<'a, str>, Cow<'a, str>, Cow<'a, Value>),
    /// A table row, by table name and primary key value.
    Row(Cow<'a, str>, Cow<'a, Value>),
}

impl<'a> Key<'a> {
    fn encode(self) -> Result<Vec<u8>> {
        keycode::serialize(&self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        keycode::deserialize(bytes)
    }
}

/// Key prefixes, allowing prefix scans of specific parts of the keyspace. These
/// must match the keys -- in particular, the enum variant indexes must match.
#[derive(Debug, Deserialize, Serialize)]
enum KeyPrefix<'a> {
    /// All table schemas.
    Table,
    /// An entire table index, by table and index name.
    Index(Cow<'a, str>, Cow<'a, str>),
    /// An entire table's rows, by table name.
    Row(Cow<'a, str>),
}

impl<'a> KeyPrefix<'a> {
    fn encode(self) -> Result<Vec<u8>> {
        keycode::serialize(&self)
    }
}
