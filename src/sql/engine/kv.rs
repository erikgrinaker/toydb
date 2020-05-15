use super::super::schema::{Catalog, Table, Tables};
use super::super::types::{Expression, Row, Value};
use super::Transaction as _;
use crate::storage::kv;
use crate::Error;

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A SQL engine based on an underlying MVCC key/value store
pub struct KV<S: kv::Store> {
    /// The underlying key/value store
    pub(super) kv: kv::MVCC<S>,
}

// FIXME Implement Clone manually due to https://github.com/rust-lang/rust/issues/26925
impl<S: kv::Store> std::clone::Clone for KV<S> {
    fn clone(&self) -> Self {
        KV::new(self.kv.clone())
    }
}

impl<S: kv::Store> KV<S> {
    /// Creates a new key/value-based SQL engine
    pub fn new(kv: kv::MVCC<S>) -> Self {
        Self { kv }
    }

    /// Fetches an unversioned metadata value
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.kv.get_metadata(key)
    }

    /// Sets an unversioned metadata value
    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        self.kv.set_metadata(key, value)
    }
}

impl<S: kv::Store> super::Engine for KV<S> {
    type Transaction = Transaction<S>;

    fn begin(&self, mode: super::Mode) -> Result<Self::Transaction, Error> {
        Ok(Self::Transaction::new(self.kv.begin_with_mode(mode)?))
    }

    fn resume(&self, id: u64) -> Result<Self::Transaction, Error> {
        Ok(Self::Transaction::new(self.kv.resume(id)?))
    }
}

/// Serializes SQL metadata.
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>, Error> {
    Ok(bincode::serialize(value)?)
}

/// Deserializes SQL metadata.
fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V, Error> {
    Ok(bincode::deserialize(bytes)?)
}

/// An SQL transaction based on an MVCC key/value transaction
pub struct Transaction<S: kv::Store> {
    txn: kv::mvcc::Transaction<S>,
}

impl<S: kv::Store> Transaction<S> {
    /// Creates a new SQL transaction from an MVCC transaction
    fn new(txn: kv::mvcc::Transaction<S>) -> Self {
        Self { txn }
    }

    /// Loads an index entry
    fn index_load(
        &self,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<HashSet<Value>, Error> {
        let key = Key::Index(table, column, value).encode();
        if let Some(value) = self.txn.get(&key)? {
            let item: (Value, HashSet<Value>) = deserialize(&value)?;
            Ok(item.1)
        } else {
            Ok(HashSet::new())
        }
    }

    /// Saves an index entry.
    /// FIXME We save the index key as part of the value, to avoid having to implement key decoders
    /// right now.
    fn index_save(
        &mut self,
        table: &str,
        column: &str,
        value: &Value,
        index: HashSet<Value>,
    ) -> Result<(), Error> {
        let key = Key::Index(table, column, value).encode();
        if index.is_empty() {
            self.txn.delete(&key)
        } else {
            self.txn.set(&key, serialize(&(value, index))?)
        }
    }
}

impl<S: kv::Store> super::Transaction for Transaction<S> {
    fn id(&self) -> u64 {
        self.txn.id()
    }

    fn mode(&self) -> super::Mode {
        self.txn.mode()
    }

    fn commit(self) -> Result<(), Error> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<(), Error> {
        self.txn.rollback()
    }

    fn create(&mut self, table: &str, row: Row) -> Result<(), Error> {
        let table = self.must_read_table(&table)?;
        table.validate_row(&row, self)?;
        let id = table.get_row_key(&row)?;
        if self.read(&table.name, &id)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} already exists for table {}",
                id, table.name
            )));
        }
        self.txn.set(&Key::Row(&table.name, &id).encode(), serialize(&row)?)?;

        // Update indexes
        for (i, column) in table.columns.iter().enumerate().filter(|(_, c)| c.index) {
            let mut index = self.index_load(&table.name, &column.name, &row[i])?;
            index.insert(id.clone());
            self.index_save(&table.name, &column.name, &row[i], index)?;
        }
        Ok(())
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<(), Error> {
        let table = self.must_read_table(&table)?;
        table.assert_unreferenced_key(id, self)?;

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
        self.txn.delete(&Key::Row(&table.name, id).encode())
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>, Error> {
        self.txn.get(&Key::Row(table, id).encode())?.map(|v| deserialize(&v)).transpose()
    }

    fn read_index(
        &self,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<HashSet<Value>, Error> {
        if !self.must_read_table(table)?.get_column(column)?.index {
            return Err(Error::Value(format!("No index on {}.{}", table, column)));
        }
        self.index_load(table, column, value)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<super::Scan, Error> {
        let table = self.must_read_table(&table)?;
        let scan = self
            .txn
            .scan_prefix(&KeyPrefix::Row(&table.name).encode())?
            .map(|r| r.and_then(|(_, v)| deserialize(&v)))
            .filter_map(|r| match r {
                Ok(row) => match &filter {
                    Some(filter) => match filter.evaluate(&table.row_env(&row)) {
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
            });

        // FIXME We buffer results here, to avoid dealing with trait lifetimes right now
        Ok(Box::new(scan.collect::<Vec<Result<Row, Error>>>().into_iter()))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<super::IndexScan, Error> {
        let table = self.must_read_table(&table)?;
        let column = table.get_column(column)?;
        if !column.index {
            return Err(Error::Value(format!("No index for {}.{}", table.name, column.name)));
        }

        let scan = self
            .txn
            .scan_prefix(&KeyPrefix::Index(&table.name, &column.name).encode())?
            .map(|r| r.and_then(|(_, v)| deserialize(&v)));

        // FIXME We buffer results here, to avoid dealing with trait lifetimes right now
        Ok(Box::new(scan.collect::<Vec<Result<(Value, HashSet<Value>), Error>>>().into_iter()))
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<(), Error> {
        let table = self.must_read_table(&table)?;
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
        self.txn.set(&Key::Row(&table.name, &id).encode(), serialize(&row)?)
    }
}

impl<S: kv::Store> Catalog for Transaction<S> {
    fn create_table(&mut self, table: &Table) -> Result<(), Error> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }
        table.validate(self)?;
        self.txn.set(&Key::Table(&table.name).encode(), serialize(table)?)
    }

    fn delete_table(&mut self, table: &str) -> Result<(), Error> {
        let table = self.must_read_table(&table)?;
        table.assert_unreferenced(self)?;
        let mut scan = self.scan(&table.name, None)?;
        while let Some(row) = scan.next().transpose()? {
            self.delete(&table.name, &table.get_row_key(&row)?)?
        }
        self.txn.delete(&Key::Table(&table.name).encode())
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>, Error> {
        self.txn.get(&Key::Table(table).encode())?.map(|v| deserialize(&v)).transpose()
    }

    fn scan_tables(&self) -> Result<Tables, Error> {
        Ok(Box::new(
            self.txn
                .scan_prefix(&KeyPrefix::Table.encode())?
                .map(|r| r.and_then(|(_, v)| deserialize(&v)))
                .collect::<Result<Vec<_>, Error>>()?
                .into_iter(),
        ))
    }
}

/// Encodes MVCC keyspace prefixes
enum KeyPrefix<'a> {
    /// Tables
    Table,
    /// Index entries for a table and column
    Index(&'a str, &'a str),
    /// Rows for a table
    Row(&'a str),
}

impl<'a> KeyPrefix<'a> {
    fn encode(&self) -> Vec<u8> {
        match self {
            KeyPrefix::Table => vec![0x01],
            KeyPrefix::Index(table, column) => [
                vec![0x03],
                table.as_bytes().to_vec(),
                vec![0x00],
                column.as_bytes().to_vec(),
                vec![0x00],
            ]
            .concat(),
            KeyPrefix::Row(table) => [vec![0x05], table.as_bytes().to_vec(), vec![0x00]].concat(),
        }
    }
}

/// Encodes tables and rows as MVCC key/value keys
enum Key<'a> {
    /// A table schema key for the given table name
    Table(&'a str),
    /// A key for an index entry
    Index(&'a str, &'a str, &'a Value),
    /// A key for a row identified by table name and row primary key
    Row(&'a str, &'a Value),
}

impl<'a> Key<'a> {
    /// Encodes the key as a byte vector
    fn encode(self) -> Vec<u8> {
        match self {
            Self::Table(name) => [KeyPrefix::Table.encode(), name.as_bytes().to_vec()].concat(),
            Self::Index(table, column, value) => {
                [KeyPrefix::Index(table, column).encode(), Self::encode_value(value)].concat()
            }
            Self::Row(table, pk) => {
                [KeyPrefix::Row(table).encode(), Self::encode_value(pk)].concat()
            }
        }
    }

    /// Encodes a value as a byte vector
    fn encode_value(value: &Value) -> Vec<u8> {
        match value {
            Value::Boolean(b) if *b => vec![0x01],
            Value::Boolean(_) => vec![0x00],
            Value::Float(f) => f.to_be_bytes().to_vec(),
            Value::Integer(i) => i.to_be_bytes().to_vec(),
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Null => vec![],
        }
    }
}
