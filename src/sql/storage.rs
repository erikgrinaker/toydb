use super::schema;
use super::types;
use crate::kv;
use crate::Error;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct Storage {
    kv: Arc<RwLock<Box<dyn kv::Store>>>,
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Storage")
    }
}

impl Storage {
    /// Creates a new Storage
    pub fn new<S: kv::Store>(store: S) -> Self {
        Storage { kv: Arc::new(RwLock::new(Box::new(store))) }
    }

    /// Creates a row
    pub fn create_row(&mut self, table: &str, row: types::Row) -> Result<(), Error> {
        let table = self.get_table(&table)?;
        let id = row
            .get(table.get_primary_key_index())
            .ok_or_else(|| Error::Value("No primary key value".into()))?;
        // FIXME Needs to check existence
        self.kv
            .write()?
            .set(&Self::key_row(&table.name, &id.to_string()), Self::serialize(row)?)?;
        Ok(())
    }

    /// Creates a table
    pub fn create_table(&mut self, table: schema::Table) -> Result<(), Error> {
        if self.table_exists(&table.name)? {
            Err(Error::Value(format!("Table {} already exists", table.name)))
        } else {
            self.kv.write()?.set(&Self::key_table(&table.name), Self::serialize(table)?)?;
            Ok(())
        }
    }

    /// Deletes a table
    pub fn drop_table(&mut self, table: &str) -> Result<(), Error> {
        self.get_table(table)?;
        self.kv.write()?.delete(&Self::key_table(table))?;
        Ok(())
    }

    /// Fetches a table schema
    pub fn get_table(&self, table: &str) -> Result<schema::Table, Error> {
        Self::deserialize(
            self.kv
                .read()?
                .get(&Self::key_table(table))?
                .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?,
        )
    }

    /// Checks if a table exists
    pub fn table_exists(&self, table: &str) -> Result<bool, Error> {
        Ok(self.kv.read()?.get(&Self::key_table(table))?.is_some())
    }

    /// Generates a key for a row
    fn key_row(table: &str, id: &str) -> String {
        format!("{}.{}", Self::key_table(table), id)
    }

    /// Generates a key for a table
    fn key_table(table: &str) -> String {
        format!("schema.table.{}", table)
    }

    /// Deserializes a value from a byte buffer
    fn deserialize<'de, V: serde::Deserialize<'de>>(bytes: Vec<u8>) -> Result<V, Error> {
        Ok(serde::Deserialize::deserialize(&mut rmps::Deserializer::new(&bytes[..]))?)
    }

    /// Serializes a value into a byte buffer
    fn serialize<V: serde::Serialize>(value: V) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        value.serialize(&mut rmps::Serializer::new(&mut bytes))?;
        Ok(bytes)
    }
}
