use super::schema;
use super::types;
use crate::kv;
use crate::utility::{deserialize, serialize};
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
        self.kv.write()?.set(&Self::key_row(&table.name, &id.to_string()), serialize(row)?)?;
        Ok(())
    }

    /// Creates a table
    pub fn create_table(&mut self, table: schema::Table) -> Result<(), Error> {
        if self.table_exists(&table.name)? {
            Err(Error::Value(format!("Table {} already exists", table.name)))
        } else {
            self.kv.write()?.set(&Self::key_table(&table.name), serialize(table)?)?;
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
        deserialize(
            self.kv
                .read()?
                .get(&Self::key_table(table))?
                .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?,
        )
    }

    /// Lists tables
    pub fn list_tables(&self) -> Result<Vec<String>, Error> {
        let mut iter = self.kv.read()?.iter_prefix("schema.table");
        let mut tables = Vec::new();
        while let Some((_, value)) = iter.next().transpose()? {
            let schema: schema::Table = deserialize(value)?;
            tables.push(schema.name)
        }
        Ok(tables)
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
}
