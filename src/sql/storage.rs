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

impl Storage {
    /// Creates a new Storage
    pub fn new<S: kv::Store>(store: S) -> Self {
        Storage { kv: Arc::new(RwLock::new(Box::new(store))) }
    }

    /// Creates a row
    pub fn create_row(&mut self, table: &str, row: types::Row) -> Result<(), Error> {
        let table = self
            .get_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?;
        table.validate_row(&row)?;
        let pk = row.get(table.primary_key).unwrap();
        if self.get_row(&table.name, &pk)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} already exists for table {}",
                pk, table.name
            )));
        }
        self.kv.write()?.set(&Self::key_row(&table.name, &pk.to_string()), serialize(row)?)?;
        Ok(())
    }

    /// Creates a table
    pub fn create_table(&mut self, table: &schema::Table) -> Result<(), Error> {
        if self.get_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }
        self.kv.write()?.set(&Self::key_table(&table.name), serialize(table)?)
    }

    /// Deletes a row
    pub fn delete_row(&mut self, table: &str, id: &types::Value) -> Result<(), Error> {
        self.kv.write()?.delete(&Self::key_row(table, &id.to_string()))
    }

    /// Deletes a table
    pub fn delete_table(&mut self, table: &str) -> Result<(), Error> {
        if self.get_table(table)?.is_none() {
            return Err(Error::Value(format!("Table {} does not exist", table)));
        }
        self.kv.write()?.delete(&Self::key_table(table))
    }

    /// Fetches a row
    pub fn get_row(&self, table: &str, id: &types::Value) -> Result<Option<types::Row>, Error> {
        self.kv.read()?.get(&Self::key_row(table, &id.to_string()))?.map(deserialize).transpose()
    }

    /// Fetches a table schema
    pub fn get_table(&self, table: &str) -> Result<Option<schema::Table>, Error> {
        self.kv.read()?.get(&Self::key_table(table))?.map(deserialize).transpose()
    }

    /// Lists tables
    pub fn list_tables(&self) -> Result<Vec<String>, Error> {
        let mut iter = self.kv.read()?.iter_prefix(b"schema.table");
        let mut tables = Vec::new();
        while let Some((_, value)) = iter.next().transpose()? {
            let schema: schema::Table = deserialize(value)?;
            tables.push(schema.name)
        }
        Ok(tables)
    }

    /// Creates an iterator over the rows of a table
    pub fn scan_rows(
        &self,
        table: &str,
    ) -> Box<dyn Iterator<Item = Result<types::Row, Error>> + Sync + Send> {
        let key = table.to_string() + ".";
        Box::new(self.kv.read().unwrap().iter_prefix(&key.as_bytes()).map(|res| match res {
            Ok((_, v)) => deserialize(v),
            Err(err) => Err(err),
        }))
    }

    /// Updates a row
    pub fn update_row(
        &mut self,
        table: &str,
        id: &types::Value,
        row: types::Row,
    ) -> Result<(), Error> {
        // FIXME For now, we're lazy and just do a delete + create
        self.delete_row(table, id)?;
        self.create_row(table, row)
    }

    /// Generates a key for a row
    fn key_row(table: &str, id: &str) -> Vec<u8> {
        format!("{}.{}", table, id).as_bytes().to_vec()
    }

    /// Generates a key for a table
    fn key_table(table: &str) -> Vec<u8> {
        format!("schema.table.{}", table).as_bytes().to_vec()
    }
}
