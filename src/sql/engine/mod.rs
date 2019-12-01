mod kv;
mod raft;

pub use kv::KV;
pub use raft::Raft;

use super::schema;
use super::types;
use crate::Error;

pub trait Engine {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction, Error>;
}

pub trait Transaction {
    fn id(&self) -> u64;
    fn commit(self) -> Result<(), Error>;
    fn rollback(self) -> Result<(), Error>;

    fn create(&mut self, table: &str, row: types::Row) -> Result<(), Error>;
    fn delete(&mut self, table: &str, id: &types::Value) -> Result<(), Error>;
    fn read(&self, table: &str, id: &types::Value) -> Result<Option<types::Row>, Error>;
    fn scan(&self, table: &str) -> Result<Scan, Error>;
    fn update(&mut self, table: &str, id: &types::Value, row: types::Row) -> Result<(), Error>;

    fn create_table(&mut self, table: &schema::Table) -> Result<(), Error>;
    fn delete_table(&mut self, table: &str) -> Result<(), Error>;
    fn list_tables(&self) -> Result<Vec<schema::Table>, Error>;
    fn read_table(&self, table: &str) -> Result<Option<schema::Table>, Error>;
}

pub type Scan =
    Box<dyn DoubleEndedIterator<Item = Result<types::Row, Error>> + Sync + Send + 'static>;
