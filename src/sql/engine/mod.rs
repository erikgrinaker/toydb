mod kv;
mod raft;

pub use crate::kv::Mode;
pub use kv::KV;
pub use raft::Raft;

use super::types;
use super::types::schema;
use crate::Error;

pub trait Engine {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction, Error> {
        self.begin_with_mode(Mode::ReadWrite)
    }

    fn begin_with_mode(&self, mode: Mode) -> Result<Self::Transaction, Error>;
    fn resume(&self, id: u64) -> Result<Self::Transaction, Error>;
}

pub trait Transaction {
    fn id(&self) -> u64;
    fn mode(&self) -> Mode;
    fn commit(self) -> Result<(), Error>;
    fn rollback(self) -> Result<(), Error>;

    fn create(&mut self, table: &str, row: types::Row) -> Result<(), Error>;
    fn delete(&mut self, table: &str, id: &types::Value) -> Result<(), Error>;
    fn read(&self, table: &str, id: &types::Value) -> Result<Option<types::Row>, Error>;
    fn scan(&self, table: &str) -> Result<Scan, Error>;
    fn update(&mut self, table: &str, id: &types::Value, row: types::Row) -> Result<(), Error>;

    fn create_table(&mut self, table: &schema::Table) -> Result<(), Error>;
    fn delete_table(&mut self, table: &str) -> Result<(), Error>;
    fn read_table(&self, table: &str) -> Result<Option<schema::Table>, Error>;
    fn scan_tables(&self) -> Result<TableScan, Error>;

    fn must_read_table(&self, table: &str) -> Result<schema::Table, Error> {
        self.read_table(table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))
    }
}

pub type Scan =
    Box<dyn DoubleEndedIterator<Item = Result<types::Row, Error>> + 'static + Sync + Send>;

pub type TableScan = Box<dyn DoubleEndedIterator<Item = schema::Table> + 'static + Sync + Send>;
