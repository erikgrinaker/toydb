use super::super::engine::Transaction;
use super::super::schema::Table;
use super::{Executor, ResultSet};
use crate::error::Result;

/// A CREATE TABLE executor
pub struct CreateTable {
    table: Table,
}

impl CreateTable {
    pub fn new(table: Table) -> Box<Self> {
        Box::new(Self { table })
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let name = self.table.name.clone();
        txn.create_table(self.table)?;
        Ok(ResultSet::CreateTable { name })
    }
}

/// A DROP TABLE executor
pub struct DropTable {
    table: String,
}

impl DropTable {
    pub fn new(table: String) -> Box<Self> {
        Box::new(Self { table })
    }
}

impl<T: Transaction> Executor<T> for DropTable {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        txn.delete_table(&self.table)?;
        Ok(ResultSet::DropTable { name: self.table })
    }
}
