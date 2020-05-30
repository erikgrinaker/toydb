use super::super::engine::Transaction;
use super::super::schema::Table;
use super::{Executor, ResultSet};
use crate::error::Result;

/// A CREATE TABLE executor
pub struct CreateTable {
    /// The table schema
    table: Table,
}

impl CreateTable {
    pub fn new(table: Table) -> Box<Self> {
        Box::new(Self { table })
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        txn.create_table(&self.table)?;
        Ok(ResultSet::CreateTable { name: self.table.name })
    }
}
