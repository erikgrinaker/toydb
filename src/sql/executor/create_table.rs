use super::super::engine::Transaction;
use super::super::types::schema::Table;
use super::{Context, Executor, ResultSet};
use crate::Error;

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
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        ctx.txn.create_table(&self.table)?;
        Ok(ResultSet::CreateTable { name: self.table.name })
    }
}
