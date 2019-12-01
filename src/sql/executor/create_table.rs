use super::super::engine::Transaction;
use super::super::schema;
use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// A CREATE TABLE executor
pub struct CreateTable;

impl CreateTable {
    pub fn execute<T: Transaction>(
        ctx: &mut Context<T>,
        schema: schema::Table,
    ) -> Result<Box<dyn Executor>, Error> {
        ctx.txn.create_table(&schema)?;
        Ok(Box::new(Self))
    }
}

impl Executor for CreateTable {
    fn columns(&self) -> Vec<String> {
        Vec::new()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        Ok(None)
    }
}
