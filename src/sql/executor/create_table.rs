use super::super::schema;
use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// A CREATE TABLE executor
pub struct CreateTable;

impl CreateTable {
    pub fn execute(ctx: &mut Context, schema: schema::Table) -> Result<Box<dyn Executor>, Error> {
        ctx.storage.create_table(&schema)?;
        Ok(Box::new(Self))
    }
}

impl Executor for CreateTable {
    fn close(&mut self) {}

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        Ok(None)
    }
}
