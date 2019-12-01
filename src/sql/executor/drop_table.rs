use super::super::engine::Transaction;
use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// A DROP TABLE executor
pub struct DropTable;

impl DropTable {
    pub fn execute<T: Transaction>(
        ctx: &mut Context<T>,
        name: String,
    ) -> Result<Box<dyn Executor>, Error> {
        ctx.txn.delete_table(&name)?;
        Ok(Box::new(Self))
    }
}

impl Executor for DropTable {
    fn columns(&self) -> Vec<String> {
        Vec::new()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        Ok(None)
    }
}
