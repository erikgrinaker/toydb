use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// A DROP TABLE executor
pub struct DropTable;

impl DropTable {
    pub fn execute(ctx: &mut Context, name: String) -> Result<Box<dyn Executor>, Error> {
        ctx.storage.delete_table(&name)?;
        Ok(Box::new(Self))
    }
}

impl Executor for DropTable {
    fn close(&mut self) {}

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        Ok(None)
    }
}
