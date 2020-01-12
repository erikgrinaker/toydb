use super::super::engine::Transaction;
use super::{Context, Effect, Executor, ResultSet};
use crate::Error;

/// A DROP TABLE executor
pub struct DropTable {
    /// Table name to drop
    table: String,
}

impl DropTable {
    pub fn new(table: String) -> Box<Self> {
        Box::new(Self { table })
    }
}

impl<T: Transaction> Executor<T> for DropTable {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        ctx.txn.delete_table(&self.table)?;
        Ok(ResultSet::from_effect(Effect::DropTable { name: self.table }))
    }
}
