use super::super::types::Row;
use super::{Context, Node};
use crate::Error;

/// A CREATE TABLE node
#[derive(Debug)]
pub struct DropTable {
    table: String,
}

impl DropTable {
    pub fn new(table: String) -> Self {
        Self { table }
    }
}

impl Node for DropTable {
    fn execute(&mut self, ctx: &mut Context) -> Result<(), Error> {
        ctx.storage.delete_table(&self.table)
    }
}

impl Iterator for DropTable {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
