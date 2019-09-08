use super::super::schema;
use super::super::types::Row;
use super::{Context, Node};
use crate::Error;

/// A CREATE TABLE node
#[derive(Debug)]
pub struct CreateTable {
    schema: schema::Table,
}

impl CreateTable {
    pub fn new(schema: schema::Table) -> Self {
        Self { schema }
    }
}

impl Node for CreateTable {
    fn execute(&mut self, ctx: &mut Context) -> Result<(), Error> {
        ctx.storage.create_table(&self.schema)
    }
}

impl Iterator for CreateTable {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
