use super::super::engine::Transaction;
use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// A table scan node
pub struct Scan {
    columns: Vec<String>,
    range: super::super::engine::Scan,
}

impl Scan {
    pub fn execute<T: Transaction>(
        ctx: &mut Context<T>,
        table: String,
    ) -> Result<Box<dyn Executor>, Error> {
        let schema = ctx
            .txn
            .read_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} not found", table)))?;
        Ok(Box::new(Self {
            columns: schema.columns.iter().map(|c| c.name.clone()).collect(),
            range: ctx.txn.scan(&table)?,
        }))
    }
}

impl Executor for Scan {
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        self.range.next().transpose()
    }
}
