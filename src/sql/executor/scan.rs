use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// A table scan node
pub struct Scan {
    columns: Vec<String>,
    range: Box<dyn Iterator<Item = Result<Row, Error>> + Sync + Send + 'static>,
}

impl Scan {
    pub fn execute(ctx: &mut Context, table: String) -> Result<Box<dyn Executor>, Error> {
        let schema = ctx
            .storage
            .get_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} not found", table)))?;
        Ok(Box::new(Self {
            columns: schema.columns.iter().map(|c| c.name.clone()).collect(),
            range: ctx.storage.scan_rows(&table),
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
