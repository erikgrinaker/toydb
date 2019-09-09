use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// A table scan node
pub struct Scan {
    range: Box<dyn Iterator<Item = Result<Row, Error>> + Sync + Send + 'static>,
}

impl Scan {
    pub fn execute(ctx: &mut Context, table: String) -> Result<Box<dyn Executor>, Error> {
        Ok(Box::new(Self { range: ctx.storage.scan_rows(&table) }))
    }
}

impl Executor for Scan {
    fn close(&mut self) {
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        self.range.next().transpose()
    }
}
