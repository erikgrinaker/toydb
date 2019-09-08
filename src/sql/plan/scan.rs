use super::super::types::Row;
use super::{Context, Node};
use crate::Error;

/// A table scan node
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Scan {
    table: String,
    #[derivative(Debug = "ignore")]
    range: Option<Box<dyn Iterator<Item = Result<Row, Error>> + Sync + Send + 'static>>,
}

impl Scan {
    pub fn new(table: String) -> Self {
        Self { table, range: None }
    }
}

impl Node for Scan {
    fn execute(&mut self, ctx: &mut Context) -> Result<(), Error> {
        self.range = Some(ctx.storage.scan_rows(&self.table));
        Ok(())
    }
}

impl Iterator for Scan {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.range {
            Some(ref mut iter) => iter.next(),
            None => None,
        }
    }
}
