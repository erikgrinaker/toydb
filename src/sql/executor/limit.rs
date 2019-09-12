use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// A limit executor
pub struct Limit {
    source: Box<dyn Executor>,
    limit: u64,
    fetched: u64,
}

impl Limit {
    pub fn execute(
        _: &mut Context,
        source: Box<dyn Executor>,
        limit: u64,
    ) -> Result<Box<dyn Executor>, Error> {
        Ok(Box::new(Self { source, limit, fetched: 0 }))
    }
}

impl Executor for Limit {
    fn columns(&self) -> Vec<String> {
        self.source.columns()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        if self.fetched >= self.limit {
            Ok(None)
        } else if let Some(row) = self.source.fetch()? {
            self.fetched += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}
