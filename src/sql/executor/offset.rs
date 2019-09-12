use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// An offset executor
pub struct Offset {
    source: Box<dyn Executor>,
}

impl Offset {
    pub fn execute(
        _: &mut Context,
        mut source: Box<dyn Executor>,
        offset: u64,
    ) -> Result<Box<dyn Executor>, Error> {
        let mut fetched = 0;
        while fetched < offset && source.fetch()?.is_some() {
            fetched += 1
        }
        Ok(Box::new(Self { source }))
    }
}

impl Executor for Offset {
    fn columns(&self) -> Vec<String> {
        self.source.columns()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        self.source.fetch()
    }
}
