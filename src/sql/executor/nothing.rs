use super::super::engine::Transaction;
use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// A source node which produces a single empty row
pub struct Nothing {
    done: bool,
}

impl Nothing {
    pub fn execute<T: Transaction>(_: &mut Context<T>) -> Result<Box<dyn Executor>, Error> {
        Ok(Box::new(Self { done: false }))
    }
}

impl Executor for Nothing {
    fn columns(&self) -> Vec<String> {
        Vec::new()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            Ok(Some(Row::new()))
        }
    }
}
