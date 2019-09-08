use super::super::types::Row;
use super::{Context, Node};
use crate::Error;

/// A source node which produces a single empty row
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Nothing {
    #[derivative(Debug = "ignore")]
    done: bool,
}

impl Nothing {
    pub fn new() -> Self {
        Self { done: false }
    }
}

impl Node for Nothing {
    fn execute(&mut self, _: &mut Context) -> Result<(), Error> {
        Ok(())
    }
}

impl Iterator for Nothing {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            None
        } else {
            self.done = true;
            Some(Ok(Row::new()))
        }
    }
}
