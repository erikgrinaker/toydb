use super::super::expression::{Environment, Expression};
use super::super::types::{Row, Value};
use super::{Context, Node};
use crate::Error;

/// A filter node
#[derive(Debug)]
pub struct Filter {
    source: Box<dyn Node>,
    predicate: Expression,
}

impl Filter {
    pub fn new(source: Box<dyn Node>, predicate: Expression) -> Self {
        Self { source, predicate }
    }
}

impl Node for Filter {
    fn execute(&mut self, ctx: &mut Context) -> Result<(), Error> {
        self.source.execute(ctx)
    }
}

impl Iterator for Filter {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let row = match self.source.next()? {
                Err(err) => return Some(Err(err)),
                Ok(r) => r,
            };
            match self.predicate.evaluate(&Environment::empty()) {
                Err(err) => return Some(Err(err)),
                Ok(Value::Boolean(true)) => return Some(Ok(row)),
                Ok(Value::Boolean(false)) => {}
                Ok(Value::Null) => {}
                Ok(value) => {
                    return Some(Err(Error::Value(format!(
                        "Unexpected value {} for filter, expected boolean",
                        value
                    ))))
                }
            }
        }
    }
}
