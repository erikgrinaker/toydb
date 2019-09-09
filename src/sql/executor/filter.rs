use super::super::expression::{Environment, Expression};
use super::super::types::{Row, Value};
use super::{Context, Executor};
use crate::Error;

/// A filter executor
pub struct Filter {
    source: Box<dyn Executor>,
    predicate: Expression,
}

impl Filter {
    pub fn execute(
        _: &mut Context,
        source: Box<dyn Executor>,
        predicate: Expression,
    ) -> Result<Box<dyn Executor>, Error> {
        Ok(Box::new(Self { source, predicate }))
    }
}

impl Executor for Filter {
    fn close(&mut self) {
        // FIXME
        //self.source = Box::new(EmptyResult)
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        while let Some(row) = self.source.fetch()? {
            match self.predicate.evaluate(&Environment::empty())? {
                Value::Boolean(true) => return Ok(Some(row)),
                Value::Boolean(false) => {}
                Value::Null => {}
                value => {
                    return Err(Error::Value(format!(
                        "Unexpected value {} for filter, expected boolean",
                        value
                    )))
                }
            }
        }
        Ok(None)
    }
}
