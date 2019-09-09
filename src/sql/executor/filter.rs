use super::super::expression::{Environment, Expression};
use super::super::types::{Row, Value};
use super::{Context, Executor};
use crate::Error;

/// A filter executor
pub struct Filter {
    source: Box<dyn Executor>,
    columns: Vec<String>,
    predicate: Expression,
}

impl Filter {
    pub fn execute(
        _: &mut Context,
        source: Box<dyn Executor>,
        predicate: Expression,
    ) -> Result<Box<dyn Executor>, Error> {
        Ok(Box::new(Self { columns: source.columns(), source, predicate }))
    }
}

impl Executor for Filter {
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        while let Some(row) = self.source.fetch()? {
            let env =
                Environment::new(self.columns.iter().cloned().zip(row.iter().cloned()).collect());
            match self.predicate.evaluate(&env)? {
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
