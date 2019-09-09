use super::super::expression::{Environment, Expressions};
use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

pub struct Projection {
    source: Box<dyn Executor>,
    //labels: Vec<String>,
    expressions: Expressions,
}

impl Projection {
    pub fn execute(
        _: &mut Context,
        source: Box<dyn Executor>,
        _labels: Vec<String>,
        expressions: Expressions,
    ) -> Result<Box<dyn Executor>, Error> {
        Ok(Box::new(Self { source, expressions }))
    }
}

impl Executor for Projection {
    fn close(&mut self) {}

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        let env = Environment::empty();
        self.source
            .fetch()?
            .map(|_| self.expressions.iter().map(|e| e.evaluate(&env)).collect())
            .transpose()
    }
}
