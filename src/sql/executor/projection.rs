use super::super::engine::Transaction;
use super::super::types::expression::{Environment, Expression, Expressions};
use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

pub struct Projection {
    source: Box<dyn Executor>,
    source_columns: Vec<String>,
    columns: Vec<String>,
    expressions: Expressions,
}

impl Projection {
    pub fn execute<T: Transaction>(
        _: &mut Context<T>,
        source: Box<dyn Executor>,
        labels: Vec<Option<String>>,
        expressions: Expressions,
    ) -> Result<Box<dyn Executor>, Error> {
        let columns = expressions
            .iter()
            .enumerate()
            .map(|(i, expr)| {
                if let Some(Some(label)) = labels.get(i) {
                    label.clone()
                } else if let Expression::Field(field) = expr {
                    field.clone()
                } else {
                    "?".to_string()
                }
            })
            .collect();
        Ok(Box::new(Self { source_columns: source.columns(), source, columns, expressions }))
    }
}

impl Executor for Projection {
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        if let Some(row) = self.source.fetch()? {
            let env = Environment::new(
                self.source_columns.iter().cloned().zip(row.iter().cloned()).collect(),
            );
            Ok(Some(
                self.expressions.iter().map(|e| e.evaluate(&env)).collect::<Result<_, Error>>()?,
            ))
        } else {
            Ok(None)
        }
    }
}
