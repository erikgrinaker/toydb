use super::super::engine::Transaction;
use super::super::types::expression::{Expression, Expressions};
use super::{Context, Executor, ResultSet};
use crate::Error;

use std::collections::HashMap;

/// A filter executor
pub struct Projection<T: Transaction> {
    /// The source of rows to project
    source: Box<dyn Executor<T>>,
    /// Labels for each column, if any
    labels: Vec<Option<String>>,
    /// Expressions to project
    expressions: Expressions,
}

impl<T: Transaction> Projection<T> {
    pub fn new(
        source: Box<dyn Executor<T>>,
        labels: Vec<Option<String>>,
        expressions: Expressions,
    ) -> Box<Self> {
        Box::new(Self { source, labels, expressions })
    }
}

impl<T: Transaction> Executor<T> for Projection<T> {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        let mut result = self.source.execute(ctx)?;
        let columns = result.columns;
        let labels = self.labels;
        result.columns = self
            .expressions
            .iter()
            .enumerate()
            .map(|(i, e)| {
                if let Some(Some(label)) = labels.get(i) {
                    label.clone()
                } else if let Expression::Field(field) = e {
                    field.clone()
                } else {
                    "?".to_string()
                }
            })
            .collect();
        if let Some(rows) = result.rows {
            let expressions = self.expressions;
            result.rows = Some(Box::new(rows.map(move |r| {
                r.and_then(|row| {
                    let env: HashMap<_, _> = columns.iter().cloned().zip(row.into_iter()).collect();
                    Ok(expressions
                        .iter()
                        .map(|e| e.evaluate(&env))
                        .collect::<Result<_, Error>>()?)
                })
            })));
        }
        Ok(result)
    }
}
