use super::super::engine::Transaction;
use super::super::types::{Column, Expression, Expressions};
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

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
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Query { columns, rows } => {
                let labels = self.labels;
                let columns = self
                    .expressions
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        Ok(if let Some(Some(label)) = labels.get(i) {
                            Column { table: None, name: Some(label.clone()) }
                        } else if let Expression::Field(i, _) = e {
                            columns[*i].clone()
                        } else {
                            Column { table: None, name: None }
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let expressions = self.expressions;
                let rows = Box::new(rows.map(move |r| {
                    r.and_then(|row| {
                        Ok(expressions
                            .iter()
                            .map(|e| e.evaluate(Some(&row)))
                            .collect::<Result<_>>()?)
                    })
                }));
                Ok(ResultSet::Query { columns, rows })
            }
            r => Err(Error::Internal(format!("Unexpected result {:?}", r))),
        }
    }
}
