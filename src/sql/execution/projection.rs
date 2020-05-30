use super::super::engine::Transaction;
use super::super::types::{Column, Expression};
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

/// A projection executor
pub struct Projection<T: Transaction> {
    source: Box<dyn Executor<T>>,
    expressions: Vec<(Expression, Option<String>)>,
}

impl<T: Transaction> Projection<T> {
    pub fn new(
        source: Box<dyn Executor<T>>,
        expressions: Vec<(Expression, Option<String>)>,
    ) -> Box<Self> {
        Box::new(Self { source, expressions })
    }
}

impl<T: Transaction> Executor<T> for Projection<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Query { columns, rows } = self.source.execute(txn)? {
            let (expressions, labels): (Vec<Expression>, Vec<Option<String>>) =
                self.expressions.into_iter().unzip();
            let columns = expressions
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    if let Some(Some(label)) = labels.get(i) {
                        Column { name: Some(label.clone()) }
                    } else if let Expression::Field(i, _) = e {
                        columns.get(*i).cloned().unwrap_or(Column { name: None })
                    } else {
                        Column { name: None }
                    }
                })
                .collect();
            let rows = Box::new(rows.map(move |r| {
                r.and_then(|row| {
                    Ok(expressions.iter().map(|e| e.evaluate(Some(&row))).collect::<Result<_>>()?)
                })
            }));
            Ok(ResultSet::Query { columns, rows })
        } else {
            Err(Error::Internal("Unexpected result".into()))
        }
    }
}
