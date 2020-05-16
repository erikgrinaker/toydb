use super::super::engine::Transaction;
use super::super::types::{Column, Expression, Expressions, Relation};
use super::{Context, Executor, ResultColumns, ResultSet};
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
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet> {
        match self.source.execute(ctx)? {
            ResultSet::Query { relation } => {
                let labels = self.labels;
                let columns = ResultColumns::from_new_columns(relation.columns);
                let mut projection = Relation {
                    columns: self
                        .expressions
                        .iter()
                        .enumerate()
                        .map(|(i, e)| {
                            Ok(if let Some(Some(label)) = labels.get(i) {
                                Column { relation: None, name: Some(label.clone()) }
                            } else if let Expression::Field(relation, field) = e {
                                let (r, f) = columns.get(relation.as_deref(), field)?;
                                Column { relation: r, name: Some(f) }
                            } else if let Expression::Column(i) = e {
                                let (r, f) = columns.columns[*i].clone(); // FIXME Should have method for this
                                Column { relation: r, name: f }
                            } else {
                                Column { relation: None, name: None }
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    rows: None,
                };
                if let Some(rows) = relation.rows {
                    let expressions = self.expressions;
                    projection.rows = Some(Box::new(rows.map(move |r| {
                        r.and_then(|row| {
                            let env = columns.as_env(&row);
                            Ok(expressions
                                .iter()
                                .map(|e| e.evaluate(&env))
                                .collect::<Result<_>>()?)
                        })
                    })));
                }
                Ok(ResultSet::Query { relation: projection })
            }
            r => Err(Error::Internal(format!("Unexpected result {:?}", r))),
        }
    }
}
