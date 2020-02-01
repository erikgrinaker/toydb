use super::super::engine::Transaction;
use super::super::types::expression::Expression;
use super::super::types::Value;
use super::{Context, Executor, ResultSet};
use crate::Error;

use std::collections::HashMap;

/// A filter executor
pub struct Filter<T: Transaction> {
    /// The source of rows to filter
    source: Box<dyn Executor<T>>,
    /// The predicate to filter by
    predicate: Expression,
}

impl<T: Transaction> Filter<T> {
    pub fn new(source: Box<dyn Executor<T>>, predicate: Expression) -> Box<Self> {
        Box::new(Self { source, predicate })
    }
}

impl<T: Transaction> Executor<T> for Filter<T> {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        let mut result = self.source.execute(ctx)?;
        if let Some(rows) = result.rows {
            let columns = result.columns.clone();
            let predicate = self.predicate;
            result.rows = Some(Box::new(rows.filter_map(move |r| {
                r.and_then(|row| {
                    let env: HashMap<_, _> =
                        columns.iter().cloned().zip(row.iter().cloned()).collect();
                    match predicate.evaluate(&env)? {
                        Value::Boolean(true) => Ok(Some(row)),
                        Value::Boolean(false) => Ok(None),
                        Value::Null => Ok(None),
                        value => Err(Error::Value(format!(
                            "Filter returned {}, expected boolean",
                            value
                        ))),
                    }
                })
                .transpose()
            })));
        }
        Ok(result)
    }
}
