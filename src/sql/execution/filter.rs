use super::super::engine::Transaction;
use super::super::types::Expression;
use super::super::types::Value;
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

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
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let result = self.source.execute(txn)?;
        if let ResultSet::Query { columns, rows } = result {
            let predicate = self.predicate;
            Ok(ResultSet::Query {
                columns,
                rows: Box::new(rows.filter_map(move |r| {
                    r.and_then(|row| match predicate.evaluate(Some(&row))? {
                        Value::Boolean(true) => Ok(Some(row)),
                        Value::Boolean(false) => Ok(None),
                        Value::Null => Ok(None),
                        value => Err(Error::Value(format!(
                            "Filter returned {}, expected boolean",
                            value
                        ))),
                    })
                    .transpose()
                })),
            })
        } else {
            Err(Error::Internal("Unexpected result".into()))
        }
    }
}
