use super::super::engine::Transaction;
use super::super::types::Expression;
use super::super::types::Value;
use super::{Context, Executor, ResultColumns, ResultSet};
use crate::Error;

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
        let result = self.source.execute(ctx)?;
        if let ResultSet::Query { mut relation } = result {
            if let Some(rows) = relation.rows {
                let columns = ResultColumns::from_new_columns(relation.columns.clone());
                let predicate = self.predicate;
                relation.rows = Some(Box::new(rows.filter_map(move |r| {
                    r.and_then(|row| match predicate.evaluate(&columns.as_env(&row))? {
                        Value::Boolean(true) => Ok(Some(row)),
                        Value::Boolean(false) => Ok(None),
                        Value::Null => Ok(None),
                        value => Err(Error::Value(format!(
                            "Filter returned {}, expected boolean",
                            value
                        ))),
                    })
                    .transpose()
                })));
            }
            return Ok(ResultSet::Query { relation });
        }
        Err(Error::Internal("Unexpected result".into()))
    }
}
