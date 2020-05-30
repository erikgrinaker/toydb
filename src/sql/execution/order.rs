use super::super::engine::Transaction;
use super::super::plan::Direction;
use super::super::types::{Expression, Row, Value};
use super::{Context, Executor, ResultSet};
use crate::error::{Error, Result};

/// An order executor
pub struct Order<T: Transaction> {
    /// The source of rows to filter
    source: Box<dyn Executor<T>>,
    /// The sort orders
    order: Vec<(Expression, Direction)>,
}

impl<T: Transaction> Order<T> {
    pub fn new(source: Box<dyn Executor<T>>, order: Vec<(Expression, Direction)>) -> Box<Self> {
        Box::new(Self { source, order })
    }
}

impl<T: Transaction> Executor<T> for Order<T> {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet> {
        match self.source.execute(ctx)? {
            ResultSet::Query { columns, mut rows } => {
                // FIXME Since we can't return errors from the sort_by closure, we have to
                // pre-evaluate all values. This means that we can't short-circuit evaluation,
                // and have to temporarily store evaluated values, which is bad for performance
                // and memory usage respectively
                struct Item {
                    row: Row,
                    values: Vec<Value>,
                };
                let mut items = Vec::new();
                while let Some(row) = rows.next().transpose()? {
                    let mut values = Vec::new();
                    for (expr, _) in self.order.iter() {
                        values.push(expr.evaluate(Some(&row))?);
                    }
                    items.push(Item { row, values })
                }

                let order = &self.order;
                items.sort_by(|a, b| {
                    for (i, (_, order)) in order.iter().enumerate() {
                        let value_a = &a.values[i];
                        let value_b = &b.values[i];
                        match value_a.partial_cmp(value_b) {
                            Some(std::cmp::Ordering::Equal) => {}
                            Some(o) => {
                                return if *order == Direction::Ascending { o } else { o.reverse() }
                            }
                            None => {}
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                Ok(ResultSet::Query {
                    columns,
                    rows: Box::new(items.into_iter().map(|i| Ok(i.row))),
                })
            }
            r => Err(Error::Internal(format!("Unexpected result {:?}", r))),
        }
    }
}
