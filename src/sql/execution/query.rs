use super::super::engine::Transaction;
use super::super::plan::Direction;
use super::super::types::{Column, Expression, Row, Value};
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

/// A filter executor
pub struct Filter<T: Transaction> {
    source: Box<dyn Executor<T>>,
    predicate: Expression,
}

impl<T: Transaction> Filter<T> {
    pub fn new(source: Box<dyn Executor<T>>, predicate: Expression) -> Box<Self> {
        Box::new(Self { source, predicate })
    }
}

impl<T: Transaction> Executor<T> for Filter<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Query { columns, rows } = self.source.execute(txn)? {
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
                    expressions.iter().map(|e| e.evaluate(Some(&row))).collect::<Result<_>>()
                })
            }));
            Ok(ResultSet::Query { columns, rows })
        } else {
            Err(Error::Internal("Unexpected result".into()))
        }
    }
}

/// An ORDER BY executor
pub struct Order<T: Transaction> {
    source: Box<dyn Executor<T>>,
    order: Vec<(Expression, Direction)>,
}

impl<T: Transaction> Order<T> {
    pub fn new(source: Box<dyn Executor<T>>, order: Vec<(Expression, Direction)>) -> Box<Self> {
        Box::new(Self { source, order })
    }
}

impl<T: Transaction> Executor<T> for Order<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Query { columns, mut rows } => {
                // FIXME Since we can't return errors from the sort_by closure, we have to
                // pre-evaluate all values. This means that we can't short-circuit evaluation,
                // and have to temporarily store evaluated values, which is bad for performance
                // and memory usage respectively
                struct Item {
                    row: Row,
                    values: Vec<Value>,
                }

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

/// A LIMIT executor
pub struct Limit<T: Transaction> {
    source: Box<dyn Executor<T>>,
    limit: u64,
}

impl<T: Transaction> Limit<T> {
    pub fn new(source: Box<dyn Executor<T>>, limit: u64) -> Box<Self> {
        Box::new(Self { source, limit })
    }
}

impl<T: Transaction> Executor<T> for Limit<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Query { columns, rows } = self.source.execute(txn)? {
            Ok(ResultSet::Query { columns, rows: Box::new(rows.take(self.limit as usize)) })
        } else {
            Err(Error::Internal("Unexpected result".into()))
        }
    }
}

/// An OFFSET executor
pub struct Offset<T: Transaction> {
    source: Box<dyn Executor<T>>,
    offset: u64,
}

impl<T: Transaction> Offset<T> {
    pub fn new(source: Box<dyn Executor<T>>, offset: u64) -> Box<Self> {
        Box::new(Self { source, offset })
    }
}

impl<T: Transaction> Executor<T> for Offset<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Query { columns, rows } = self.source.execute(txn)? {
            Ok(ResultSet::Query { columns, rows: Box::new(rows.skip(self.offset as usize)) })
        } else {
            Err(Error::Internal("Unexpected result".into()))
        }
    }
}
