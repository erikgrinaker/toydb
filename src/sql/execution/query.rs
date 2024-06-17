use super::QueryIterator;
use crate::errinput;
use crate::error::Result;
use crate::sql::plan::Direction;
use crate::sql::types::{Column, Expression, Row, Value};

/// A filter executor
pub struct Filter {
    source: QueryIterator,
    predicate: Expression,
}

impl Filter {
    pub fn new(source: QueryIterator, predicate: Expression) -> Self {
        Self { source, predicate }
    }

    pub fn execute(self) -> QueryIterator {
        QueryIterator {
            columns: self.source.columns,
            rows: Box::new(self.source.rows.filter_map(move |r| {
                r.and_then(|row| match self.predicate.evaluate(Some(&row))? {
                    Value::Boolean(true) => Ok(Some(row)),
                    Value::Boolean(false) => Ok(None),
                    Value::Null => Ok(None),
                    value => errinput!("filter returned {value}, expected boolean",),
                })
                .transpose()
            })),
        }
    }
}

/// A projection executor
pub struct Projection {
    source: QueryIterator,
    expressions: Vec<(Expression, Option<String>)>,
}

impl Projection {
    pub fn new(source: QueryIterator, expressions: Vec<(Expression, Option<String>)>) -> Self {
        Self { source, expressions }
    }

    pub fn execute(self) -> QueryIterator {
        let (expressions, labels): (Vec<Expression>, Vec<Option<String>>) =
            self.expressions.into_iter().unzip();
        let columns = expressions
            .iter()
            .enumerate()
            .map(|(i, e)| {
                if let Some(Some(label)) = labels.get(i) {
                    Column { name: Some(label.clone()) }
                } else if let Expression::Field(i, _) = e {
                    self.source.columns.get(*i).cloned().unwrap_or(Column { name: None })
                } else {
                    Column { name: None }
                }
            })
            .collect();
        let rows = Box::new(self.source.rows.map(move |r| {
            r.and_then(|row| {
                expressions.iter().map(|e| e.evaluate(Some(&row))).collect::<Result<_>>()
            })
        }));
        QueryIterator { columns, rows }
    }
}

/// An ORDER BY executor
pub struct Order {
    source: QueryIterator,
    order: Vec<(Expression, Direction)>,
}

impl Order {
    pub fn new(source: QueryIterator, order: Vec<(Expression, Direction)>) -> Self {
        Self { source, order }
    }

    pub fn execute(mut self) -> Result<QueryIterator> {
        // FIXME Since we can't return errors from the sort_by closure, we have
        // to pre-evaluate all values. This means that we can't short-circuit
        // evaluation, and have to temporarily store evaluated values, which is
        // bad for performance and memory usage respectively
        struct Item {
            row: Row,
            values: Vec<Value>,
        }

        let mut items = Vec::new();
        while let Some(row) = self.source.next().transpose()? {
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
                    Some(o) => return if *order == Direction::Ascending { o } else { o.reverse() },
                    None => {}
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(QueryIterator {
            columns: self.source.columns,
            rows: Box::new(items.into_iter().map(|i| Ok(i.row))),
        })
    }
}

/// A LIMIT executor
pub struct Limit {
    source: QueryIterator,
    limit: u64,
}

impl Limit {
    pub fn new(source: QueryIterator, limit: u64) -> Self {
        Self { source, limit }
    }

    pub fn execute(self) -> QueryIterator {
        QueryIterator {
            columns: self.source.columns,
            rows: Box::new(self.source.rows.take(self.limit as usize)),
        }
    }
}

/// An OFFSET executor
pub struct Offset {
    source: QueryIterator,
    offset: u64,
}

impl Offset {
    pub fn new(source: QueryIterator, offset: u64) -> Self {
        Self { source, offset }
    }

    pub fn execute(self) -> QueryIterator {
        QueryIterator {
            columns: self.source.columns,
            rows: Box::new(self.source.rows.skip(self.offset as usize)),
        }
    }
}
