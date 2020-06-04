use super::super::engine::Transaction;
use super::super::types::{Expression, Rows};
use super::{Executor, ResultSet, Row, Value};
use crate::error::{Error, Result};

/// A nested loop join executor
/// FIXME This code is horrible, clean it up at some point
pub struct NestedLoopJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self { left, right, predicate, outer })
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Query { mut columns, rows } = self.left.execute(txn)? {
            if let ResultSet::Query { columns: rcolumns, rows: rrows } = self.right.execute(txn)? {
                columns.extend(rcolumns);
                return Ok(ResultSet::Query {
                    rows: Box::new(NestedLoopRows::new(
                        columns.len(),
                        rows,
                        rrows.collect::<Result<Vec<_>>>()?,
                        self.predicate,
                        self.outer,
                    )),
                    columns,
                });
            }
        }
        Err(Error::Internal("Unexpected result set".into()))
    }
}

struct NestedLoopRows {
    size: usize,
    predicate: Option<Expression>,
    left: Rows,
    left_cur: Option<Result<Row>>,
    // FIXME right should be Rows too, but requires impl Clone
    right: Box<dyn Iterator<Item = Row> + Send>,
    right_orig: Vec<Row>,
    right_pad: bool,
    right_emitted: bool,
}

impl NestedLoopRows {
    fn new(
        size: usize,
        mut left: Rows,
        right: Vec<Row>,
        predicate: Option<Expression>,
        right_pad: bool,
    ) -> Self {
        Self {
            size,
            predicate,
            left_cur: left.next(),
            left,
            right: Box::new(right.clone().into_iter()),
            right_orig: right,
            right_pad,
            right_emitted: false,
        }
    }

    fn next_right(&mut self) -> Result<Option<Row>> {
        let left_row = match self.left_cur.clone().transpose()? {
            Some(r) => r,
            None => return Ok(None),
        };
        while let Some(right_row) = self.right.next() {
            if let Some(predicate) = &self.predicate {
                let mut row = left_row.clone();
                row.extend(right_row.clone());
                match predicate.evaluate(Some(&row))? {
                    Value::Boolean(true) => return Ok(Some(right_row)),
                    Value::Boolean(false) => {}
                    Value::Null => {}
                    value => {
                        return Err(Error::Value(format!(
                            "Join predicate returned {}, expected boolean",
                            value
                        )))
                    }
                }
            } else {
                return Ok(Some(right_row));
            }
        }
        Ok(None)
    }
}

impl Iterator for NestedLoopRows {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok(mut row)) = self.left_cur.clone() {
            let right_row = match self.next_right().transpose() {
                Some(Ok(i)) => {
                    self.right_emitted = true;
                    i
                }
                Some(Err(e)) => return Some(Err(e)),
                None => {
                    self.right = Box::new(self.right_orig.clone().into_iter());
                    if self.right_pad && !self.right_emitted {
                        while row.len() < self.size {
                            row.push(Value::Null)
                        }
                        self.left_cur = self.left.next();
                        return Some(Ok(row));
                    }
                    self.right_emitted = false;
                    self.left_cur = self.left.next();
                    continue;
                }
            };
            row.extend(right_row);
            return Some(Ok(row));
        }
        self.left_cur.clone()
    }
}
