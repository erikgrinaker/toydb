use super::super::engine::Transaction;
use super::super::types::{Columns, Expression, Rows};
use super::{Executor, ResultSet, Row, Value};
use crate::error::{Error, Result};

/// A nested loop join executor
/// FIXME This code is horrible, clean it up at some point
pub struct NestedLoopJoin<T: Transaction> {
    /// The left source
    left: Box<dyn Executor<T>>,
    /// The right source
    right: Box<dyn Executor<T>>,
    /// The join predicate
    predicate: Option<Expression>,
    /// Whether to pad missing right items (for outer joins)
    pad: bool,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        pad: bool,
    ) -> Box<Self> {
        Box::new(Self { left, right, predicate, pad })
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let result = self.left.execute(txn)?;
        let right = self.right.execute(txn)?;
        if let ResultSet::Query { mut columns, rows } = result {
            if let ResultSet::Query { columns: right_columns, rows: right_rows } = right {
                columns.extend(right_columns);
                return Ok(ResultSet::Query {
                    rows: Box::new(NestedLoopRows::new(
                        columns.clone(),
                        rows,
                        right_rows.collect::<Result<Vec<_>>>()?,
                        self.predicate,
                        self.pad,
                    )),
                    columns,
                });
            }
        }
        Err(Error::Internal("Unexpected result set".into()))
    }
}

struct NestedLoopRows {
    columns: Columns,
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
        columns: Columns,
        mut left: Rows,
        right: Vec<Row>,
        predicate: Option<Expression>,
        right_pad: bool,
    ) -> Self {
        Self {
            columns,
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
        let o = match self.left_cur.clone() {
            Some(Ok(o)) => o,
            Some(Err(e)) => return Err(e),
            None => return Ok(None),
        };
        while let Some(i) = self.right.next() {
            if let Some(predicate) = &self.predicate {
                let mut row = Vec::new();
                row.extend(o.clone());
                row.extend(i.clone());
                match predicate.evaluate(Some(&row))? {
                    Value::Boolean(true) => return Ok(Some(i)),
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
                return Ok(Some(i));
            }
        }
        Ok(None)
    }
}

impl Iterator for NestedLoopRows {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.left_cur.is_some() {
            if let Some(Err(e)) = &self.left_cur {
                return Some(Err(e.clone()));
            }
            let i = match self.next_right().transpose() {
                Some(Ok(i)) => {
                    self.right_emitted = true;
                    i
                }
                Some(Err(e)) => return Some(Err(e)),
                None => {
                    self.right = Box::new(self.right_orig.clone().into_iter());
                    if self.right_pad && !self.right_emitted {
                        let mut row = self.left_cur.clone().unwrap().unwrap();
                        while row.len() < self.columns.len() {
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
            let mut o = match self.left_cur.clone() {
                Some(Ok(o)) => o,
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            };
            o.extend(i);
            return Some(Ok(o));
        }
        None
    }
}
