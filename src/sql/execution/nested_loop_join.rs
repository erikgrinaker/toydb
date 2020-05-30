use super::super::engine::Transaction;
use super::super::types::{Columns, Expression, Rows};
use super::{Executor, ResultSet, Row, Value};
use crate::error::{Error, Result};

/// A nested loop join executor
/// FIXME This code is horrible, clean it up at some point
pub struct NestedLoopJoin<T: Transaction> {
    /// The outer source
    outer: Box<dyn Executor<T>>,
    /// The inner source
    inner: Box<dyn Executor<T>>,
    /// The join predicate
    predicate: Option<Expression>,
    /// Whether to pad missing inner items (i.e. for outer joins)
    pad_inner: bool,
    /// Whether to flip inputs (for right outer joins)
    flip: bool,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(
        outer: Box<dyn Executor<T>>,
        inner: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        pad_inner: bool,
        flip: bool,
    ) -> Box<Self> {
        Box::new(Self { outer, inner, predicate, pad_inner, flip })
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let (result, inner) = if self.flip {
            (self.inner.execute(txn)?, self.outer.execute(txn)?)
        } else {
            (self.outer.execute(txn)?, self.inner.execute(txn)?)
        };
        if let ResultSet::Query { columns, rows } = result {
            if let ResultSet::Query { columns: inner_columns, rows: inner_rows } = inner {
                let mut join_columns = Columns::new();
                if self.flip {
                    join_columns.extend(inner_columns);
                    join_columns.extend(columns);
                } else {
                    join_columns.extend(columns);
                    join_columns.extend(inner_columns);
                }

                return Ok(ResultSet::Query {
                    columns: join_columns.clone(),
                    rows: Box::new(NestedLoopRows::new(
                        join_columns,
                        rows,
                        inner_rows.collect::<Result<Vec<_>>>()?,
                        self.predicate,
                        self.pad_inner,
                        self.flip,
                    )),
                });
            }
        }
        Err(Error::Internal("Unexpected result set".into()))
    }
}

struct NestedLoopRows {
    columns: Columns,
    predicate: Option<Expression>,
    outer: Rows,
    outer_cur: Option<Result<Row>>,
    // FIXME inner should be Rows too, but requires impl Clone
    inner: Box<dyn Iterator<Item = Row> + Send>,
    inner_orig: Vec<Row>,
    inner_pad: bool,
    inner_emitted: bool,
    flipped: bool,
}

impl NestedLoopRows {
    fn new(
        columns: Columns,
        mut outer: Rows,
        inner: Vec<Row>,
        predicate: Option<Expression>,
        inner_pad: bool,
        flipped: bool,
    ) -> Self {
        Self {
            columns,
            predicate,
            outer_cur: outer.next(),
            outer,
            inner: Box::new(inner.clone().into_iter()),
            inner_orig: inner,
            inner_pad,
            inner_emitted: false,
            flipped,
        }
    }

    fn next_inner(&mut self) -> Result<Option<Row>> {
        let o = match self.outer_cur.clone() {
            Some(Ok(o)) => o,
            Some(Err(e)) => return Err(e),
            None => return Ok(None),
        };
        while let Some(i) = self.inner.next() {
            if let Some(predicate) = &self.predicate {
                let mut row = Vec::new();
                if self.flipped {
                    row.extend(i.clone());
                    row.extend(o.clone());
                } else {
                    row.extend(o.clone());
                    row.extend(i.clone());
                }
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
        while self.outer_cur.is_some() {
            if let Some(Err(e)) = &self.outer_cur {
                return Some(Err(e.clone()));
            }
            let mut i = match self.next_inner().transpose() {
                Some(Ok(i)) => {
                    self.inner_emitted = true;
                    i
                }
                Some(Err(e)) => return Some(Err(e)),
                None => {
                    self.inner = Box::new(self.inner_orig.clone().into_iter());
                    if self.inner_pad && !self.inner_emitted {
                        let mut row = self.outer_cur.clone().unwrap().unwrap();
                        while row.len() < self.columns.len() {
                            if self.flipped {
                                row.insert(0, Value::Null)
                            } else {
                                row.push(Value::Null)
                            }
                        }
                        self.outer_cur = self.outer.next();
                        return Some(Ok(row));
                    }
                    self.inner_emitted = false;
                    self.outer_cur = self.outer.next();
                    continue;
                }
            };
            let mut o = match self.outer_cur.clone() {
                Some(Ok(o)) => o,
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            };
            if self.flipped {
                i.extend(o);
                return Some(Ok(i));
            } else {
                o.extend(i);
                return Some(Ok(o));
            }
        }
        None
    }
}
