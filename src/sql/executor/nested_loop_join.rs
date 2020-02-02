use super::super::engine::Transaction;
use super::super::types::expression::Expression;
use super::{Context, Executor, ResultColumns, ResultRows, ResultSet, Row, Value};
use crate::Error;

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
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        let (mut result, inner) = if self.flip {
            (self.inner.execute(ctx)?, self.outer.execute(ctx)?)
        } else {
            (self.outer.execute(ctx)?, self.inner.execute(ctx)?)
        };
        let env_columns = result.columns.clone().merge(inner.columns.clone());
        result.columns = if self.flip {
            inner.columns.merge(result.columns)
        } else {
            result.columns.merge(inner.columns)
        };

        let mut inner_rows = vec![];
        if let Some(mut rows) = inner.rows {
            while let Some(row) = rows.next().transpose()? {
                inner_rows.push(row);
            }
        }

        if let Some(rows) = result.rows {
            result.rows = Some(Box::new(NestedLoopRows::new(
                env_columns,
                rows,
                inner_rows,
                self.predicate,
                self.pad_inner,
                self.flip,
            )));
        }

        Ok(result)
    }
}

struct NestedLoopRows {
    columns: ResultColumns,
    predicate: Option<Expression>,
    outer: ResultRows,
    outer_cur: Option<Result<Row, Error>>,
    // FIXME inner should be ResultRows too, but requires impl Clone
    inner: Box<dyn Iterator<Item = Row> + Send>,
    inner_orig: Vec<Row>,
    inner_pad: bool,
    inner_emitted: bool,
    flipped: bool,
}

impl NestedLoopRows {
    fn new(
        columns: ResultColumns,
        mut outer: ResultRows,
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

    fn next_inner(&mut self) -> Result<Option<Row>, Error> {
        let o = match self.outer_cur.clone() {
            Some(Ok(o)) => o,
            Some(Err(e)) => return Err(e),
            None => return Ok(None),
        };
        while let Some(i) = self.inner.next() {
            if let Some(predicate) = &self.predicate {
                let mut row = o.clone();
                row.extend(i.clone());
                let env = self.columns.as_env(&row);
                match predicate.evaluate(&env)? {
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
    type Item = Result<Row, Error>;

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
                        while row.len() < self.columns.columns.len() {
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
