use super::super::engine::Transaction;
use super::{Context, Executor, ResultRows, ResultSet, Row};
use crate::Error;

/// A nested loop join executor
pub struct NestedLoopJoin<T: Transaction> {
    /// The outer source
    outer: Box<dyn Executor<T>>,
    /// The right source
    inner: Box<dyn Executor<T>>,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(outer: Box<dyn Executor<T>>, inner: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { outer, inner })
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin<T> {
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        let mut result = self.outer.execute(ctx)?;
        let inner = self.inner.execute(ctx)?;
        result.columns.extend(inner.columns);

        let mut inner_rows = vec![];
        if let Some(mut rows) = inner.rows {
            while let Some(row) = rows.next().transpose()? {
                inner_rows.push(row);
            }
        }

        if let Some(rows) = result.rows {
            result.rows = Some(Box::new(NestedLoopRows::new(rows, inner_rows).map(move |r| {
                r.and_then(|(mut a, b)| {
                    a.extend(b);
                    Ok(a)
                })
            })));
        }

        Ok(result)
    }
}

struct NestedLoopRows {
    outer: ResultRows,
    outer_cur: Option<Result<Row, Error>>,
    // FIXME inner should be ResultRows too, but requires impl Clone
    inner: Box<dyn Iterator<Item = Row> + Send>,
    inner_orig: Vec<Row>,
}

impl NestedLoopRows {
    fn new(mut outer: ResultRows, inner: Vec<Row>) -> Self {
        Self {
            outer_cur: outer.next(),
            outer,
            inner: Box::new(inner.clone().into_iter()),
            inner_orig: inner,
        }
    }
}

impl Iterator for NestedLoopRows {
    type Item = Result<(Row, Row), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = match self.inner.next() {
            Some(i) => i,
            None => {
                self.inner = Box::new(self.inner_orig.clone().into_iter());
                match self.inner.next() {
                    Some(i) => {
                        self.outer_cur = self.outer.next();
                        i
                    }
                    None => return None,
                }
            }
        };
        let o = match self.outer_cur.clone() {
            Some(Ok(o)) => o,
            Some(Err(e)) => return Some(Err(e)),
            None => return None,
        };
        Some(Ok((o, i)))
    }
}
