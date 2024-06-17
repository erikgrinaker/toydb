use super::QueryIterator;
use crate::errdata;
use crate::error::Result;
use crate::sql::types::{Expression, Row, Rows, Value};

use std::collections::HashMap;

/// A nested loop join. Iterates over the right result for every row in the left
/// result, optionally filtering on the join predicate.
///
/// TODO: revisit this and clean it up.
pub(super) fn nested_loop(
    left: QueryIterator,
    right: QueryIterator,
    predicate: Option<Expression>,
    outer: bool,
) -> Result<QueryIterator> {
    // TODO Since making the iterators or sources clonable is non-trivial (requiring
    // either avoiding Rust standard iterators or making sources generic), we simply
    // fetch the entire right result as a vector.
    let rows = Box::new(NestedLoopRows::new(
        left.rows,
        right.rows.collect::<Result<Vec<_>>>()?,
        right.columns.len(),
        predicate,
        outer,
    ));
    let mut columns = left.columns;
    columns.extend(right.columns);
    Ok(QueryIterator { rows, columns })
}

struct NestedLoopRows {
    left: Rows,
    left_row: Option<Result<Row>>,
    right: Box<dyn Iterator<Item = Row> + Send>,
    right_vec: Vec<Row>,
    right_empty: Vec<Value>,
    right_hit: bool,
    predicate: Option<Expression>,
    outer: bool,
}

impl NestedLoopRows {
    fn new(
        mut left: Rows,
        right: Vec<Row>,
        right_width: usize,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Self {
        Self {
            left_row: left.next(),
            left,
            right: Box::new(right.clone().into_iter()),
            right_vec: right,
            right_empty: std::iter::repeat(Value::Null).take(right_width).collect(),
            right_hit: false,
            predicate,
            outer,
        }
    }

    // Tries to get the next joined row, with error handling.
    fn try_next(&mut self) -> Result<Option<Row>> {
        // While there is a valid left row, look for a right-hand match to return.
        while let Some(Ok(left_row)) = self.left_row.clone() {
            // If there is a hit in the remaining right rows, return it.
            if let Some(row) = self.try_next_hit(&left_row)? {
                self.right_hit = true;
                return Ok(Some(row));
            }

            // Otherwise, continue with the next left row and reset the right source.
            self.left_row = self.left.next();
            self.right = Box::new(self.right_vec.clone().into_iter());

            // If this is an outer join, when we reach the end of the right items without a hit,
            // we should return a row with nulls for the right fields.
            if self.outer && !self.right_hit {
                let mut row = left_row;
                row.extend(self.right_empty.clone());
                return Ok(Some(row));
            }
            self.right_hit = false;
        }
        self.left_row.clone().transpose()
    }

    /// Tries to find the next combined row that matches the predicate in the remaining right rows.
    fn try_next_hit(&mut self, left_row: &[Value]) -> Result<Option<Row>> {
        for right_row in &mut self.right {
            let mut row = left_row.to_vec();
            row.extend(right_row);
            if let Some(predicate) = &self.predicate {
                match predicate.evaluate(Some(&row))? {
                    Value::Boolean(true) => return Ok(Some(row)),
                    Value::Boolean(false) => {}
                    Value::Null => {}
                    value => return errdata!("join predicate returned {value}, expected boolean"),
                }
            } else {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

impl Iterator for NestedLoopRows {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

/// Executes a hash join.
///
/// TODO: clean this up.
pub(super) fn hash(
    left: QueryIterator,
    left_field: usize,
    right: QueryIterator,
    right_field: usize,
    outer: bool,
) -> Result<QueryIterator> {
    let QueryIterator { mut columns, rows } = left;
    let QueryIterator { columns: rcolumns, rows: rrows } = right;
    let (l, r, outer) = (left_field, right_field, outer);
    let right: HashMap<Value, Row> = rrows
        .map(|res| match res {
            Ok(row) if row.len() <= r => errdata!("right index {r} out of bounds"),
            Ok(row) => Ok((row[r].clone(), row)),
            Err(err) => Err(err),
        })
        .collect::<Result<_>>()?;
    let empty = std::iter::repeat(Value::Null).take(rcolumns.len());
    columns.extend(rcolumns);
    let rows = Box::new(rows.filter_map(move |res| match res {
        Ok(row) if row.len() <= l => Some(errdata!("left index {l} out of bounds")),
        Ok(mut row) => match right.get(&row[l]) {
            Some(hit) => {
                row.extend(hit.clone());
                Some(Ok(row))
            }
            None if outer => {
                row.extend(empty.clone());
                Some(Ok(row))
            }
            None => None,
        },
        Err(err) => Some(Err(err)),
    }));
    Ok(QueryIterator { columns, rows })
}
