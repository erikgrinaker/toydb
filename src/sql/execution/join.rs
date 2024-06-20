use super::QueryIterator;
use crate::errdata;
use crate::error::Result;
use crate::sql::types::{Expression, Row, Rows, Value};

use itertools::Itertools as _;
use std::collections::HashMap;
use std::iter::Peekable;

/// A nested loop join. Iterates over the right source for every row in the left
/// source, optionally filtering on the join predicate. If outer is true, and
/// there are no matches in the right source for a row in the left source, a
/// joined row with NULL values for the right source is returned (typically used
/// for a LEFT JOIN).
pub(super) fn nested_loop(
    left: QueryIterator,
    right: QueryIterator,
    predicate: Option<Expression>,
    outer: bool,
) -> Result<QueryIterator> {
    let right_size = right.columns.len();
    let columns = left.columns.into_iter().chain(right.columns).collect();
    let rows =
        Box::new(NestedLoopIterator::new(left.rows, right.rows, right_size, predicate, outer)?);
    Ok(QueryIterator { rows, columns })
}

/// NestedLoopIterator implements nested loop joins.
///
/// This could be trivially implemented with carthesian_product(), but we need
/// to handle the left outer join case where there is no match in the right
/// source.
struct NestedLoopIterator {
    /// The left source.
    left: Peekable<Rows>,
    /// The right source.
    right: std::vec::IntoIter<Row>,
    /// The buffered right result.
    right_vec: Vec<Row>,
    /// The column width of the right source.
    right_size: usize,
    /// True if a right match has been seen for the current left row.
    right_match: bool,
    /// The join predicate.
    predicate: Option<Expression>,
    /// If true, emit a row when there is no match in the right source.
    outer: bool,
}

impl NestedLoopIterator {
    fn new(
        left: Rows,
        right: Rows,
        right_size: usize,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Result<Self> {
        // Collect the right source into a vector. We could use a borrowing
        // iterator into the vec instead of cloning an owned iterator, but it
        // comes with lifetime hassles and we end up cloning each row when
        // iterating anyway.
        //
        // TODO: consider making the iterators clonable.
        let right_vec = right.collect::<Result<Vec<_>>>()?;
        let right = right_vec.clone().into_iter();
        let left = left.peekable();
        Ok(Self { left, right, right_vec, right_size, right_match: false, predicate, outer })
    }

    // Returns the next joined row, if any, with error handling.
    fn try_next(&mut self) -> Result<Option<Row>> {
        // While there is a valid left row, look for a right-hand match to return.
        while let Some(Ok(left_row)) = self.left.peek() {
            // If there is a match in the remaining right rows, return it.
            for right_row in &mut self.right {
                // We could avoid cloning here unless we're actually emitting
                // the row, but we keep it simple.
                let row = left_row.iter().cloned().chain(right_row).collect();
                let is_match = match &self.predicate {
                    Some(predicate) => match predicate.evaluate(Some(&row))? {
                        Value::Boolean(true) => true,
                        Value::Boolean(false) | Value::Null => false,
                        v => return errdata!("join predicate returned {v}, expected boolean"),
                    },
                    None => true,
                };
                if is_match {
                    self.right_match = true;
                    return Ok(Some(row));
                }
            }

            // We reached the end of the right source, reset it.
            self.right = self.right_vec.clone().into_iter();

            // If there was no match for this row, and this is an outer join,
            // emit a row with right NULLs.
            if !self.right_match && self.outer {
                let row = left_row
                    .iter()
                    .cloned()
                    .chain(std::iter::repeat(Value::Null).take(self.right_size))
                    .collect();
                self.left.next();
                return Ok(Some(row));
            }

            // Move onto the next left row.
            self.left.next();
            self.right_match = false;
        }

        // Otherwise, there's either a None or Err in left. Return it.
        self.left.next().transpose()
    }
}

impl Iterator for NestedLoopIterator {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

/// Executes a hash join. This builds a hash table of rows from the right source
/// keyed on the join value, then iterates over the left source and looks up
/// matching rows in the hash table. If outer is true, and there is no match
/// in the right source for a row in the left source, a row with NULL values
/// for the right source is emitted instead.
///
/// TODO: add more tests for the multiple match case.
pub(super) fn hash(
    left: QueryIterator,
    left_field: usize,
    right: QueryIterator,
    right_field: usize,
    outer: bool,
) -> Result<QueryIterator> {
    // Build the combined columns.
    let right_size = right.columns.len();
    let columns = left.columns.into_iter().chain(right.columns).collect();

    // Build the hash table from the right source.
    let mut rows = right.rows;
    let mut right: HashMap<Value, Vec<Row>> = HashMap::new();
    while let Some(row) = rows.next().transpose()? {
        let id = row[right_field].clone();
        right.entry(id).or_default().push(row);
    }

    // Set up an iterator for an empty right row in the outer case.
    let empty = std::iter::repeat(Value::Null).take(right_size);

    // Set up the join iterator.
    let rows = Box::new(left.rows.flat_map(move |result| -> Rows {
        // Pass through errors.
        let Ok(row) = result else {
            return Box::new(std::iter::once(result));
        };
        // Join the left row with any matching right rows.
        match right.get(&row[left_field]) {
            Some(matches) => Box::new(
                std::iter::once(row)
                    .cartesian_product(matches.clone())
                    .map(|(l, r)| l.into_iter().chain(r).collect())
                    .map(Ok),
            ),
            None if outer => {
                Box::new(std::iter::once(Ok(row.into_iter().chain(empty.clone()).collect())))
            }
            None => Box::new(std::iter::empty()),
        }
    }));

    Ok(QueryIterator { columns, rows })
}
