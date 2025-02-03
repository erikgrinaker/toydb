use std::iter::Peekable;

use itertools::Itertools as _;
use std::collections::HashMap;

use crate::errinput;
use crate::error::Result;
use crate::sql::types::{Expression, Row, Rows, Value};

/// A nested loop join. Iterates over the right source for every row in the left
/// source, optionally filtering on the join predicate. If outer is true, and
/// there are no matches in the right source for a row in the left source, a
/// joined row with NULL values for the right source is returned (typically used
/// for a LEFT JOIN).
pub fn nested_loop(
    left: Rows,
    right: Rows,
    right_size: usize,
    predicate: Option<Expression>,
    outer: bool,
) -> Result<Rows> {
    Ok(Box::new(NestedLoopIterator::new(left, right, right_size, predicate, outer)?))
}

/// NestedLoopIterator implements nested loop joins.
///
/// This could be trivially implemented with carthesian_product(), but we need
/// to handle the left outer join case where there is no match in the right
/// source.
#[derive(Clone)]
struct NestedLoopIterator {
    /// The left source.
    left: Peekable<Rows>,
    /// The right source.
    right: Rows,
    /// The initial right iterator state. Cloned to reset right.
    right_init: Rows,
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
        let left = left.peekable();
        let right_init = right.clone();
        Ok(Self { left, right, right_init, right_size, right_match: false, predicate, outer })
    }

    // Returns the next joined row, if any.
    fn try_next(&mut self) -> Result<Option<Row>> {
        // While there is a valid left row, look for a right-hand match to return.
        while let Some(Ok(left_row)) = self.left.peek() {
            // If there is a match in the remaining right rows, return it.
            while let Some(right_row) = self.right.next().transpose()? {
                // We could avoid cloning here unless we're actually emitting
                // the row, but we keep it simple.
                let row = left_row.iter().cloned().chain(right_row).collect();
                let is_match = match &self.predicate {
                    Some(predicate) => match predicate.evaluate(Some(&row))? {
                        Value::Boolean(true) => true,
                        Value::Boolean(false) | Value::Null => false,
                        v => return errinput!("join predicate returned {v}, expected boolean"),
                    },
                    None => true,
                };
                if is_match {
                    self.right_match = true;
                    return Ok(Some(row));
                }
            }

            // We reached the end of the right source, reset it.
            self.right = self.right_init.clone();

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
pub fn hash(
    left: Rows,
    left_column: usize,
    right: Rows,
    right_column: usize,
    right_size: usize,
    outer: bool,
) -> Result<Rows> {
    // Build the hash table from the right source.
    let mut rows = right;
    let mut right: HashMap<Value, Vec<Row>> = HashMap::new();
    while let Some(row) = rows.next().transpose()? {
        let value = row[right_column].clone();
        if value.is_undefined() {
            continue; // NULL and NAN equality is always false
        }
        right.entry(value).or_default().push(row);
    }

    // Set up an iterator for an empty right row in the outer case.
    let empty = std::iter::repeat(Value::Null).take(right_size);

    // Set up the join iterator.
    let join = left.flat_map(move |result| -> Rows {
        // Pass through errors.
        let Ok(row) = result else {
            return Box::new(std::iter::once(result));
        };
        // Join the left row with any matching right rows.
        match right.get(&row[left_column]) {
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
    });
    Ok(Box::new(join))
}
