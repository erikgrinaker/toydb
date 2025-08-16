use std::collections::HashMap;
use std::iter::Peekable;

use crate::errinput;
use crate::error::Result;
use crate::sql::types::{Expression, Row, Rows, Value};

/// NestedLoopJoiner implements nested loop joins.
///
/// For every row in the left source, iterate over the right source and join
/// them. Rows are filtered on the join predicate, if given.
///
/// If outer is true, and there are no matches in the right source for a row in
/// the left source, a joined row with NULL values for the right source is
/// returned (typically used for a LEFT JOIN).
///
/// This could be trivially implemented with carthesian_product(), but we need
/// to handle the left outer join case where there is no match in the right
/// source.
#[derive(Clone)]
pub struct NestedLoopJoiner {
    /// The left source.
    left: Peekable<Rows>,
    /// The right source.
    right: Rows,
    /// The original right iterator state. Can be cloned to reset the
    /// right source to its original state.
    right_original: Rows,
    /// The number of columns in the right source.
    right_columns: usize,
    /// True if a right match has been seen for the current left row.
    right_matched: bool,
    /// The join predicate.
    predicate: Option<Expression>,
    /// If true, emit a row when there is no match in the right source.
    outer: bool,
}

impl NestedLoopJoiner {
    /// Creates a new nested loop joiner.
    pub fn new(
        left: Rows,
        right: Rows,
        right_columns: usize,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Self {
        let left = left.peekable();
        let right_original = right.clone();
        Self { left, right, right_original, right_columns, right_matched: false, predicate, outer }
    }

    // Returns the next joined row, if any.
    fn try_next(&mut self) -> Result<Option<Row>> {
        // While there is a valid left row, look for a right-hand match to return.
        while let Some(Ok(left)) = self.left.peek() {
            // If there is a match in the remaining right rows, return it.
            while let Some(right) = self.right.next().transpose()? {
                let row = left.iter().cloned().chain(right).collect();
                if let Some(predicate) = &self.predicate {
                    match predicate.evaluate(Some(&row))? {
                        Value::Boolean(true) => {}
                        Value::Boolean(false) | Value::Null => continue,
                        v => return errinput!("join predicate returned {v}, expected boolean"),
                    }
                }
                self.right_matched = true;
                return Ok(Some(row));
            }

            // If there was no right match for the left row, and this is an
            // outer join, emit a row with right NULLs.
            if !self.right_matched && self.outer {
                self.right_matched = true;
                return Ok(Some(
                    left.iter()
                        .cloned()
                        .chain(std::iter::repeat_n(Value::Null, self.right_columns))
                        .collect(),
                ));
            }

            // We reached the end of the right source. Reset it and move onto
            // the next left row.
            self.right = self.right_original.clone();
            self.right_matched = false;
            self.left.next().transpose()?;
        }

        // Otherwise, there's either a None or Err in left. Return it.
        self.left.next().transpose()
    }
}

impl Iterator for NestedLoopJoiner {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

/// HashJoiner implements hash joins.
///
/// This builds a hash table of rows from the right source keyed on the join
/// value, then iterates over the left source and looks up matching rows in the
/// hash table.
///
/// If outer is true, and there is no match in the right source for a row in the
/// left source, a row with NULL values for the right source is emitted instead.
#[derive(Clone)]
pub struct HashJoiner {
    /// The left source.
    left: Rows,
    /// The left column to join on.
    left_column: usize,
    /// The right hash map to join on.
    right: HashMap<Value, Vec<Row>>,
    /// The number of columns in the right source.
    right_columns: usize,
    /// If true, emit a row when there is no match in the right source.
    outer: bool,
    /// Any pending matches to emit.
    pending: Rows,
}

impl HashJoiner {
    /// Creates a new hash joiner.
    pub fn new(
        left: Rows,
        left_column: usize,
        mut right: Rows,
        right_column: usize,
        right_columns: usize,
        outer: bool,
    ) -> Result<Self> {
        // Build a hash map from the right source.
        let mut right_map: HashMap<Value, Vec<Row>> = HashMap::new();
        while let Some(row) = right.next().transpose()? {
            let value = row[right_column].clone();
            if value.is_undefined() {
                continue; // undefined will never match anything
            }
            right_map.entry(value).or_default().push(row);
        }

        let pending = Box::new(std::iter::empty());

        Ok(Self { left, left_column, right: right_map, right_columns, outer, pending })
    }

    // Returns the next joined row, if any.
    fn try_next(&mut self) -> Result<Option<Row>> {
        // If there's a pending row stashed from a previous call, return it.
        if let Some(row) = self.pending.next().transpose()? {
            return Ok(Some(row));
        }

        // Find the next left row to join with.
        while let Some(left) = self.left.next().transpose()? {
            if let Some(right) = self.right.get(&left[self.left_column]).cloned() {
                // Join with all right matches and stash them in pending.
                self.pending = Box::new(
                    right
                        .into_iter()
                        .map(move |right| left.iter().cloned().chain(right).collect())
                        .map(Ok),
                );
                return self.pending.next().transpose();
            } else if self.outer {
                // If there is no match for the left row, but it's an outer
                // join, emit a row with right NULLs.
                return Ok(Some(
                    left.into_iter()
                        .chain(std::iter::repeat_n(Value::Null, self.right_columns))
                        .collect(),
                ));
            }
        }

        Ok(None)
    }
}

impl Iterator for HashJoiner {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
