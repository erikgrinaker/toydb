use std::cmp::Ordering;

use itertools::{izip, Itertools as _};

use crate::errinput;
use crate::error::Result;
use crate::sql::planner::Direction;
use crate::sql::types::{Expression, Rows, Value};

/// Filters the input rows (i.e. WHERE).
pub fn filter(source: Rows, predicate: Expression) -> Rows {
    Box::new(source.filter_map(move |r| {
        r.and_then(|row| match predicate.evaluate(Some(&row))? {
            Value::Boolean(true) => Ok(Some(row)),
            Value::Boolean(false) | Value::Null => Ok(None),
            value => errinput!("filter returned {value}, expected boolean",),
        })
        .transpose()
    }))
}

/// Limits the result to the given number of rows (i.e. LIMIT).
pub fn limit(source: Rows, limit: usize) -> Rows {
    Box::new(source.take(limit))
}

/// Skips the given number of rows (i.e. OFFSET).
pub fn offset(source: Rows, offset: usize) -> Rows {
    Box::new(source.skip(offset))
}

/// Sorts the rows (i.e. ORDER BY).
pub fn order(source: Rows, order: Vec<(Expression, Direction)>) -> Result<Rows> {
    // We can't use sort_by_cached_key(), since expression evaluation is
    // fallible, and since we may have to vary the sort direction of each
    // expression. Precompute the sort values instead, and map them based on
    // the row index.
    let mut irows: Vec<_> = source.enumerate().map(|(i, r)| r.map(|row| (i, row))).try_collect()?;
    let mut sort_values = Vec::with_capacity(irows.len());
    for (_, row) in &irows {
        let values: Vec<_> = order.iter().map(|(e, _)| e.evaluate(Some(row))).try_collect()?;
        sort_values.push(values)
    }

    irows.sort_by(|&(a, _), &(b, _)| {
        let dirs = order.iter().map(|(_, dir)| dir);
        for (a, b, dir) in izip!(&sort_values[a], &sort_values[b], dirs) {
            match a.cmp(b) {
                Ordering::Equal => {}
                order if *dir == Direction::Descending => return order.reverse(),
                order => return order,
            }
        }
        Ordering::Equal
    });

    Ok(Box::new(irows.into_iter().map(|(_, row)| Ok(row))))
}

/// Projects the rows using the given expressions (i.e. SELECT).
pub fn project(source: Rows, expressions: Vec<Expression>) -> Rows {
    Box::new(source.map(move |result| {
        result.and_then(|row| expressions.iter().map(|e| e.evaluate(Some(&row))).collect())
    }))
}

/// Remaps source columns to target column indexes, or drops them if None.
pub fn remap(source: Rows, targets: Vec<Option<usize>>) -> Rows {
    let size = targets.iter().filter_map(|v| *v).map(|i| i + 1).max().unwrap_or(0);
    Box::new(source.map_ok(move |row| {
        let mut out = vec![Value::Null; size];
        for (value, target) in row.into_iter().zip(&targets) {
            if let Some(index) = target {
                out[*index] = value;
            }
        }
        out
    }))
}
