use itertools::izip;

use super::QueryIterator;
use crate::errinput;
use crate::error::Result;
use crate::sql::planner::Direction;
use crate::sql::types::{Expression, Label, Value};

/// Filters the input rows (i.e. WHERE).
pub(super) fn filter(source: QueryIterator, predicate: Expression) -> QueryIterator {
    source.map_rows(|rows| {
        rows.filter_map(move |r| {
            r.and_then(|row| match predicate.evaluate(Some(&row))? {
                Value::Boolean(true) => Ok(Some(row)),
                Value::Boolean(false) => Ok(None),
                Value::Null => Ok(None),
                value => errinput!("filter returned {value}, expected boolean",),
            })
            .transpose()
        })
    })
}

/// Limits the result to the given number of rows (i.e. LIMIT).
pub(super) fn limit(source: QueryIterator, limit: u64) -> QueryIterator {
    source.map_rows(|rows| rows.take(limit as usize))
}

/// Skips the given number of rows (i.e. OFFSET).
pub(super) fn offset(source: QueryIterator, offset: u64) -> QueryIterator {
    source.map_rows(|rows| rows.skip(offset as usize))
}

/// Sorts the rows (i.e. ORDER BY).
pub(super) fn order(source: QueryIterator, order: Vec<(Expression, Direction)>) -> QueryIterator {
    source.try_map_rows(move |rows| {
        // We can't use sort_by_cached_key(), since expression evaluation is
        // fallible, and since we may have to vary the sort direction of each
        // expression. Precompute the sort values instead, and map them based on
        // the row index.
        let mut irows: Vec<_> =
            rows.enumerate().map(|(i, r)| r.map(|row| (i, row))).collect::<Result<_>>()?;

        let mut sort_values = Vec::with_capacity(irows.len());
        for (_, row) in &irows {
            let values: Vec<_> =
                order.iter().map(|(e, _)| e.evaluate(Some(row))).collect::<Result<_>>()?;
            sort_values.push(values)
        }

        irows.sort_by(|&(a, _), &(b, _)| {
            let dirs = order.iter().map(|(_, dir)| dir);
            for (a, b, dir) in izip!(&sort_values[a], &sort_values[b], dirs) {
                match a.cmp(b) {
                    std::cmp::Ordering::Equal => {}
                    order if *dir == Direction::Descending => return order.reverse(),
                    order => return order,
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(irows.into_iter().map(|(_, row)| Ok(row)))
    })
}

/// Projects the rows using the given expressions and labels (i.e. SELECT).
pub(super) fn project(
    source: QueryIterator,
    labels: Vec<Option<Label>>,
    expressions: Vec<Expression>,
) -> QueryIterator {
    // Use explicit column label if given, or pass through the source column
    // label if referenced (e.g. SELECT a, b, a FROM table).
    //
    // TODO: the labels can probably be resolved entirely during planning,
    // without having to plumb them through execution.
    source
        .map_columns(|columns| {
            labels
                .into_iter()
                .enumerate()
                .map(|(i, label)| {
                    if let Some(label) = label {
                        Some(label)
                    } else if let Expression::Field(f, _) = &expressions[i] {
                        columns.get(*f).cloned().expect("invalid field reference")
                    } else {
                        None
                    }
                })
                .collect()
        })
        .map_rows(|rows| {
            rows.map(move |r| {
                r.and_then(|row| expressions.iter().map(|e| e.evaluate(Some(&row))).collect())
            })
        })
}
