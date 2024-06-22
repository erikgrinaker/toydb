use crate::error::Result;
use crate::sql::planner::Aggregate;
use crate::sql::types::{Row, Rows, Value};

use std::collections::HashMap;

/// Aggregates rows (i.e. GROUP BY).
pub(super) fn aggregate(
    mut source: Rows,
    aggregates: Vec<Aggregate>,
    group_by: usize,
) -> Result<Rows> {
    // Aggregate rows from source, grouped by non-aggregation columns. For
    // example, SELECT a, b, SUM(c), MAX(d) ... uses a,b as grouping buckets and
    // SUM(c),MAX(d) as accumulators for each a,b bucket.
    let mut accumulators: HashMap<Row, Vec<Accumulator>> = HashMap::new();
    while let Some(mut row) = source.next().transpose()? {
        accumulators
            .entry(row.split_off(aggregates.len()))
            .or_insert(aggregates.iter().map(Accumulator::new).collect())
            .iter_mut()
            .zip(row)
            .try_for_each(|(acc, value)| acc.add(value))?
    }

    // If there were no rows and no group-by columns, return a row of empty
    // accumulators, e.g.: SELECT COUNT(*) FROM t WHERE FALSE
    if accumulators.is_empty() && group_by == 0 {
        accumulators.insert(Vec::new(), aggregates.iter().map(Accumulator::new).collect());
    }

    // Emit the aggregate and row values for each row bucket.
    Ok(Box::new(accumulators.into_iter().map(|(row, accs)| {
        accs.into_iter()
            .map(|acc| acc.value())
            .chain(row.into_iter().map(Ok))
            .collect::<Result<_>>()
    })))
}

/// Accumulates aggregate values. Uses an enum rather than a trait since we need
/// to keep these in a vector (could use boxed trait objects too).
pub enum Accumulator {
    Average { count: i64, sum: Value },
    Count(i64),
    Max(Option<Value>),
    Min(Option<Value>),
    Sum(Option<Value>),
}

impl std::fmt::Display for Accumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let aggregate = match self {
            Self::Average { .. } => Aggregate::Average,
            Self::Count(_) => Aggregate::Count,
            Self::Max(_) => Aggregate::Max,
            Self::Min(_) => Aggregate::Min,
            Self::Sum(_) => Aggregate::Sum,
        };
        write!(f, "{aggregate}")
    }
}

impl Accumulator {
    /// Creates a new accumulator from an aggregate kind.
    fn new(aggregate: &Aggregate) -> Self {
        match aggregate {
            Aggregate::Average => Self::Average { count: 0, sum: Value::Integer(0) },
            Aggregate::Count => Self::Count(0),
            Aggregate::Max => Self::Max(None),
            Aggregate::Min => Self::Min(None),
            Aggregate::Sum => Self::Sum(None),
        }
    }

    /// Adds a value to the accumulator.
    fn add(&mut self, value: Value) -> Result<()> {
        use std::cmp::Ordering;
        match (self, value) {
            (Self::Average { sum: Value::Null, count: _ }, _) => {}
            (Self::Average { sum, count: _ }, Value::Null) => *sum = Value::Null,
            (Self::Average { sum, count }, value) => {
                *sum = sum.checked_add(&value)?;
                *count += 1;
            }

            (Self::Count(_), Value::Null) => {}
            (Self::Count(c), _) => *c += 1,

            (Self::Max(Some(Value::Null)), _) => {}
            (Self::Max(max), Value::Null) => *max = Some(Value::Null),
            (Self::Max(max @ None), value) => *max = Some(value),
            (Self::Max(Some(max)), value) => {
                if value.cmp(max) == Ordering::Greater {
                    *max = value
                }
            }

            (Self::Min(Some(Value::Null)), _) => {}
            (Self::Min(min), Value::Null) => *min = Some(Value::Null),
            (Self::Min(min @ None), value) => *min = Some(value),
            (Self::Min(Some(min)), value) => {
                if value.cmp(min) == Ordering::Less {
                    *min = value
                }
            }

            (Self::Sum(sum @ None), value) => *sum = Some(Value::Integer(0).checked_add(&value)?),
            (Self::Sum(Some(sum)), value) => *sum = sum.checked_add(&value)?,
        }
        Ok(())
    }

    /// Returns the aggregate value.
    fn value(self) -> Result<Value> {
        Ok(match self {
            Self::Average { count: 0, sum: _ } => Value::Null,
            Self::Average { count, sum } => sum.checked_div(&Value::Integer(count))?,
            Self::Count(c) => c.into(),
            Self::Max(None) | Self::Min(None) | Self::Sum(None) => Value::Null,
            Self::Max(Some(v)) | Self::Min(Some(v)) | Self::Sum(Some(v)) => v,
        })
    }
}
