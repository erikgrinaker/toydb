use crate::error::Result;
use crate::sql::planner::Aggregate;
use crate::sql::types::{Row, Rows, Value};

use itertools::Itertools as _;
use std::collections::BTreeMap;

/// Aggregates row values from the source according to the aggregates, using the
/// group_by column values as buckets.
pub(super) fn aggregate(
    mut source: Rows,
    aggregates: Vec<(usize, Aggregate)>,
    group_by: Vec<usize>,
) -> Result<Rows> {
    // Keep a set of accumulators keyed by bucket.
    let mut accumulators: BTreeMap<Row, Vec<Accumulator>> = BTreeMap::new();
    while let Some(row) = source.next().transpose()? {
        let bucket = group_by.iter().map(|i| row[*i].clone()).collect();
        accumulators
            .entry(bucket)
            .or_insert_with(|| aggregates.iter().map(|(_, agg)| Accumulator::new(agg)).collect())
            .iter_mut()
            .enumerate()
            .map(|(i, agg)| (aggregates[i].0, agg))
            .try_for_each(|(i, acc)| acc.add(row[i].clone()))?
    }

    // If there were no rows and no group-by columns, return a row of empty
    // accumulators, e.g.: SELECT COUNT(*) FROM t WHERE FALSE
    if accumulators.is_empty() && group_by.is_empty() {
        accumulators
            .insert(Vec::new(), aggregates.iter().map(|(_, agg)| Accumulator::new(agg)).collect());
    }

    // Emit the aggregate and row values for each row bucket. We use an
    // intermediate vec since btree_map::IntoIter doesn't implement Clone.
    let accumulators = accumulators.into_iter().collect_vec();
    Ok(Box::new(accumulators.into_iter().map(|(row, accs)| {
        accs.into_iter()
            .map(|acc| acc.value())
            .chain(row.into_iter().map(Ok))
            .collect::<Result<_>>()
    })))
}

/// Accumulates aggregate values. Uses an enum rather than a trait since we need
/// to keep these in a vector (could use boxed trait objects too).
#[derive(Clone)]
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
    /// TODO: have this take &Value.
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
