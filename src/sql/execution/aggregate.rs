use crate::error::Result;
use crate::sql::planner::Aggregate;
use crate::sql::types::{Expression, Row, Rows, Value};

use itertools::Itertools as _;
use std::collections::BTreeMap;

/// Aggregates row values from the source according to the aggregates, using the
/// group_by expressions as buckets.
pub(super) fn aggregate(
    mut source: Rows,
    aggregates: Vec<Aggregate>,
    group_by: Vec<Expression>,
) -> Result<Rows> {
    let mut aggregator = Aggregator::new(aggregates, group_by);
    while let Some(row) = source.next().transpose()? {
        aggregator.add(row)?;
    }
    aggregator.into_rows()
}

/// Computes bucketed aggregates for rows.
struct Aggregator {
    /// Bucketed accumulators (by group_by values).
    buckets: BTreeMap<Vec<Value>, Vec<Accumulator>>,
    /// The set of empty accumulators.
    empty: Vec<Accumulator>,
    /// Expressions to accumulate. Indexes map to accumulators.
    exprs: Vec<Expression>,
    /// Group by expressions. Indexes map to bucket values.
    group_by: Vec<Expression>,
}

impl Aggregator {
    /// Creates a new aggregator for the given aggregates and GROUP BY buckets.
    fn new(aggregates: Vec<Aggregate>, group_by: Vec<Expression>) -> Self {
        use Aggregate::*;
        let accumulators = aggregates.iter().map(Accumulator::new).collect();
        let exprs = aggregates
            .into_iter()
            .map(|aggregate| match aggregate {
                Average(expr) | Count(expr) | Max(expr) | Min(expr) | Sum(expr) => expr,
            })
            .collect();
        Self { buckets: BTreeMap::new(), empty: accumulators, group_by, exprs }
    }

    /// Adds a row to the aggregator.
    fn add(&mut self, row: Row) -> Result<()> {
        // Compute the bucket value.
        let bucket: Vec<Value> =
            self.group_by.iter().map(|expr| expr.evaluate(Some(&row))).collect::<Result<_>>()?;

        // Compute and accumulate the input values.
        let accumulators = self.buckets.entry(bucket).or_insert_with(|| self.empty.clone());
        for (accumulator, expr) in accumulators.iter_mut().zip(&self.exprs) {
            let value = expr.evaluate(Some(&row))?;
            accumulator.add(value)?;
        }
        Ok(())
    }

    /// Returns a row iterator over the aggregate result.
    fn into_rows(self) -> Result<Rows> {
        // If there were no rows and no group_by expressions, return a row of
        // empty accumulators, e.g. SELECT COUNT(*) FROM t WHERE FALSE
        if self.buckets.is_empty() && self.group_by.is_empty() {
            let result = self.empty.into_iter().map(|acc| acc.value()).collect();
            return Ok(Box::new(std::iter::once(result)));
        }

        // Emit the aggregate and group_by values for each bucket. We use an
        // intermediate vec since btree_map::IntoIter doesn't implement Clone
        // (required by Rows).
        let buckets = self.buckets.into_iter().collect_vec();
        Ok(Box::new(buckets.into_iter().map(|(bucket, accumulators)| {
            accumulators
                .into_iter()
                .map(|acc| acc.value())
                .chain(bucket.into_iter().map(Ok))
                .collect()
        })))
    }
}

/// Accumulates aggregate values. Uses an enum rather than a trait since we need
/// to keep these in a vector (could use boxed trait objects too).
#[derive(Clone)]
enum Accumulator {
    Average { count: i64, sum: Value },
    Count(i64),
    Max(Option<Value>),
    Min(Option<Value>),
    Sum(Option<Value>),
}

impl Accumulator {
    /// Creates a new accumulator from an aggregate kind.
    fn new(aggregate: &Aggregate) -> Self {
        match aggregate {
            Aggregate::Average(_) => Self::Average { count: 0, sum: Value::Integer(0) },
            Aggregate::Count(_) => Self::Count(0),
            Aggregate::Max(_) => Self::Max(None),
            Aggregate::Min(_) => Self::Min(None),
            Aggregate::Sum(_) => Self::Sum(None),
        }
    }

    /// Adds a value to the accumulator.
    /// TODO: NULL values should possibly be ignored, not yield NULL (see Postgres?).
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
