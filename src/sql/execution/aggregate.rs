use std::collections::BTreeMap;

use itertools::Itertools as _;

use crate::error::Result;
use crate::sql::planner::Aggregate;
use crate::sql::types::{Expression, Row, Rows, Value};

/// Aggregates row values from the source according to the aggregates, using the
/// group_by expressions as buckets. Emits rows with group_by buckets then
/// aggregates in the given order.
pub fn aggregate(
    mut source: Rows,
    group_by: Vec<Expression>,
    aggregates: Vec<Aggregate>,
) -> Result<Rows> {
    let mut aggregator = Aggregator::new(group_by, aggregates);
    while let Some(row) = source.next().transpose()? {
        aggregator.add(row)?;
    }
    aggregator.into_rows()
}

/// Computes bucketed aggregates for rows.
struct Aggregator {
    /// Bucketed accumulators (by group_by values).
    buckets: BTreeMap<Vec<Value>, Vec<Accumulator>>,
    /// The set of empty accumulators. Used to create new buckets.
    empty: Vec<Accumulator>,
    /// Group by expressions. Indexes map to bucket values.
    group_by: Vec<Expression>,
    /// Expressions to accumulate. Indexes map to accumulators.
    expressions: Vec<Expression>,
}

impl Aggregator {
    /// Creates a new aggregator for the given GROUP BY buckets and aggregates.
    fn new(group_by: Vec<Expression>, aggregates: Vec<Aggregate>) -> Self {
        use Aggregate::*;
        let accumulators = aggregates.iter().map(Accumulator::new).collect();
        let expressions = aggregates
            .into_iter()
            .map(|aggregate| match aggregate {
                Average(expr) | Count(expr) | Max(expr) | Min(expr) | Sum(expr) => expr,
            })
            .collect();
        Self { buckets: BTreeMap::new(), empty: accumulators, group_by, expressions }
    }

    /// Adds a row to the aggregator.
    fn add(&mut self, row: Row) -> Result<()> {
        // Compute the bucket value.
        let bucket: Vec<Value> =
            self.group_by.iter().map(|expr| expr.evaluate(Some(&row))).try_collect()?;

        // Compute and accumulate the input values.
        let accumulators = self.buckets.entry(bucket).or_insert_with(|| self.empty.clone());
        for (accumulator, expr) in accumulators.iter_mut().zip(&self.expressions) {
            accumulator.add(expr.evaluate(Some(&row))?)?;
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

        // Emit the group_by and aggregate values for each bucket. We use an
        // intermediate vec since btree_map::IntoIter doesn't implement Clone
        // (required by Rows).
        let buckets = self.buckets.into_iter().collect_vec();
        Ok(Box::new(buckets.into_iter().map(|(bucket, accumulators)| {
            bucket
                .into_iter()
                .map(Ok)
                .chain(accumulators.into_iter().map(|acc| acc.value()))
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
    fn add(&mut self, value: Value) -> Result<()> {
        // Aggregates ignore NULL values.
        if value == Value::Null {
            return Ok(());
        }
        match self {
            Self::Average { sum, count } => (*sum, *count) = (sum.checked_add(&value)?, *count + 1),
            Self::Count(count) => *count += 1,
            Self::Max(max @ None) => *max = Some(value),
            Self::Max(Some(max)) if value > *max => *max = value,
            Self::Max(Some(_)) => {}
            Self::Min(min @ None) => *min = Some(value),
            Self::Min(Some(min)) if value < *min => *min = value,
            Self::Min(Some(_)) => {}
            Self::Sum(sum @ None) => *sum = Some(Value::Integer(0).checked_add(&value)?),
            Self::Sum(Some(sum)) => *sum = sum.checked_add(&value)?,
        }
        Ok(())
    }

    /// Returns the aggregate value.
    fn value(self) -> Result<Value> {
        Ok(match self {
            Self::Average { count: 0, sum: _ } => Value::Null,
            Self::Average { count, sum } => sum.checked_div(&Value::Integer(count))?,
            Self::Count(count) => count.into(),
            Self::Max(Some(value)) | Self::Min(Some(value)) | Self::Sum(Some(value)) => value,
            Self::Max(None) | Self::Min(None) | Self::Sum(None) => Value::Null,
        })
    }
}
