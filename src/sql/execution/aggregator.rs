use std::collections::BTreeMap;

use itertools::Itertools as _;

use crate::error::Result;
use crate::sql::planner::Aggregate;
use crate::sql::types::{Expression, Row, Rows, Value};

/// Computes bucketed aggregates for input rows. For example, this query would
/// compute COUNT and SUM aggregates bucketed by category and brand:
///
/// SELECT COUNT(*), SUM(price) FROM products GROUP BY category, brand
pub struct Aggregator {
    /// GROUP BY expressions.
    group_by: Vec<Expression>,
    /// Aggregates to compute.
    aggregates: Vec<Aggregate>,
    /// Accumulators indexed by group_by bucket.
    buckets: BTreeMap<Vec<Value>, Vec<Accumulator>>,
}

impl Aggregator {
    /// Creates a new aggregator for the given GROUP BY buckets and aggregates.
    pub fn new(group_by: Vec<Expression>, aggregates: Vec<Aggregate>) -> Self {
        Self { group_by, aggregates, buckets: BTreeMap::new() }
    }

    /// Adds a row to the aggregator.
    pub fn add(&mut self, row: &Row) -> Result<()> {
        // Compute the bucket values.
        let bucket = self.group_by.iter().map(|expr| expr.evaluate(Some(row))).try_collect()?;

        // Look up the bucket accumulators, or create a new bucket.
        let accumulators = self
            .buckets
            .entry(bucket)
            .or_insert_with(|| self.aggregates.iter().map(Accumulator::new).collect())
            .iter_mut();

        // Collect expressions to evaluate.
        let exprs = self.aggregates.iter().map(|a| a.expr());

        // Accumulate the evaluated values.
        for (accumulator, expr) in accumulators.zip_eq(exprs) {
            accumulator.add(expr.evaluate(Some(row))?)?;
        }
        Ok(())
    }

    /// Adds rows to the aggregator.
    pub fn add_rows(&mut self, rows: Rows) -> Result<()> {
        for row in rows {
            self.add(&row?)?;
        }
        Ok(())
    }

    /// Returns a row iterator over the aggregate result.
    pub fn into_rows(self) -> Rows {
        // If there were no rows and no group_by expressions, return a row of
        // empty accumulators (e.g. SELECT COUNT(*) FROM t WHERE FALSE).
        if self.buckets.is_empty() && self.group_by.is_empty() {
            let result =
                self.aggregates.iter().map(Accumulator::new).map(|acc| acc.value()).try_collect();
            return Box::new(std::iter::once(result));
        }

        // Emit the group_by and aggregate values for each bucket. We use an
        // intermediate vec since btree_map::IntoIter doesn't implement Clone
        // (required by Rows).
        let buckets = self.buckets.into_iter().collect_vec();
        Box::new(buckets.into_iter().map(|(bucket, accumulators)| {
            bucket
                .into_iter()
                .map(Ok)
                .chain(accumulators.into_iter().map(|acc| acc.value()))
                .collect()
        }))
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
