use super::super::engine::Transaction;
use super::super::plan::Aggregate;
use super::super::types::{Column, Value};
use super::{Executor, ResultSet};
use crate::error::{Error, Result};

use std::cmp::Ordering;
use std::collections::HashMap;

/// An aggregation executor
pub struct Aggregation<T: Transaction> {
    source: Box<dyn Executor<T>>,
    aggregates: Vec<Aggregate>,
    accumulators: HashMap<Vec<Value>, Vec<Box<dyn Accumulator>>>,
}

impl<T: Transaction> Aggregation<T> {
    pub fn new(source: Box<dyn Executor<T>>, aggregates: Vec<Aggregate>) -> Box<Self> {
        Box::new(Self { source, aggregates, accumulators: HashMap::new() })
    }
}

impl<T: Transaction> Executor<T> for Aggregation<T> {
    #[allow(clippy::or_fun_call)]
    fn execute(mut self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let agg_count = self.aggregates.len();
        match self.source.execute(txn)? {
            ResultSet::Query { columns, mut rows } => {
                while let Some(mut row) = rows.next().transpose()? {
                    self.accumulators
                        .entry(row.split_off(self.aggregates.len()))
                        .or_insert(self.aggregates.iter().map(<dyn Accumulator>::from).collect())
                        .iter_mut()
                        .zip(row)
                        .try_for_each(|(acc, value)| acc.accumulate(&value))?
                }
                // If there were no rows and no group-by columns, return a row of empty accumulators:
                // SELECT COUNT(*) FROM t WHERE FALSE
                if self.accumulators.is_empty() && self.aggregates.len() == columns.len() {
                    self.accumulators.insert(
                        Vec::new(),
                        self.aggregates.iter().map(<dyn Accumulator>::from).collect(),
                    );
                }
                Ok(ResultSet::Query {
                    columns: columns
                        .into_iter()
                        .enumerate()
                        .map(|(i, c)| if i < agg_count { Column { name: None } } else { c })
                        .collect(),
                    rows: Box::new(self.accumulators.into_iter().map(|(bucket, accs)| {
                        Ok(accs
                            .into_iter()
                            .map(|acc| acc.aggregate())
                            .chain(bucket.into_iter())
                            .collect())
                    })),
                })
            }
            r => Err(Error::Internal(format!("Unexpected result {:?}", r))),
        }
    }
}

// An accumulator
pub trait Accumulator: std::fmt::Debug + Send {
    // Accumulates a value
    fn accumulate(&mut self, value: &Value) -> Result<()>;

    // Calculates a final aggregate
    fn aggregate(&self) -> Value;
}

impl dyn Accumulator {
    fn from(aggregate: &Aggregate) -> Box<dyn Accumulator> {
        match aggregate {
            Aggregate::Average => Box::new(Average::new()),
            Aggregate::Count => Box::new(Count::new()),
            Aggregate::Max => Box::new(Max::new()),
            Aggregate::Min => Box::new(Min::new()),
            Aggregate::Sum => Box::new(Sum::new()),
        }
    }
}

// Count non-null values
#[derive(Debug)]
pub struct Count {
    count: u64,
}

impl Count {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for Count {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::Null => {}
            _ => self.count += 1,
        }
        Ok(())
    }

    fn aggregate(&self) -> Value {
        Value::Integer(self.count as i64)
    }
}

// Average value
#[derive(Debug)]
pub struct Average {
    count: Count,
    sum: Sum,
}

impl Average {
    pub fn new() -> Self {
        Self { count: Count::new(), sum: Sum::new() }
    }
}

impl Accumulator for Average {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        self.count.accumulate(value)?;
        self.sum.accumulate(value)?;
        Ok(())
    }

    fn aggregate(&self) -> Value {
        match (self.sum.aggregate(), self.count.aggregate()) {
            (Value::Integer(s), Value::Integer(c)) => Value::Integer(s / c),
            (Value::Float(s), Value::Integer(c)) => Value::Float(s / c as f64),
            _ => Value::Null,
        }
    }
}

// Maximum value
#[derive(Debug)]
pub struct Max {
    max: Option<Value>,
}

impl Max {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Accumulator for Max {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(max) = &mut self.max {
            match value.partial_cmp(max) {
                _ if max.datatype() != value.datatype() => *max = Value::Null,
                None => *max = Value::Null,
                Some(Ordering::Greater) => *max = value.clone(),
                Some(Ordering::Equal) | Some(Ordering::Less) => {}
            };
        } else {
            self.max = Some(value.clone())
        }
        Ok(())
    }

    fn aggregate(&self) -> Value {
        match &self.max {
            Some(value) => value.clone(),
            None => Value::Null,
        }
    }
}

// Minimum value
#[derive(Debug)]
pub struct Min {
    min: Option<Value>,
}

impl Min {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Accumulator for Min {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(min) = &mut self.min {
            match value.partial_cmp(min) {
                _ if min.datatype() != value.datatype() => *min = Value::Null,
                None => *min = Value::Null,
                Some(Ordering::Less) => *min = value.clone(),
                Some(Ordering::Equal) | Some(Ordering::Greater) => {}
            };
        } else {
            self.min = Some(value.clone())
        }
        Ok(())
    }

    fn aggregate(&self) -> Value {
        match &self.min {
            Some(value) => value.clone(),
            None => Value::Null,
        }
    }
}

// Sum of values
#[derive(Debug)]
pub struct Sum {
    sum: Option<Value>,
}

impl Sum {
    pub fn new() -> Self {
        Self { sum: None }
    }
}

impl Accumulator for Sum {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        self.sum = match (&self.sum, value) {
            (Some(Value::Integer(s)), Value::Integer(i)) => Some(Value::Integer(s + i)),
            (Some(Value::Float(s)), Value::Float(f)) => Some(Value::Float(s + f)),
            (None, Value::Integer(i)) => Some(Value::Integer(*i)),
            (None, Value::Float(f)) => Some(Value::Float(*f)),
            _ => Some(Value::Null),
        };
        Ok(())
    }

    fn aggregate(&self) -> Value {
        match &self.sum {
            Some(value) => value.clone(),
            None => Value::Null,
        }
    }
}
