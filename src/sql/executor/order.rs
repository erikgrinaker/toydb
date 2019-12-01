use super::super::engine::Transaction;
use super::super::expression::{Environment, Expression};
use super::super::planner;
use super::super::types::{Row, Value};
use super::{Context, Executor};
use crate::Error;

/// An order executor
pub struct Order {
    columns: Vec<String>,
    stack: Vec<Row>,
}

impl Order {
    pub fn execute<T: Transaction>(
        _: &mut Context<T>,
        mut source: Box<dyn Executor>,
        orders: Vec<(Expression, planner::Order)>,
    ) -> Result<Box<dyn Executor>, Error> {
        let columns = source.columns();

        // FIXME Since we can't return errors from the sort_by closure, we have to
        // pre-evaluate all values. This means that we can't short-circuit evaluation,
        // and have to temporarily store evaluated values, which is bad for performance
        // and memory usage respectively.
        struct Item {
            row: Row,
            values: Vec<Value>,
        };
        let mut items = Vec::new();
        while let Some(row) = source.fetch()? {
            let env = Environment::new(columns.iter().cloned().zip(row.iter().cloned()).collect());
            let mut values = Vec::new();
            for (expr, _) in orders.iter() {
                values.push(expr.evaluate(&env)?);
            }
            items.push(Item { row, values });
        }

        items.sort_by(|a, b| {
            for (i, (_, order)) in orders.iter().enumerate() {
                let value_a = &a.values[i];
                let value_b = &b.values[i];
                match value_a.partial_cmp(value_b) {
                    Some(std::cmp::Ordering::Equal) => {}
                    Some(o) => {
                        return if *order == planner::Order::Ascending { o } else { o.reverse() }
                    }
                    None => {}
                }
            }
            std::cmp::Ordering::Equal
        });
        let mut rows: Vec<Row> = items.into_iter().map(|i| i.row).collect();
        rows.reverse(); // To make it a stack - should be optimized
        Ok(Box::new(Self { columns, stack: rows }))
    }
}

impl Executor for Order {
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        Ok(self.stack.pop())
    }
}
