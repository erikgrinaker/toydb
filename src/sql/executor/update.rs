use super::super::engine::Transaction;
use super::super::types::expression::{Environment, Expression};
use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;
use std::collections::BTreeMap;

/// An update executor
pub struct Update;

impl Update {
    pub fn execute<T: Transaction>(
        ctx: &mut Context<T>,
        table: String,
        mut source: Box<dyn Executor>,
        expressions: BTreeMap<String, Expression>,
    ) -> Result<Box<dyn Executor>, Error> {
        let table = ctx
            .txn
            .read_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?;
        while let Some(row) = source.fetch()? {
            let id = table.row_key(&row)?;
            let mut keyed_row = table.row_to_hashmap(row);
            let env = Environment::new(keyed_row.clone());
            for (c, expr) in &expressions {
                *keyed_row
                    .get_mut(c)
                    .ok_or_else(|| Error::Value(format!("Unknown column {}", c)))? =
                    expr.evaluate(&env)?;
            }
            ctx.txn.update(&table.name, &id, table.row_from_hashmap(keyed_row))?
        }
        Ok(Box::new(Self))
    }
}

impl Executor for Update {
    fn columns(&self) -> Vec<String> {
        Vec::new()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        Ok(None)
    }
}
