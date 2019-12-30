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
        let schema = ctx
            .txn
            .read_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?;
        let pk_index = schema.primary_key;
        let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        while let Some(mut row) = source.fetch()? {
            let pk = row.get(pk_index).unwrap().clone();
            let env = Environment::new(columns.iter().cloned().zip(row.iter().cloned()).collect());
            for (c, expr) in &expressions {
                row[schema.column_index(&c).unwrap()] = expr.evaluate(&env)?;
            }
            ctx.txn.update(&table, &pk, row)?
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
