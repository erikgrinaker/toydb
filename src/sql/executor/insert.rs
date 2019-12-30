use super::super::engine::Transaction;
use super::super::types::expression::{Environment, Expressions};
use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

pub struct Insert;

impl Insert {
    pub fn execute<T: Transaction>(
        ctx: &mut Context<T>,
        table: &str,
        columns: Vec<String>,
        expressions: Vec<Expressions>,
    ) -> Result<Box<dyn Executor>, Error> {
        let table = ctx
            .txn
            .read_table(table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?;
        for exprs in expressions {
            let mut row = Row::new();
            for expr in exprs {
                row.push(expr.evaluate(&Environment::empty())?);
            }
            row = table.normalize_row(
                row,
                if !columns.is_empty() { Some(columns.clone()) } else { None },
            )?;
            ctx.txn.create(&table.name, row)?;
        }
        Ok(Box::new(Self))
    }
}

impl Executor for Insert {
    fn columns(&self) -> Vec<String> {
        Vec::new()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        Ok(None)
    }
}
