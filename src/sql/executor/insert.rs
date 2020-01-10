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
        let env = Environment::empty();
        for exprs in expressions {
            ctx.txn.create(
                &table.name,
                table.make_row(
                    exprs.into_iter().map(|e| e.evaluate(&env)).collect::<Result<_, Error>>()?,
                    if !columns.is_empty() { Some(&columns) } else { None },
                )?,
            )?;
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
