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
        let table = ctx.txn.must_read_table(&table)?;
        let env = Environment::empty();
        for exprs in expressions {
            let mut row =
                exprs.into_iter().map(|e| e.evaluate(&env)).collect::<Result<_, Error>>()?;
            if columns.is_empty() {
                row = table.pad_row(row)?;
            } else {
                row = table.make_row(&columns, row)?;
            }
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
