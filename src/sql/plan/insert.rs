use super::super::expression::Expressions;
use super::super::types::Row;
use super::{Context, Node};
use crate::Error;

/// An INSERT node
#[derive(Debug)]
pub struct Insert {
    table: String,
    expressions: Vec<Expressions>,
}

impl Insert {
    pub fn new(table: String, expressions: Vec<Expressions>) -> Self {
        Self { table, expressions }
    }
}

impl Node for Insert {
    fn execute(&mut self, ctx: &mut Context) -> Result<(), Error> {
        for exprs in &self.expressions {
            let mut row = Row::new();
            for expr in exprs {
                row.push(expr.evaluate()?);
            }
            ctx.storage.create_row(&self.table, row)?;
        }
        Ok(())
    }
}

impl Iterator for Insert {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
