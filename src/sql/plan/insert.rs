use super::super::expression::{Environment, Expressions};
use super::super::types::Row;
use super::{Context, Node};
use crate::Error;

/// An INSERT node
#[derive(Debug)]
pub struct Insert {
    table: String,
    columns: Vec<String>,
    expressions: Vec<Expressions>,
}

impl Insert {
    pub fn new(table: String, columns: Vec<String>, expressions: Vec<Expressions>) -> Self {
        Self { table, columns, expressions }
    }
}

impl Node for Insert {
    fn execute(&mut self, ctx: &mut Context) -> Result<(), Error> {
        let table = ctx
            .storage
            .get_table(&self.table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", self.table)))?;
        for exprs in &self.expressions {
            let mut row = Row::new();
            for expr in exprs {
                row.push(expr.evaluate(&Environment::empty())?);
            }
            row = table.normalize_row(
                row,
                if !self.columns.is_empty() { Some(self.columns.clone()) } else { None },
            )?;
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
