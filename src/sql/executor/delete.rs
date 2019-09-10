use super::super::types::Row;
use super::{Context, Executor};
use crate::Error;

/// A delete executor
pub struct Delete;

impl Delete {
    pub fn execute(
        ctx: &mut Context,
        mut source: Box<dyn Executor>,
        table: String,
    ) -> Result<Box<dyn Executor>, Error> {
        let pk = ctx
            .storage
            .get_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?
            .primary_key;
        while let Some(row) = source.fetch()? {
            ctx.storage.delete_row(&table, row.get(pk).unwrap())?
        }
        Ok(Box::new(Self))
    }
}

impl Executor for Delete {
    fn columns(&self) -> Vec<String> {
        Vec::new()
    }

    fn fetch(&mut self) -> Result<Option<Row>, Error> {
        Ok(None)
    }
}
