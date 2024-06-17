use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::types::schema::Table;

/// A CREATE TABLE executor
pub struct CreateTable {
    table: Table,
}

impl CreateTable {
    pub fn new(table: Table) -> Self {
        Self { table }
    }

    pub fn execute(self, txn: &mut impl Transaction) -> Result<()> {
        txn.create_table(self.table)
    }
}

/// A DROP TABLE executor
pub struct DropTable {
    table: String,
    if_exists: bool,
}

impl DropTable {
    pub fn new(table: String, if_exists: bool) -> Self {
        Self { table, if_exists }
    }

    pub fn execute(self, txn: &mut impl Transaction) -> Result<bool> {
        // TODO the planner should deal with this.
        if self.if_exists && txn.get_table(&self.table)?.is_none() {
            return Ok(false);
        }
        txn.drop_table(&self.table)?;
        Ok(true)
    }
}
