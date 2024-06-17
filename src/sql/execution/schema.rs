use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::types::schema::Table;

// Creates a table (i.e. CREATE TABLE).
pub(super) fn create_table(txn: &mut impl Transaction, schema: Table) -> Result<()> {
    txn.create_table(schema)
}

/// Deletes a table (i.e. DROP TABLE). Returns true if the table existed.
pub(super) fn drop_table(txn: &mut impl Transaction, table: &str, if_exists: bool) -> Result<bool> {
    // TODO the planner should deal with this.
    if if_exists && txn.get_table(table)?.is_none() {
        return Ok(false);
    }
    txn.drop_table(table)?;
    Ok(true)
}
