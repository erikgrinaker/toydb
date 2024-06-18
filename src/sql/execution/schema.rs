use crate::error::Result;
use crate::sql::engine::Catalog;
use crate::sql::types::schema::Table;

// Creates a table (i.e. CREATE TABLE).
pub(super) fn create_table(catalog: &impl Catalog, schema: Table) -> Result<()> {
    catalog.create_table(schema)
}

/// Deletes a table (i.e. DROP TABLE). Returns true if the table existed.
pub(super) fn drop_table(catalog: &impl Catalog, table: &str, if_exists: bool) -> Result<bool> {
    catalog.drop_table(table, if_exists)
}
