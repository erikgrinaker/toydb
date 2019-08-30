use super::types::DataType;
use serde_derive::{Deserialize, Serialize};

/// A table
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: String,
}

impl Table {
    pub fn get_primary_key_index(&self) -> usize {
        self.columns.iter().position(|c| c.name == self.primary_key).unwrap()
    }
}

/// A table column
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
}
