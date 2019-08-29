use super::types::DataType;
use serde_derive::{Deserialize, Serialize};

/// A table
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: String,
}

/// A table column
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
}
