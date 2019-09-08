use super::types::{DataType, Value};
use crate::Error;

/// A table
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Table {
    /// The table name
    pub name: String,
    /// The table columns
    pub columns: Vec<Column>,
    /// The index of the primary key column
    pub primary_key: usize,
}

impl Table {
    /// Generates an SQL DDL query for the table schema
    pub fn to_query(&self) -> String {
        let mut query = format!("CREATE TABLE {} (\n", self.name);
        for (i, column) in self.columns.iter().enumerate() {
            query += &format!("  {} {}", column.name, column.datatype);
            if i == self.primary_key {
                query += " PRIMARY KEY";
            }
            if !column.nullable {
                query += " NOT NULL";
            }
            if i < self.columns.len() - 1 {
                query += ",";
            }
            query += "\n";
        }
        query += ")";
        query
    }

    /// Validates the schema
    pub fn validate(&self) -> Result<(), Error> {
        if self.columns.is_empty() {
            return Err(Error::Value("Table has no columns".into()));
        }
        let pk = self
            .columns
            .get(self.primary_key)
            .ok_or_else(|| Error::Value("Primary key column does not exist".into()))?;
        if pk.nullable {
            return Err(Error::Value(format!("Primary key column {} cannot be nullable", pk.name)));
        }
        Ok(())
    }

    /// Validates a row
    pub fn validate_row(&self, row: &[Value]) -> Result<(), Error> {
        if row.len() != self.columns.len() {
            return Err(Error::Value(format!(
                "Invalid row size {} for table {}, expected {}",
                row.len(),
                self.name,
                self.columns.len()
            )));
        }
        for (column, value) in self.columns.iter().zip(row.iter()) {
            column.validate_value(value)?;
        }
        Ok(())
    }
}

/// A table column
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column datatype
    pub datatype: DataType,
    /// Whether the column allows null values
    pub nullable: bool,
}

impl Column {
    /// Validates a column value
    pub fn validate_value(&self, value: &Value) -> Result<(), Error> {
        match value.datatype() {
            None if self.nullable => Ok(()),
            None => Err(Error::Value(format!("NULL value not allowed for column {}", self.name))),
            Some(ref datatype) if datatype != &self.datatype => Err(Error::Value(format!(
                "Invalid datatype {} for {} column {}",
                datatype, self.datatype, self.name
            ))),
            _ => Ok(()),
        }
    }
}
