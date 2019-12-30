use super::{DataType, Row, Value};
use crate::Error;
use std::collections::HashMap;

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
    /// Returns the index of a named column, if it exists
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Normalizes a partial row into a full row, and reorders it
    /// according to the column names.
    pub fn normalize_row(&self, mut row: Row, columns: Option<Vec<String>>) -> Result<Row, Error> {
        if let Some(cols) = columns {
            if row.len() != cols.len() {
                return Err(Error::Value("Column and value counts do not match".into()));
            }
            let mut column_values = HashMap::new();
            for (c, v) in cols.into_iter().zip(row.into_iter()) {
                if self.column_index(&c).is_none() {
                    return Err(Error::Value(format!(
                        "Unknown column {} in table {}",
                        c, self.name
                    )));
                }
                if column_values.insert(c.clone(), v).is_some() {
                    return Err(Error::Value(format!("Column {} specified multiple times", c)));
                }
            }
            row = self
                .columns
                .iter()
                .map(|c| column_values.get(&c.name).cloned().unwrap_or(Value::Null))
                .collect();
        }
        while row.len() < self.columns.len() {
            row.push(Value::Null)
        }
        Ok(row)
    }

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
        }?;
        match value {
            Value::String(s) if s.len() > 1024 => {
                Err(Error::Value("Strings cannot be more than 1024 bytes".into()))
            }
            _ => Ok(()),
        }
    }
}
