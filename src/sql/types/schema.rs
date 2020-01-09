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
}

impl Table {
    /// Creates a new table schema
    pub fn new(name: String, columns: Vec<Column>) -> Result<Self, Error> {
        let table = Self { name, columns };
        table.validate()?;
        Ok(table)
    }

    /// Generates an SQL DDL query for the table schema
    pub fn as_sql(&self) -> String {
        format!(
            "CREATE TABLE {} (\n{}\n)",
            self.name,
            self.columns
                .iter()
                .map(|c| format!("  {}", c.as_sql()))
                .collect::<Vec<String>>()
                .join(",\n")
        )
    }

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

    /// Returns a row from a hashmap keyed by column name, padding it with nulls if needed
    pub fn row_from_hashmap(&self, row: HashMap<String, Value>) -> Row {
        self.columns.iter().map(|c| row.get(&c.name).cloned().unwrap_or(Value::Null)).collect()
    }

    /// Returns the row as a hashmap keyed by column name, padding the row with nulls if needed
    pub fn row_to_hashmap(&self, row: Row) -> HashMap<String, Value> {
        self.columns
            .iter()
            .map(|c| c.name.clone())
            .zip(row.into_iter().chain(std::iter::repeat(Value::Null)))
            .collect()
    }

    /// Returns the primary key value of a row
    pub fn row_key(&self, row: &[Value]) -> Result<Value, Error> {
        // FIXME This should be indexed
        row.get(
            self.columns
                .iter()
                .position(|c| c.primary_key)
                .ok_or_else(|| Error::Value("Primary key not found".into()))?,
        )
        .cloned()
        .ok_or_else(|| Error::Value("Primary key value not found for row".into()))
    }

    /// Validates the table schema
    pub fn validate(&self) -> Result<(), Error> {
        if self.columns.is_empty() {
            return Err(Error::Value(format!("Table {} has no columns", self.name)));
        }
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => return Err(Error::Value(format!("No primary key in table {}", self.name))),
            _ => return Err(Error::Value(format!("Multiple primary keys in table {}", self.name))),
        };
        for column in &self.columns {
            column.validate()?;
        }
        Ok(())
    }

    /// Validates a row
    pub fn validate_row(&self, row: &[Value]) -> Result<(), Error> {
        if row.len() != self.columns.len() {
            return Err(Error::Value(format!("Invalid row size for table {}", self.name)));
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
    /// Whether the column is a primary key
    pub primary_key: bool,
    /// Whether the column allows null values
    pub nullable: bool,
    /// The default value of the column
    pub default: Option<Value>,
    /// Whether the column should only take unique values
    pub unique: bool,
    /// The table which is referenced by this foreign key
    pub references: Option<String>,
}

impl Column {
    /// Generates SQL DDL for the column
    pub fn as_sql(&self) -> String {
        let mut sql = format!("{} {}", self.name, self.datatype);
        if self.primary_key {
            sql += " PRIMARY KEY";
        }
        if !self.nullable {
            sql += " NOT NULL";
        }
        if let Some(default) = &self.default {
            sql += &format!(" DEFAULT {}", default);
        }
        if self.unique {
            sql += " UNIQUE";
        }
        if let Some(reference) = &self.references {
            sql += &format!(" REFERENCES {}", reference);
        }
        sql
    }

    /// Validates the column schema
    pub fn validate(&self) -> Result<(), Error> {
        if self.primary_key && self.nullable {
            return Err(Error::Value(format!("Primary key {} cannot be nullable", self.name)));
        }
        if let Some(default) = &self.default {
            if let Some(datatype) = default.datatype() {
                if datatype != self.datatype {
                    return Err(Error::Value(format!(
                        "Default value for column {} has datatype {}, must be {}",
                        self.name, datatype, self.datatype
                    )));
                }
            } else if !self.nullable {
                return Err(Error::Value(format!(
                    "Can't use NULL as default value for non-nullable column {}",
                    self.name
                )));
            }
        } else if self.nullable {
            return Err(Error::Value(format!(
                "Nullable column {} must have a default value",
                self.name
            )));
        }
        Ok(())
    }

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
