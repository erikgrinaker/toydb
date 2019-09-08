use super::types::DataType;
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
            query += &format!(
                "  {} {}",
                column.name,
                match column.datatype {
                    DataType::Boolean => "BOOLEAN",
                    DataType::Float => "FLOAT",
                    DataType::Integer => "INTEGER",
                    DataType::String => "VARCHAR",
                }
            );
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
