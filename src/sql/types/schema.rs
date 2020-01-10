use super::super::parser::lexer::Keyword;
use super::{DataType, Row, Value};
use crate::Error;

use regex::Regex;
use std::collections::HashMap;

lazy_static! {
    static ref RE_IDENT: Regex = Regex::new(r#"^\w[\w_]*$"#).unwrap();
}

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
        let mut sql = String::from("CREATE TABLE ");
        if RE_IDENT.is_match(&self.name) && Keyword::from_str(&self.name).is_none() {
            sql += &self.name;
        } else {
            sql += &format!("\"{}\"", self.name.replace("\"", "\"\""));
        }
        sql += &format!(
            " (\n{}\n)",
            self.columns
                .iter()
                .map(|c| format!("  {}", c.as_sql()))
                .collect::<Vec<String>>()
                .join(",\n")
        );
        sql
    }

    /// Fetches a column by name
    /// FIXME Should index these for performance
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    // Builds a row from a set of values, optionally with a set of column names, padding
    // it with default values as necessary.
    pub fn make_row(&self, values: Vec<Value>, columns: Option<&[String]>) -> Result<Row, Error> {
        if let Some(columns) = columns {
            if values.len() != columns.len() {
                return Err(Error::Value("Column and value counts do not match".into()));
            }
            let mut inputs = HashMap::new();
            for (c, v) in columns.iter().zip(values.into_iter()) {
                if self.get_column(c).is_none() {
                    return Err(Error::Value(format!(
                        "Unknown column {} in table {}",
                        c, self.name
                    )));
                }
                if inputs.insert(c.clone(), v).is_some() {
                    return Err(Error::Value(format!("Column {} given multiple times", c)));
                }
            }
            let mut row = Row::new();
            for column in self.columns.iter() {
                if let Some(value) = inputs.get(&column.name) {
                    row.push(value.clone())
                } else if let Some(value) = &column.default {
                    row.push(value.clone())
                } else {
                    return Err(Error::Value(format!("No value given for column {}", column.name)));
                }
            }
            Ok(row)
        } else {
            let mut row = Row::new();
            for (i, column) in self.columns.iter().enumerate() {
                if let Some(value) = values.get(i) {
                    row.push(value.clone())
                } else if let Some(value) = &column.default {
                    row.push(value.clone())
                } else {
                    return Err(Error::Value(format!("No value given for column {}", column.name)));
                }
            }
            Ok(row)
        }
    }

    /// Returns the primary key column of the table
    pub fn primary_key(&self) -> Result<&Column, Error> {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .ok_or_else(|| Error::Value("Primary key not found".into()))
    }

    /// Returns the set of tables references by this table
    pub fn references(&self) -> Vec<Column> {
        self.columns.iter().filter(|c| c.references.is_some()).cloned().collect()
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

    /// Returns outbound references from a row as a table/pk hash map
    // FIXME Should remove duplicates, for performance
    pub fn row_references(&self, row: &[Value]) -> Result<HashMap<String, Vec<Value>>, Error> {
        let mut refs = HashMap::new();
        for (i, column) in self.columns.iter().enumerate() {
            if let Some(target) = &column.references {
                match row.get(i).cloned() {
                    Some(Value::Null) => {}
                    Some(Value::Float(f)) if f.is_nan() => {}
                    Some(v) => refs.entry(target.clone()).or_insert_with(Vec::new).push(v),
                    None => {
                        return Err(Error::Value(format!(
                            "No value found for column {}",
                            column.name
                        )))
                    }
                }
            }
        }
        Ok(refs)
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
        let mut sql = String::new();
        if RE_IDENT.is_match(&self.name) && Keyword::from_str(&self.name).is_none() {
            sql += &self.name;
        } else {
            sql += &format!("\"{}\"", self.name.replace("\"", "\"\""));
        }
        sql += &format!(" {}", self.datatype);
        if self.primary_key {
            sql += " PRIMARY KEY";
        }
        if !self.nullable && !self.primary_key {
            sql += " NOT NULL";
        }
        if let Some(default) = &self.default {
            sql += &format!(" DEFAULT {}", default);
        }
        if self.unique && !self.primary_key {
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
        if self.primary_key && !self.unique {
            return Err(Error::Value(format!("Primary key {} must be unique", self.name)));
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
