use super::super::engine::Transaction;
use super::super::parser::format_ident;
use super::{DataType, Row, Value};
use crate::Error;

use std::collections::HashMap;

/// A table schema
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
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
        Ok(table)
    }

    /// Generates an SQL DDL query for the table schema
    pub fn as_sql(&self) -> String {
        format!(
            "CREATE TABLE {} (\n{}\n)",
            format_ident(&self.name),
            self.columns
                .iter()
                .map(|c| format!("  {}", c.as_sql()))
                .collect::<Vec<String>>()
                .join(",\n")
        )
    }

    /// Asserts that the table is not referenced by other tables, otherwise returns an error
    pub fn assert_unreferenced(&self, txn: &mut dyn Transaction) -> Result<(), Error> {
        for source in txn.scan_tables()?.filter(|t| t.name != self.name) {
            if let Some(column) =
                source.columns.iter().find(|c| c.references.as_ref() == Some(&self.name))
            {
                return Err(Error::Value(format!(
                    "Table {} is referenced by table {} column {}",
                    self.name, source.name, column.name
                )));
            }
        }
        Ok(())
    }

    /// Asserts that this primary key is not referenced from any other rows, otherwise errors
    pub fn assert_unreferenced_key(
        &self,
        pk: &Value,
        txn: &mut dyn Transaction,
    ) -> Result<(), Error> {
        for source in txn.scan_tables()? {
            let refs = source
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.references.as_deref() == Some(&self.name))
                .collect::<Vec<_>>();
            if refs.is_empty() {
                continue;
            }
            let mut scan = txn.scan(&source.name)?;
            while let Some(row) = scan.next().transpose()? {
                for (i, column) in refs.iter() {
                    if row.get(*i).unwrap_or(&Value::Null) == pk
                        && (source.name != self.name || &source.get_row_key(&row)? != pk)
                    {
                        return Err(Error::Value(format!(
                            "Primary key {} is referenced by table {} column {}",
                            pk, source.name, column.name
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Fetches a column by name
    pub fn get_column(&self, name: &str) -> Result<&Column, Error> {
        self.columns.iter().find(|c| c.name == name).ok_or_else(|| {
            Error::Value(format!("Column {} not found in table {}", name, self.name))
        })
    }

    /// Fetches a column index by name
    pub fn get_column_index(&self, name: &str) -> Result<usize, Error> {
        self.columns.iter().position(|c| c.name == name).ok_or_else(|| {
            Error::Value(format!("Column {} not found in table {}", name, self.name))
        })
    }

    /// Returns the primary key column of the table
    pub fn get_primary_key(&self) -> Result<&Column, Error> {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .ok_or_else(|| Error::Value(format!("Primary key not found in table {}", self.name)))
    }

    /// Returns the primary key value of a row
    pub fn get_row_key(&self, row: &[Value]) -> Result<Value, Error> {
        row.get(
            self.columns
                .iter()
                .position(|c| c.primary_key)
                .ok_or_else(|| Error::Value("Primary key not found".into()))?,
        )
        .cloned()
        .ok_or_else(|| Error::Value("Primary key value not found for row".into()))
    }

    // Builds a row from a set of values, optionally with a set of column names, padding
    // it with default values as necessary.
    pub fn make_row(&self, columns: &[String], values: Vec<Value>) -> Result<Row, Error> {
        if columns.len() != values.len() {
            return Err(Error::Value("Column and value counts do not match".into()));
        }
        let mut inputs = HashMap::new();
        for (c, v) in columns.iter().zip(values.into_iter()) {
            self.get_column(c)?;
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
    }

    /// Makes a hashmap for a row
    pub fn make_row_hashmap(&self, row: Row) -> HashMap<String, Value> {
        self.columns
            .iter()
            .map(|c| c.name.clone())
            .zip(row.into_iter().chain(std::iter::repeat(Value::Null)))
            .collect()
    }

    /// Pads a row with default values where possible
    pub fn pad_row(&self, mut row: Row) -> Result<Row, Error> {
        for column in self.columns.iter().skip(row.len()) {
            if let Some(default) = &column.default {
                row.push(default.clone())
            } else {
                return Err(Error::Value(format!("No default value for column {}", column.name)));
            }
        }
        Ok(row)
    }

    /// Sets a named row field to a value
    pub fn set_row_field(&self, row: &mut Row, field: &str, value: Value) -> Result<(), Error> {
        *row.get_mut(self.get_column_index(field)?)
            .ok_or_else(|| Error::Value(format!("Field {} not found in row", field)))? = value;
        Ok(())
    }

    /// Validates the table schema
    pub fn validate(&self, txn: &mut dyn Transaction) -> Result<(), Error> {
        if self.columns.is_empty() {
            return Err(Error::Value(format!("Table {} has no columns", self.name)));
        }
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => return Err(Error::Value(format!("No primary key in table {}", self.name))),
            _ => return Err(Error::Value(format!("Multiple primary keys in table {}", self.name))),
        };
        for column in &self.columns {
            column.validate(self, txn)?;
        }
        Ok(())
    }

    /// Validates a row
    pub fn validate_row(&self, row: &[Value], txn: &mut dyn Transaction) -> Result<(), Error> {
        if row.len() != self.columns.len() {
            return Err(Error::Value(format!("Invalid row size for table {}", self.name)));
        }
        let pk = self.get_row_key(row)?;
        for (column, value) in self.columns.iter().zip(row.iter()) {
            column.validate_value(self, &pk, value, txn)?;
        }
        Ok(())
    }
}

/// A table column schema
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
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
        let mut sql = format_ident(&self.name);
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
    pub fn validate(&self, table: &Table, txn: &mut dyn Transaction) -> Result<(), Error> {
        // Validate primary key
        if self.primary_key && self.nullable {
            return Err(Error::Value(format!("Primary key {} cannot be nullable", self.name)));
        }
        if self.primary_key && !self.unique {
            return Err(Error::Value(format!("Primary key {} must be unique", self.name)));
        }

        // Validate default value
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

        // Validate references
        if let Some(reference) = &self.references {
            let target = if reference == &table.name {
                table.clone()
            } else if let Some(table) = txn.read_table(reference)? {
                table
            } else {
                return Err(Error::Value(format!(
                    "Table {} referenced by column {} does not exist",
                    reference, self.name
                )));
            };
            if self.datatype != target.get_primary_key()?.datatype {
                return Err(Error::Value(format!(
                    "Can't reference {} primary key of table {} from {} column {}",
                    target.get_primary_key()?.datatype,
                    target.name,
                    self.datatype,
                    self.name
                )));
            }
        }

        Ok(())
    }

    /// Validates a column value
    pub fn validate_value(
        &self,
        table: &Table,
        pk: &Value,
        value: &Value,
        txn: &mut dyn Transaction,
    ) -> Result<(), Error> {
        // Validate datatype
        match value.datatype() {
            None if self.nullable => Ok(()),
            None => Err(Error::Value(format!("NULL value not allowed for column {}", self.name))),
            Some(ref datatype) if datatype != &self.datatype => Err(Error::Value(format!(
                "Invalid datatype {} for {} column {}",
                datatype, self.datatype, self.name
            ))),
            _ => Ok(()),
        }?;

        // Validate value
        match value {
            Value::String(s) if s.len() > 1024 => {
                Err(Error::Value("Strings cannot be more than 1024 bytes".into()))
            }
            _ => Ok(()),
        }?;

        // Validate outgoing references
        if let Some(target) = &self.references {
            match value {
                Value::Null => Ok(()),
                Value::Float(f) if f.is_nan() => Ok(()),
                v if target == &table.name && v == pk => Ok(()),
                v if txn.read(target, v)?.is_none() => Err(Error::Value(format!(
                    "Referenced primary key {} in table {} does not exist",
                    v, target,
                ))),
                _ => Ok(()),
            }?;
        }

        // Validate uniqueness constraints
        if self.unique && !self.primary_key && value != &Value::Null {
            let index = table.get_column_index(&self.name)?;
            let mut scan = txn.scan(&table.name)?;
            while let Some(row) = scan.next().transpose()? {
                if row.get(index).unwrap_or(&Value::Null) == value
                    && &table.get_row_key(&row)? != pk
                {
                    return Err(Error::Value(format!(
                        "Unique value {} already exists for column {}",
                        value, self.name
                    )));
                }
            }
        }

        Ok(())
    }
}
