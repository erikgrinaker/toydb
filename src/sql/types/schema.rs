use super::{DataType, Value};
use crate::encoding;
use crate::errinput;
use crate::error::{Error, Result};
use crate::sql::engine::Transaction;
use crate::sql::parser::format_ident;

use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// A table schema
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl encoding::Value for Table {}

impl Table {
    /// Creates a new table schema
    pub fn new(name: String, columns: Vec<Column>) -> Result<Self> {
        let table = Self { name, columns };
        Ok(table)
    }

    /// Fetches a column by name
    pub fn get_column(&self, name: &str) -> Result<&Column> {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .ok_or(errinput!("column {name} not found in table {}", self.name))
    }

    /// Fetches a column index by name
    pub fn get_column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name == name)
            .ok_or(errinput!("column {name} not found in table {}", self.name))
    }

    /// Returns the primary key column of the table
    pub fn get_primary_key(&self) -> Result<&Column> {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .ok_or(errinput!("primary key not found in table {}", self.name))
    }

    /// Returns the primary key value of a row
    pub fn get_row_key(&self, row: &[Value]) -> Result<Value> {
        row.get(
            self.columns
                .iter()
                .position(|c| c.primary_key)
                .ok_or::<Error>(errinput!("primary key not found"))?,
        )
        .cloned()
        .ok_or(errinput!("primary key value not found for row"))
    }

    /// Validates the table schema
    pub fn validate(&self, txn: &mut dyn Transaction) -> Result<()> {
        if self.columns.is_empty() {
            return errinput!("table {} has no columns", self.name);
        }
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => return errinput!("no primary key in table {}", self.name),
            _ => return errinput!("multiple primary keys in table {}", self.name),
        };
        for column in &self.columns {
            column.validate(self, txn)?;
        }
        Ok(())
    }

    /// Validates a row
    pub fn validate_row(&self, row: &[Value], txn: &mut dyn Transaction) -> Result<()> {
        if row.len() != self.columns.len() {
            return errinput!("invalid row size for table {}", self.name);
        }
        let pk = self.get_row_key(row)?;
        for (column, value) in self.columns.iter().zip(row.iter()) {
            column.validate_value(self, &pk, value, txn)?;
        }
        Ok(())
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CREATE TABLE {} (\n{}\n)",
            format_ident(&self.name),
            self.columns.iter().map(|c| format!("  {}", c)).collect::<Vec<String>>().join(",\n")
        )
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
    /// Whether the column should be indexed
    pub index: bool,
}

impl Column {
    /// Validates the column schema
    pub fn validate(&self, table: &Table, txn: &mut dyn Transaction) -> Result<()> {
        // Validate primary key
        if self.primary_key && self.nullable {
            return errinput!("primary key {} cannot be nullable", self.name);
        }
        if self.primary_key && !self.unique {
            return errinput!("primary key {} must be unique", self.name);
        }

        // Validate default value
        if let Some(default) = &self.default {
            if let Some(datatype) = default.datatype() {
                if datatype != self.datatype {
                    return errinput!(
                        "default value for column {} has datatype {datatype}, must be {}",
                        self.name,
                        self.datatype
                    );
                }
            } else if !self.nullable {
                return errinput!(
                    "can't use NULL as default value for non-nullable column {}",
                    self.name
                );
            }
        } else if self.nullable {
            return errinput!("nullable column {} must have a default value", self.name);
        }

        // Validate references
        if let Some(reference) = &self.references {
            let target = if reference == &table.name {
                table.clone()
            } else if let Some(table) = txn.read_table(reference)? {
                table
            } else {
                return errinput!(
                    "table {reference} referenced by column {} does not exist",
                    self.name
                );
            };
            if self.datatype != target.get_primary_key()?.datatype {
                return errinput!(
                    "can't reference {} primary key of table {} from {} column {}",
                    target.get_primary_key()?.datatype,
                    target.name,
                    self.datatype,
                    self.name
                );
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
    ) -> Result<()> {
        // Validate datatype
        match value.datatype() {
            None if self.nullable => Ok(()),
            None => errinput!("NULL value not allowed for column {}", self.name),
            Some(ref datatype) if datatype != &self.datatype => errinput!(
                "invalid datatype {} for {} column {}",
                datatype,
                self.datatype,
                self.name
            ),
            _ => Ok(()),
        }?;

        // Validate value
        match value {
            Value::String(s) if s.len() > 1024 => {
                errinput!("strings cannot be more than 1024 bytes")
            }
            _ => Ok(()),
        }?;

        // Validate outgoing references
        if let Some(target) = &self.references {
            match value {
                Value::Null => Ok(()),
                Value::Float(f) if f.is_nan() => Ok(()),
                v if target == &table.name && v == pk => Ok(()),
                v if txn.read(target, v)?.is_none() => {
                    errinput!("referenced primary key {v} in table {target} does not exist",)
                }
                _ => Ok(()),
            }?;
        }

        // Validate uniqueness constraints
        if self.unique && !self.primary_key && value != &Value::Null {
            let index = table.get_column_index(&self.name)?;
            let mut scan = txn.scan(&table.name, None)?;
            while let Some(row) = scan.next().transpose()? {
                if row.get(index).unwrap_or(&Value::Null) == value
                    && &table.get_row_key(&row)? != pk
                {
                    return errinput!(
                        "unique value {value} already exists for column {}",
                        self.name
                    );
                }
            }
        }

        Ok(())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        if self.index {
            sql += " INDEX";
        }
        write!(f, "{}", sql)
    }
}
