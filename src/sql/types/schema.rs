use super::value::{DataType, Value};
use crate::encoding;
use crate::error::Result;
use crate::sql::engine::{Catalog, Transaction};
use crate::{errdata, errinput};

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// A table schema, which specifies the structure and constraints of its data.
///
/// Tables can't change after they are created. There is no ALTER TABLE nor
/// CREATE/DROP INDEX -- only CREATE TABLE and DROP TABLE.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Table {
    /// The table name. Can't be empty.
    pub name: String,
    /// The primary key column index. A table must have a primary key, and it
    /// can only be a single column.
    pub primary_key: usize,
    /// The table's columns. Must have at least one.
    pub columns: Vec<Column>,
}

impl encoding::Value for Table {}

/// A table column.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Column {
    /// Column name. Can't be empty.
    pub name: String,
    /// Column datatype.
    pub datatype: DataType,
    /// Whether the column allows null values. Not legal for primary keys.
    pub nullable: bool,
    /// The column's default value. If None, the user must specify an explicit
    /// value. Must match the column datatype. Nullable columns require a
    /// default (often Null), and Null is only a valid default when nullable.
    pub default: Option<Value>,
    /// Whether the column should only allow unique values. Must be true for a
    /// primary key column.
    pub unique: bool,
    /// Whether the column should have a secondary index. Never set for primary
    /// keys, which have an implicit primary index.
    pub index: bool,
    /// If set, this column is a foreign key reference to the given table's
    /// primary key. Must be of the same type as the target primary key.
    /// TODO: enforce an index for foreign keys.
    pub references: Option<String>,
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CREATE TABLE {} (", format_ident(&self.name))?;
        for (i, column) in self.columns.iter().enumerate() {
            write!(f, "  {} {}", format_ident(&column.name), column.datatype)?;
            if i == self.primary_key {
                write!(f, " PRIMARY KEY")?;
            } else if !column.nullable {
                write!(f, " NOT NULL")?;
            }
            if let Some(default) = &column.default {
                write!(f, " DEFAULT {default}")?;
            }
            if i != self.primary_key {
                if column.unique {
                    write!(f, " UNIQUE")?;
                }
                if column.index {
                    write!(f, " INDEX")?;
                }
            }
            if let Some(reference) = &column.references {
                write!(f, " REFERENCES {reference}")?;
            }
            if i < self.columns.len() - 1 {
                write!(f, ",")?;
            }
            writeln!(f)?;
        }
        write!(f, ")")
    }
}

impl Table {
    /// Fetches a column by name.
    ///
    /// TODO: consider getting rid of all these helpers.
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

    /// Returns the primary key value of a row.
    pub fn get_row_key<'a>(&self, row: &'a [Value]) -> Result<&'a Value> {
        row.get(self.primary_key).ok_or(errdata!("no primary key in row {row:?}"))
    }

    /// Validates the table schema.
    pub fn validate(&self, catalog: &impl Catalog) -> Result<()> {
        if self.name.is_empty() {
            return errinput!("table name can't be empty");
        }
        if self.columns.is_empty() {
            return errinput!("table has no columns");
        }
        if self.columns.get(self.primary_key).is_none() {
            return errinput!("invalid primary key index");
        }

        for (i, column) in self.columns.iter().enumerate() {
            if column.name.is_empty() {
                return errinput!("column name can't be empty");
            }
            let cname = &column.name; // for formatting convenience

            // Validate primary key.
            let is_primary_key = i == self.primary_key;
            if is_primary_key {
                if column.nullable {
                    return errinput!("primary key {cname} cannot be nullable");
                }
                if !column.unique {
                    return errinput!("primary key {cname} must be unique");
                }
                if column.index {
                    return errinput!("primary key {cname} can't have an index");
                }
            }

            // Validate default value.
            match &column.default {
                Some(Value::Null) if !column.nullable => {
                    return errinput!("invalid NULL default for non-nullable column {cname}")
                }
                Some(Value::Null) => {}
                Some(value) if value.datatype().as_ref() != Some(&column.datatype) => {
                    let (ctype, vtype) = (&column.datatype, value.datatype().unwrap());
                    return errinput!("invalid datatype {vtype} for {ctype} column {cname}",);
                }
                Some(_) => {}
                None if column.nullable => {
                    return errinput!("nullable column {cname} must have a default value")
                }
                None => {}
            }

            // Validate unique index.
            if column.unique && !column.index && !is_primary_key {
                return errinput!("unique column {cname} must have a secondary index");
            }

            // Validate references.
            if let Some(reference) = &column.references {
                if !column.index && !is_primary_key {
                    return errinput!("reference column {cname} must have a secondary index");
                }
                let target_type = if reference == &self.name {
                    self.columns[self.primary_key].datatype
                } else if let Some(target) = catalog.get_table(reference)? {
                    target.columns[target.primary_key].datatype
                } else {
                    return errinput!("unknown table {reference} referenced by column {cname}");
                };
                if column.datatype != target_type {
                    return errinput!(
                        "can't reference {target_type} primary key of {reference} from {} column {cname}",
                        column.datatype,
                    );
                }
            }
        }
        Ok(())
    }

    /// Validates a row.
    ///
    /// TODO: clean this up together with the Local engine. Who should be
    /// responsible for non-local validation (i.e. primary/unique conflicts and
    /// reference integrity)?
    pub fn validate_row(&self, row: &[Value], txn: &impl Transaction) -> Result<()> {
        if row.len() != self.columns.len() {
            return errinput!("invalid row size for table {}", self.name);
        }
        let pk = self.get_row_key(row)?;
        for (i, (column, value)) in self.columns.iter().zip(row.iter()).enumerate() {
            // Validate datatype.
            match value.datatype() {
                None if column.nullable => {}
                None => return errinput!("NULL value not allowed for column {}", column.name),
                Some(ref datatype) if datatype != &column.datatype => {
                    return errinput!(
                        "invalid datatype {} for {} column {}",
                        datatype,
                        column.datatype,
                        column.name
                    )
                }
                _ => {}
            }

            // Validate value
            match value {
                Value::String(s) if s.len() > 1024 => {
                    errinput!("strings cannot be more than 1024 bytes")
                }
                _ => Ok(()),
            }?;

            // Validate outgoing references
            if let Some(target) = &column.references {
                match value {
                    Value::Null => Ok(()),
                    Value::Float(f) if f.is_nan() => Ok(()),
                    v if target == &self.name && v == pk => Ok(()),
                    v if txn.get(target, &[v.clone()])?.is_empty() => {
                        errinput!("referenced primary key {v} in table {target} does not exist",)
                    }
                    _ => Ok(()),
                }?;
            }

            // Validate uniqueness constraints.
            // TODO: this needs an index lookup.
            if column.unique && i != self.primary_key && value != &Value::Null {
                let index = self.get_column_index(&column.name)?;
                let mut scan = txn.scan(&self.name, None)?;
                while let Some(row) = scan.next().transpose()? {
                    if row.get(index).unwrap_or(&Value::Null) == value
                        && self.get_row_key(&row)? != pk
                    {
                        return errinput!(
                            "unique value {value} already exists for column {}",
                            column.name
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

/// Formats an identifier as valid SQL, quoting it if necessary.
fn format_ident(ident: &str) -> Cow<str> {
    if crate::sql::parser::is_ident(ident) {
        return ident.into();
    }
    format!("\"{}\"", ident.replace('\"', "\"\"")).into()
}
