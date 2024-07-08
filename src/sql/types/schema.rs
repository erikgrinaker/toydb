use super::{DataType, Value};
use crate::encoding;
use crate::errinput;
use crate::error::Result;
use crate::sql::engine::{Catalog, Transaction};

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// A table schema, which specifies the data structure and constraints.
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

    /// Validates a row, including uniqueness constraints and references.
    ///
    /// If update is true, the row replaces an existing entry with the same
    /// primary key. Otherwise, it is an insert. Primary key changes are
    /// implemented as a delete+insert.
    pub fn validate_row(&self, row: &[Value], update: bool, txn: &impl Transaction) -> Result<()> {
        if row.len() != self.columns.len() {
            return errinput!("invalid row size for table {}", self.name);
        }

        // Validate primary key.
        let id = &row[self.primary_key];
        let idslice = &row[self.primary_key..=self.primary_key];
        if id.is_undefined() {
            return errinput!("invalid primary key {id}");
        }
        if !update && !txn.get(&self.name, idslice)?.is_empty() {
            return errinput!("primary key {id} already exists");
        }

        for (i, (column, value)) in self.columns.iter().zip(row).enumerate() {
            let (cname, ctype) = (&column.name, &column.datatype);
            let valueslice = &row[i..=i];

            // Validate datatype.
            if let Some(ref vtype) = value.datatype() {
                if vtype != ctype {
                    return errinput!("invalid datatype {vtype} for {ctype} column {cname}");
                }
            }
            if value == &Value::Null && !column.nullable {
                return errinput!("NULL value not allowed for column {cname}");
            }

            // Validate outgoing references.
            if let Some(target) = &column.references {
                match value {
                    v if v.is_undefined() => {}
                    v if target == &self.name && v == id => {}
                    v if txn.get(target, valueslice)?.is_empty() => {
                        return errinput!("reference {v} not in table {target}");
                    }
                    _ => {}
                }
            }

            // Validate uniqueness constraints. Unique columns are indexed.
            if column.unique && i != self.primary_key && !value.is_undefined() {
                let mut index = txn.lookup_index(&self.name, &column.name, valueslice)?;
                if update {
                    index.remove(id); // ignore existing version of this row
                }
                if !index.is_empty() {
                    return errinput!("value {value} already in unique column {cname}");
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
