use crate::encoding;
use crate::errdata;
use crate::error::Result;

use serde::{Deserialize, Serialize};

/// A primitive data type.
#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    /// A boolean: true or false.
    Boolean,
    /// A 64-bit signed integer.
    Integer,
    /// A 64-bit floating point number.
    Float,
    /// A UTF-8 encoded string.
    String,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::Boolean => "BOOLEAN",
            Self::Integer => "INTEGER",
            Self::Float => "FLOAT",
            Self::String => "STRING",
        })
    }
}

/// A primitive value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// An unknown value of unknown type.
    Null,
    /// A boolean.
    Boolean(bool),
    /// A 64-bit signed integer.
    Integer(i64),
    /// A 64-bit floating point number.
    Float(f64),
    /// A UTF-8 encoded string.
    String(String),
}

impl encoding::Value for Value {}

// TODO: revisit and document the f64 handling here.
impl std::cmp::Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.datatype().hash(state);
        match self {
            Value::Null => self.hash(state),
            Value::Boolean(v) => v.hash(state),
            Value::Integer(v) => v.hash(state),
            Value::Float(v) => v.to_be_bytes().hash(state),
            Value::String(v) => v.hash(state),
        }
    }
}

impl Value {
    /// Returns the value's datatype, or None for null values.
    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }

    /// Returns the inner boolean, or an error if not a boolean
    /// TODO: this and below should implement TryFrom instead.
    pub fn boolean(self) -> Result<bool> {
        match self {
            Self::Boolean(b) => Ok(b),
            v => errdata!("not a boolean: {v:?}"),
        }
    }

    /// Returns the inner float, or an error if not a float
    pub fn float(self) -> Result<f64> {
        match self {
            Self::Float(f) => Ok(f),
            v => errdata!("not a float: {v:?}"),
        }
    }

    /// Returns the inner integer, or an error if not an integer
    pub fn integer(self) -> Result<i64> {
        match self {
            Self::Integer(i) => Ok(i),
            v => errdata!("not an integer: {v:?}"),
        }
    }

    /// Returns the inner string, or an error if not a string
    pub fn string(self) -> Result<String> {
        match self {
            Self::String(s) => Ok(s),
            v => errdata!("not a string: {v:?}"),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // TODO: don't allocate here.
        f.write_str(
            match self {
                Self::Null => "NULL".to_string(),
                Self::Boolean(b) if *b => "TRUE".to_string(),
                Self::Boolean(_) => "FALSE".to_string(),
                Self::Integer(i) => i.to_string(),
                Self::Float(f) => f.to_string(),
                Self::String(s) => s.clone(),
            }
            .as_ref(),
        )
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            // For ordering purposes, NULL is ordered first.
            //
            // TODO: revisit this and make sure the NULL handling is sound and
            // consistent with e.g. hash and eq handling.
            (Self::Null, Self::Null) => Some(std::cmp::Ordering::Equal),
            (Self::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Self::Null) => Some(std::cmp::Ordering::Greater),
            (Self::Boolean(a), Self::Boolean(b)) => a.partial_cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            (Self::Float(a), Self::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Self::Integer(a), Self::Float(b)) => (*a as f64).partial_cmp(b),
            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
            (Self::String(a), Self::String(b)) => a.partial_cmp(b),
            (_, _) => None,
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_owned())
    }
}

// TODO: reconsider use of Cow, it's unnecessary.
impl<'a> From<Value> for std::borrow::Cow<'a, Value> {
    fn from(v: Value) -> Self {
        std::borrow::Cow::Owned(v)
    }
}

impl<'a> From<&'a Value> for std::borrow::Cow<'a, Value> {
    fn from(v: &'a Value) -> Self {
        std::borrow::Cow::Borrowed(v)
    }
}

/// A row of values.
pub type Row = Vec<Value>;

/// A row iterator.
///
/// TODO: try to avoid boxing here.
pub type Rows = Box<dyn Iterator<Item = Result<Row>> + Send>;

/// A column (in a result set, see schema::Column for table columns).
///
/// TODO: revisit column handling in result sets.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: Option<String>,
}

/// A set of columns.
pub type Columns = Vec<Column>;
