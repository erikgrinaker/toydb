use crate::encoding;
use crate::errdata;
use crate::errinput;
use crate::error::{Error, Result};

use serde::{Deserialize, Serialize};

/// A primitive data type.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Serialize, Deserialize)]
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

// TODO: revisit and document the f64 handling here. FWIW, PostgreSQL considers
// NaN = NaN, maybe we should too.
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

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            // For ordering purposes, we consider e.g. NULL and NaN equal, and
            // establish a total order.
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Boolean(a), Self::Boolean(b)) => a.cmp(b),
            (Self::Integer(a), Self::Integer(b)) => a.cmp(b),
            (Self::Integer(a), Self::Float(b)) => (*a as f64).total_cmp(b),
            (Self::Float(a), Self::Integer(b)) => a.total_cmp(&(*b as f64)),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(b),
            (Self::String(a), Self::String(b)) => a.cmp(b),

            // Mixed types. Should rarely come up, but we may as well establish
            // an order, especially since we also implement Eq. We can handle
            // any special cases during expression evaluation.
            (Self::Null, _) => Ordering::Less,
            (_, Self::Null) => Ordering::Greater,
            (Self::Boolean(_), _) => Ordering::Less,
            (_, Self::Boolean(_)) => Ordering::Greater,
            (Self::Float(_), _) => Ordering::Less,
            (_, Self::Float(_)) => Ordering::Greater,
            (Self::Integer(_), _) => Ordering::Less,
            (_, Self::Integer(_)) => Ordering::Greater,
            #[allow(unreachable_patterns)]
            (Self::String(_), _) => Ordering::Less,
            #[allow(unreachable_patterns)]
            (_, Self::String(_)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Value {
    /// Adds two values. Errors when invalid.
    pub fn checked_add(&self, other: &Self) -> Result<Self> {
        use Value::*;
        Ok(match (self, other) {
            (Integer(lhs), Integer(rhs)) => {
                Integer(lhs.checked_add(*rhs).ok_or::<Error>(errinput!("integer overflow"))?)
            }
            (Integer(lhs), Float(rhs)) => Float(*lhs as f64 + rhs),
            (Float(lhs), Integer(rhs)) => Float(lhs + *rhs as f64),
            (Float(lhs), Float(rhs)) => Float(lhs + rhs),
            (Null, Integer(_) | Float(_) | Null) => Null,
            (Integer(_) | Float(_), Null) => Null,
            (lhs, rhs) => return errinput!("can't add {lhs} and {rhs}"),
        })
    }

    /// Divides two values. Errors when invalid.
    pub fn checked_div(&self, other: &Self) -> Result<Self> {
        use Value::*;
        Ok(match (self, other) {
            (Integer(_), Integer(0)) => return errinput!("can't divide by zero"),
            (Integer(lhs), Integer(rhs)) => Integer(lhs / rhs),
            (Integer(lhs), Float(rhs)) => Float(*lhs as f64 / rhs),
            (Float(lhs), Integer(rhs)) => Float(lhs / *rhs as f64),
            (Float(lhs), Float(rhs)) => Float(lhs / rhs),
            (Null, Integer(_) | Float(_) | Null) => Null,
            (Integer(_) | Float(_), Null) => Null,
            (lhs, rhs) => return errinput!("can't divide {lhs} and {rhs}"),
        })
    }

    /// Exponentiates two values. Errors when invalid.
    pub fn checked_pow(&self, other: &Self) -> Result<Self> {
        use Value::*;
        Ok(match (self, other) {
            (Integer(lhs), Integer(rhs)) if *rhs >= 0 => {
                let rhs: u32 = (*rhs).try_into().or(errinput!("integer overflow"))?;
                Integer(lhs.checked_pow(rhs).ok_or::<Error>(errinput!("integer overflow"))?)
            }
            (Integer(lhs), Integer(rhs)) => Float((*lhs as f64).powf(*rhs as f64)),
            (Integer(lhs), Float(rhs)) => Float((*lhs as f64).powf(*rhs)),
            (Float(lhs), Integer(rhs)) => Float((lhs).powi(*rhs as i32)),
            (Float(lhs), Float(rhs)) => Float((lhs).powf(*rhs)),
            (Integer(_) | Float(_), Null) => Null,
            (Null, Integer(_) | Float(_) | Null) => Null,
            (lhs, rhs) => return errinput!("can't exponentiate {lhs} and {rhs}"),
        })
    }

    /// Multiplies two values. Errors when invalid.
    pub fn checked_mul(&self, other: &Self) -> Result<Self> {
        use Value::*;
        Ok(match (self, other) {
            (Integer(lhs), Integer(rhs)) => {
                Integer(lhs.checked_mul(*rhs).ok_or::<Error>(errinput!("integer overflow"))?)
            }
            (Integer(lhs), Float(rhs)) => Float(*lhs as f64 * rhs),
            (Float(lhs), Integer(rhs)) => Float(lhs * *rhs as f64),
            (Float(lhs), Float(rhs)) => Float(lhs * rhs),
            (Null, Integer(_) | Float(_) | Null) => Null,
            (Integer(_) | Float(_), Null) => Null,
            (lhs, rhs) => return errinput!("can't multiply {lhs} and {rhs}"),
        })
    }

    /// Finds the remainder of two values. Errors when invalid.
    pub fn checked_rem(&self, other: &Self) -> Result<Self> {
        use Value::*;
        Ok(match (self, other) {
            // Uses remainder semantics, like Postgres.
            (Integer(_), Integer(0)) => return errinput!("can't divide by zero"),
            (Integer(lhs), Integer(rhs)) => Integer(lhs % rhs),
            (Integer(lhs), Float(rhs)) => Float(*lhs as f64 % rhs),
            (Float(lhs), Integer(rhs)) => Float(lhs % *rhs as f64),
            (Float(lhs), Float(rhs)) => Float(lhs % rhs),
            (Integer(_) | Float(_) | Null, Null) => Null,
            (Null, Integer(_) | Float(_)) => Null,
            (lhs, rhs) => return errinput!("can't take modulo of {lhs} and {rhs}"),
        })
    }

    /// Subtracts two values. Errors when invalid.
    pub fn checked_sub(&self, other: &Self) -> Result<Self> {
        use Value::*;
        Ok(match (self, other) {
            (Integer(lhs), Integer(rhs)) => {
                Integer(lhs.checked_sub(*rhs).ok_or::<Error>(errinput!("integer overflow"))?)
            }
            (Integer(lhs), Float(rhs)) => Float(*lhs as f64 - rhs),
            (Float(lhs), Integer(rhs)) => Float(lhs - *rhs as f64),
            (Float(lhs), Float(rhs)) => Float(lhs - rhs),
            (Null, Integer(_) | Float(_) | Null) => Null,
            (Integer(_) | Float(_), Null) => Null,
            (lhs, rhs) => return errinput!("can't subtract {lhs} and {rhs}"),
        })
    }

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

    /// Returns true if the value is unknown (NULL or NaN).
    pub fn is_unknown(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Float(f) if f.is_nan() => true,
            _ => false,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Boolean(true) => f.write_str("TRUE"),
            Self::Boolean(false) => f.write_str("FALSE"),
            Self::Integer(integer) => integer.fmt(f),
            Self::Float(float) => float.fmt(f),
            Self::String(string) => f.write_str(string),
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

impl TryFrom<Value> for bool {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let Value::Boolean(b) = value else { return errdata!("not a boolean: {value}") };
        Ok(b)
    }
}

impl TryFrom<Value> for f64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let Value::Float(f) = value else { return errdata!("not a float: {value}") };
        Ok(f)
    }
}

impl TryFrom<Value> for i64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let Value::Integer(i) = value else { return errdata!("not an integer: {value}") };
        Ok(i)
    }
}

impl TryFrom<Value> for String {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let Value::String(s) = value else { return errdata!("not a string: {value}") };
        Ok(s)
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
pub type Rows = Box<dyn Iterator<Item = Result<Row>>>;

/// A column label, used in result sets and query plans.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Label {
    /// No label.
    None,
    /// An unqualified column name.
    Unqualified(String),
    /// A fully qualified column name.
    Qualified(String, String),
}

impl std::fmt::Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, ""),
            Self::Unqualified(name) => write!(f, "{name}"),
            Self::Qualified(table, column) => write!(f, "{table}.{column}"),
        }
    }
}

impl Label {
    /// Creates an unqualified label for a Some.
    pub fn maybe_name(name: Option<String>) -> Self {
        name.map(Self::Unqualified).unwrap_or(Self::None)
    }

    /// Creates a qualified label if table is given, otherwise unqualified.
    pub fn maybe_qualified(table: Option<String>, column: String) -> Self {
        match table {
            Some(table) => Self::Qualified(table, column),
            None => Self::Unqualified(column),
        }
    }

    /// Formats the label as a short column header.
    pub fn as_header(&self) -> String {
        match self {
            Self::Qualified(_, column) | Self::Unqualified(column) => column.to_string(),
            Self::None => "?".to_string(),
        }
    }

    /// Returns true if the label is None.
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Returns true if the label is not None.
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }
}
