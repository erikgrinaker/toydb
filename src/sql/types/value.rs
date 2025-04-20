use std::borrow::Cow;
use std::cmp::{Eq, Ordering, PartialEq};
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::result::Result as StdResult;

use dyn_clone::DynClone;
use serde::{Deserialize, Serialize, Serializer};

use crate::encoding;
use crate::error::{Error, Result};
use crate::sql::parser::ast;
use crate::{errdata, errinput};

/// A primitive SQL data type. For simplicity, only a handful of scalar types
/// are supported (no compound types).
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

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Boolean => write!(f, "BOOLEAN"),
            Self::Integer => write!(f, "INTEGER"),
            Self::Float => write!(f, "FLOAT"),
            Self::String => write!(f, "STRING"),
        }
    }
}

/// A primitive SQL value, represented as a native Rust type.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// An unknown value of unknown type (i.e. SQL NULL).
    ///
    /// In code, Null is considered equal to Null, so that we can detect, index,
    /// and order these values. The SQL NULL semantics are implemented during
    /// Expression evaluation.
    Null,
    /// A boolean.
    Boolean(bool),
    /// A 64-bit signed integer.
    Integer(i64),
    /// A 64-bit floating point number.
    ///
    /// In code, NaN is considered equal to NaN, so that we can detect, index,
    /// and order these values. The SQL NAN semantics are implemented during
    /// Expression evaluation.
    ///
    /// -0.0 and -NaN are considered equal to their positive counterpart, and
    /// normalized as positive when serialized (for key lookups).
    Float(#[serde(serialize_with = "serialize_f64")] f64),
    /// A UTF-8 encoded string.
    String(String),
}

impl encoding::Value for Value {}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Boolean(true) => f.write_str("TRUE"),
            Self::Boolean(false) => f.write_str("FALSE"),
            Self::Integer(integer) => integer.fmt(f),
            Self::Float(float) => write!(f, "{float:?}"),
            Self::String(string) => write!(f, "'{}'", string.escape_debug()),
        }
    }
}

/// Serialize f64 -0.0 and -NaN as positive, such that they're considered equal
/// in the key/value store (e.g. for index lookups).
fn serialize_f64<S: Serializer>(value: &f64, serializer: S) -> StdResult<S::Ok, S::Error> {
    let mut value = *value;
    if (value.is_nan() || value == 0.0) && value.is_sign_negative() {
        value = -value;
    }
    serializer.serialize_f64(value)
}

// Consider Nulls and ±NaNs equal. Rust already considers -0.0 == 0.0.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (Self::Integer(a), Self::Integer(b)) => a == b,
            (Self::Integer(a), Self::Float(b)) => *a as f64 == *b,
            (Self::Float(a), Self::Integer(b)) => *a == *b as f64,
            (Self::Float(a), Self::Float(b)) => a == b || a.is_nan() && b.is_nan(),
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Null, Self::Null) => true,
            (_, _) => false,
        }
    }
}

impl Eq for Value {}

// Allow hashing Nulls and floats, and hash -0.0 and -NaN as positive.
impl Hash for Value {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        core::mem::discriminant(self).hash(hasher); // hash the type
        match self {
            Self::Null => {}
            Self::Boolean(v) => v.hash(hasher),
            Self::Integer(v) => v.hash(hasher),
            Self::Float(v) => {
                // Hash -NaN and -0.0 as positive.
                let mut v = *v;
                if (v.is_nan() || v == 0.0) && v.is_sign_negative() {
                    v = -v;
                }
                v.to_bits().hash(hasher)
            }
            Self::String(v) => v.hash(hasher),
        }
    }
}

// Consider Nulls and NaNs equal when ordering.
//
// We establish a total order across all types, even though mixed types will
// rarely/never come up: String > Integer/Float > Boolean > Null.
impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Boolean(a), Self::Boolean(b)) => a.cmp(b),
            (Self::Integer(a), Self::Integer(b)) => a.cmp(b),
            (Self::Integer(a), Self::Float(b)) => (*a as f64).total_cmp(b),
            (Self::Float(a), Self::Integer(b)) => a.total_cmp(&(*b as f64)),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(b),
            (Self::String(a), Self::String(b)) => a.cmp(b),

            (Self::Null, _) => Ordering::Less,
            (_, Self::Null) => Ordering::Greater,
            (Self::Boolean(_), _) => Ordering::Less,
            (_, Self::Boolean(_)) => Ordering::Greater,
            (Self::Float(_), _) => Ordering::Less,
            (_, Self::Float(_)) => Ordering::Greater,
            (Self::Integer(_), _) => Ordering::Less,
            (_, Self::Integer(_)) => Ordering::Greater,
            // String is ordered last.
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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

    /// Returns true if the value is undefined (NULL or NaN).
    pub fn is_undefined(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Float(f) if f.is_nan() => true,
            _ => false,
        }
    }

    /// Adds two values. Errors if invalid.
    pub fn checked_add(&self, other: &Self) -> Result<Self> {
        use Value::*;

        Ok(match (self, other) {
            (Integer(lhs), Integer(rhs)) => match lhs.checked_add(*rhs) {
                Some(i) => Integer(i),
                None => return errinput!("integer overflow"),
            },
            (Integer(lhs), Float(rhs)) => Float(*lhs as f64 + rhs),
            (Float(lhs), Integer(rhs)) => Float(lhs + *rhs as f64),
            (Float(lhs), Float(rhs)) => Float(lhs + rhs),
            (Null, Integer(_) | Float(_) | Null) => Null,
            (Integer(_) | Float(_), Null) => Null,
            (lhs, rhs) => return errinput!("can't add {lhs} and {rhs}"),
        })
    }

    /// Divides two values. Errors if invalid.
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

    /// Multiplies two values. Errors if invalid.
    pub fn checked_mul(&self, other: &Self) -> Result<Self> {
        use Value::*;

        Ok(match (self, other) {
            (Integer(lhs), Integer(rhs)) => match lhs.checked_mul(*rhs) {
                Some(i) => Integer(i),
                None => return errinput!("integer overflow"),
            },
            (Integer(lhs), Float(rhs)) => Float(*lhs as f64 * rhs),
            (Float(lhs), Integer(rhs)) => Float(lhs * *rhs as f64),
            (Float(lhs), Float(rhs)) => Float(lhs * rhs),
            (Null, Integer(_) | Float(_) | Null) => Null,
            (Integer(_) | Float(_), Null) => Null,
            (lhs, rhs) => return errinput!("can't multiply {lhs} and {rhs}"),
        })
    }

    /// Exponentiates two values. Errors if invalid.
    pub fn checked_pow(&self, other: &Self) -> Result<Self> {
        use Value::*;

        Ok(match (self, other) {
            (Integer(lhs), Integer(rhs)) if *rhs >= 0 => {
                let rhs = (*rhs).try_into().or_else(|_| errinput!("integer overflow"))?;
                match lhs.checked_pow(rhs) {
                    Some(i) => Integer(i),
                    None => return errinput!("integer overflow"),
                }
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

    /// Finds the remainder of two values. Errors if invalid.
    ///
    /// NB: uses the remainder, not modulo, like Postgres. This means that for
    /// negative values, the result has the sign of the dividend, rather than
    /// always returning a positive value.
    pub fn checked_rem(&self, other: &Self) -> Result<Self> {
        use Value::*;

        Ok(match (self, other) {
            (Integer(_), Integer(0)) => return errinput!("can't divide by zero"),
            (Integer(lhs), Integer(rhs)) => Integer(lhs % rhs),
            (Integer(lhs), Float(rhs)) => Float(*lhs as f64 % rhs),
            (Float(lhs), Integer(rhs)) => Float(lhs % *rhs as f64),
            (Float(lhs), Float(rhs)) => Float(lhs % rhs),
            (Integer(_) | Float(_) | Null, Null) => Null,
            (Null, Integer(_) | Float(_)) => Null,
            (lhs, rhs) => return errinput!("can't take remainder of {lhs} and {rhs}"),
        })
    }

    /// Subtracts two values. Errors if invalid.
    pub fn checked_sub(&self, other: &Self) -> Result<Self> {
        use Value::*;

        Ok(match (self, other) {
            (Integer(lhs), Integer(rhs)) => match lhs.checked_sub(*rhs) {
                Some(i) => Integer(i),
                None => return errinput!("integer overflow"),
            },
            (Integer(lhs), Float(rhs)) => Float(*lhs as f64 - rhs),
            (Float(lhs), Integer(rhs)) => Float(lhs - *rhs as f64),
            (Float(lhs), Float(rhs)) => Float(lhs - rhs),
            (Null, Integer(_) | Float(_) | Null) => Null,
            (Integer(_) | Float(_), Null) => Null,
            (lhs, rhs) => return errinput!("can't subtract {lhs} and {rhs}"),
        })
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
        let Value::Boolean(b) = value else {
            return errdata!("not a boolean: {value}");
        };
        Ok(b)
    }
}

impl TryFrom<Value> for f64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let Value::Float(f) = value else {
            return errdata!("not a float: {value}");
        };
        Ok(f)
    }
}

impl TryFrom<Value> for i64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let Value::Integer(i) = value else {
            return errdata!("not an integer: {value}");
        };
        Ok(i)
    }
}

impl TryFrom<Value> for String {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let Value::String(s) = value else {
            return errdata!("not a string: {value}");
        };
        Ok(s)
    }
}

impl<'a> From<&'a Value> for Cow<'a, Value> {
    fn from(v: &'a Value) -> Self {
        Cow::Borrowed(v)
    }
}

/// A row of values.
pub type Row = Vec<Value>;

/// A row iterator.
pub type Rows = Box<dyn RowIterator>;

/// A row iterator trait, which requires the iterator to be both clonable and
/// object-safe. Cloning allows resetting an iterator back to an initial state,
/// e.g. for nested loop joins. It uses a blanket implementation, and relies on
/// dyn_clone to allow cloning trait objects.
pub trait RowIterator: Iterator<Item = Result<Row>> + DynClone {}

dyn_clone::clone_trait_object!(RowIterator);

impl<I: Iterator<Item = Result<Row>> + DynClone> RowIterator for I {}

/// A column label, used in query results and plans.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Label {
    /// No label.
    None,
    /// An unqualified column name.
    Unqualified(String),
    /// A fully qualified table/column name.
    Qualified(String, String),
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, ""),
            Self::Unqualified(name) => write!(f, "{name}"),
            Self::Qualified(table, column) => write!(f, "{table}.{column}"),
        }
    }
}

impl Label {
    /// Formats the label as a short column header.
    pub fn as_header(&self) -> &str {
        match self {
            Self::Qualified(_, column) | Self::Unqualified(column) => column.as_str(),
            Self::None => "?",
        }
    }
}

impl From<Label> for ast::Expression {
    /// Builds an ast::Expression::Column for a label. Can't be None.
    fn from(label: Label) -> Self {
        match label {
            Label::Qualified(table, column) => ast::Expression::Column(Some(table), column),
            Label::Unqualified(column) => ast::Expression::Column(None, column),
            Label::None => panic!("can't convert None label to AST expression"),
        }
    }
}

impl From<Option<String>> for Label {
    fn from(name: Option<String>) -> Self {
        name.map(Label::Unqualified).unwrap_or(Label::None)
    }
}
