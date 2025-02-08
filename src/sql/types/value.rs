use std::borrow::Cow;
use std::cmp::{Eq, Ordering, PartialEq};
use std::fmt::Display;
use std::hash::{Hash, Hasher};

use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};

use crate::encoding;
use crate::error::{Error, Result};
use crate::sql::parser::ast;
use crate::{errdata, errinput};

/// A primitive SQL value.
///
/// For simplicity, only a handful of representative scalar types are supported,
/// no compound types or more compact variants.
///
/// In SQL, neither Null nor floating point NaN are considered equal to
/// themselves (they are unknown values). However, in code, we consider them
/// equal and comparable. This is necessary to allow sorting and processing of
/// these values (e.g. in index lookups, aggregation buckets, etc.). SQL
/// expression evaluation have special handling of these values to produce the
/// desired NULL != NULL and NAN != NAN semantics in SQL queries.
///
/// Float -0.0 is considered equal to 0.0. It is normalized to 0.0 when stored.
/// Similarly, -NaN is normalized to NaN.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

// In code, consider Null and NaN equal, so that we can detect and process these
// values (e.g. in index lookups, aggregation groups, etc). SQL expressions
// handle them specially to provide their undefined value semantics.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Boolean(l), Self::Boolean(r)) => l == r,
            (Self::Integer(l), Self::Integer(r)) => l == r,
            (Self::Float(l), Self::Float(r)) => l == r || l.is_nan() && r.is_nan(),
            (Self::String(l), Self::String(r)) => l == r,
            (l, r) => core::mem::discriminant(l) == core::mem::discriminant(r),
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        // Normalize to treat +/-0.0 and +/-NAN as equal when hashing.
        match self.normalize_ref().as_ref() {
            Self::Null => {}
            Self::Boolean(v) => v.hash(state),
            Self::Integer(v) => v.hash(state),
            Self::Float(v) => v.to_bits().hash(state),
            Self::String(v) => v.hash(state),
        }
    }
}

// For ordering purposes, we consider NULL and NaN equal. We establish a total
// order across all types, even though mixed types will rarely/never come up.
impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        use Ordering::*;
        use Value::*;
        match (self, other) {
            (Null, Null) => Equal,
            (Boolean(a), Boolean(b)) => a.cmp(b),
            (Integer(a), Integer(b)) => a.cmp(b),
            (Integer(a), Float(b)) => (*a as f64).total_cmp(b),
            (Float(a), Integer(b)) => a.total_cmp(&(*b as f64)),
            (Float(a), Float(b)) => a.total_cmp(b),
            (String(a), String(b)) => a.cmp(b),

            (Null, _) => Less,
            (_, Null) => Greater,
            (Boolean(_), _) => Less,
            (_, Boolean(_)) => Greater,
            (Float(_), _) => Less,
            (_, Float(_)) => Greater,
            (Integer(_), _) => Less,
            (_, Integer(_)) => Greater,
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
    /// Adds two values. Errors when invalid.
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

    /// Multiplies two values. Errors when invalid.
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

    /// Exponentiates two values. Errors when invalid.
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

    /// Finds the remainder of two values. Errors when invalid.
    ///
    /// NB: uses the remainder, not modulo, like Postgres. For negative values,
    /// the result has the sign of the dividend, rather than always returning a
    /// positive value (modulo).
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

    /// Subtracts two values. Errors when invalid.
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
        *self == Self::Null || matches!(self, Self::Float(f) if f.is_nan())
    }

    /// Normalizes a value in place. Currently normalizes -0.0 and -NAN to 0.0
    /// and NAN respectively, which is the canonical value used e.g. in primary
    /// key and index lookups.
    pub fn normalize(&mut self) {
        if let Cow::Owned(normalized) = self.normalize_ref() {
            *self = normalized;
        }
    }

    /// Normalizes a borrowed value. Currently normalizes -0.0 and -NAN to 0.0
    /// and NAN respectively, which is the canonical value used e.g. in primary
    /// key and index lookups. Returns a Cow::Owned when changed, to avoid
    /// allocating in the common case where the value doesn't change.
    pub fn normalize_ref(&self) -> Cow<'_, Self> {
        if let Self::Float(f) = self {
            if (f.is_nan() || *f == -0.0) && f.is_sign_negative() {
                return Cow::Owned(Self::Float(-f));
            }
        }
        Cow::Borrowed(self)
    }

    // Returns true if the value is already normalized.
    pub fn is_normalized(&self) -> bool {
        matches!(self.normalize_ref(), Cow::Borrowed(_))
    }
}

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

/// A row of values.
pub type Row = Vec<Value>;

/// A row iterator.
pub type Rows = Box<dyn RowIterator>;

/// A row iterator trait, which requires the iterator to be both clonable and
/// object-safe. Cloning is needed to be able to reset an iterator back to an
/// initial state, e.g. during nested loop joins. It has a blanket
/// implementation for all matching iterators.
pub trait RowIterator: Iterator<Item = Result<Row>> + DynClone {}
impl<I: Iterator<Item = Result<Row>> + DynClone> RowIterator for I {}
dyn_clone::clone_trait_object!(RowIterator);

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
            Label::None => panic!("can't convert None label to AST expression"), // shouldn't happen
        }
    }
}

impl From<Option<String>> for Label {
    fn from(name: Option<String>) -> Self {
        name.map(Label::Unqualified).unwrap_or(Label::None)
    }
}
