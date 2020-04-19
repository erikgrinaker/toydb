mod expression;
pub use expression::{Environment, Expression, Expressions};

use crate::Error;

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// A datatype
#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
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

/// A specific value of a data type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl std::cmp::Eq for Value {}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.datatype().hash(state);
        match self {
            Value::Null => self.hash(state),
            Value::Boolean(v) => v.hash(state),
            Value::Integer(v) => v.hash(state),
            Value::Float(v) => v.to_be_bytes().hash(state), // FIXME Is this sane?
            Value::String(v) => v.hash(state),
        }
    }
}

impl Value {
    /// Returns the value's datatype, or None for null values
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
    pub fn boolean(self) -> Result<bool, Error> {
        match self {
            Self::Boolean(b) => Ok(b),
            v => Err(Error::Value(format!("Not a boolean: {:?}", v))),
        }
    }

    /// Returns the inner float, or an error if not a float
    pub fn float(self) -> Result<f64, Error> {
        match self {
            Self::Float(f) => Ok(f),
            v => Err(Error::Value(format!("Not a float: {:?}", v))),
        }
    }

    /// Returns the inner integer, or an error if not an integer
    pub fn integer(self) -> Result<i64, Error> {
        match self {
            Self::Integer(i) => Ok(i),
            v => Err(Error::Value(format!("Not an integer: {:?}", v))),
        }
    }

    /// Returns the inner string, or an error if not a string
    pub fn string(self) -> Result<String, Error> {
        match self {
            Self::String(s) => Ok(s),
            v => Err(Error::Value(format!("Not a string: {:?}", v))),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Null, _) => Some(Ordering::Less),
            (_, Self::Null) => Some(Ordering::Greater),
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

/// A row of values
pub type Row = Vec<Value>;

/// A row iterator
pub type Rows = Box<dyn Iterator<Item = Result<Row, Error>> + Send>;

/// A column (in a result set, see schema::Column for table columns)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub relation: Option<String>,
    pub name: Option<String>,
}

/// A set of columns
pub type Columns = Vec<Column>;

/// A relation, i.e. combination of columns and rows - used for query results.
/// FIXME This should possibly have a name as well, and be used to qualify query fields.
#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Debug, PartialEq)]
pub struct Relation {
    pub columns: Columns,
    #[derivative(Debug = "ignore")]
    #[derivative(PartialEq = "ignore")]
    #[serde(skip)]
    pub rows: Option<Rows>,
}

impl Relation {
    /// Converts the relation into a single row, if any
    pub fn into_row(mut self) -> Result<Option<Row>, Error> {
        self.next().transpose()
    }

    /// Converts the relation into the first value of the first row, if any
    pub fn into_value(self) -> Result<Option<Value>, Error> {
        Ok(self.into_row()?.filter(|row| !row.is_empty()).map(|mut row| row.remove(0)))
    }
}

impl Iterator for Relation {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // Make sure iteration is aborted on the first error, otherwise callers
        // will keep calling next for as long as it keeps returning errors
        if let Some(ref mut rows) = self.rows {
            let result = rows.next();
            if let Some(Err(_)) = result {
                self.rows = None;
            }
            result
        } else {
            None
        }
    }
}
