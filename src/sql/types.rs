use super::executor::Executor;
use crate::Error;

/// A datatype
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
            Self::String => "VARCHAR",
        })
    }
}

/// A value
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Value {
    /// An unknown value
    Null,
    /// A boolean
    Boolean(bool),
    /// A signed 64-bit integer
    Integer(i64),
    /// A signed 64-bit float
    Float(f64),
    /// A UTF-8 encoded string
    String(String),
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

/// A result set
pub struct ResultSet {
    executor: Option<Box<dyn Executor>>,
}

impl ResultSet {
    /// Creates a result set from an executor
    pub fn from_executor(executor: Box<dyn Executor>) -> Self {
        Self { executor: Some(executor) }
    }
}

impl Iterator for ResultSet {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // Make sure iteration is aborted on the first error, otherwise callers
        // may keep calling next for as long as it keeps returning errors
        if let Some(ref mut iter) = self.executor {
            match iter.next() {
                Some(Err(err)) => {
                    self.executor = None;
                    Some(Err(err))
                }
                r => r,
            }
        } else {
            None
        }
    }
}
