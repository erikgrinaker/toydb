use serde_derive::{Deserialize, Serialize};

/// A datatype
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

/// A value
#[derive(Clone, Debug, PartialEq, PartialOrd)]
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
                Value::Null => "NULL".to_string(),
                Value::Boolean(b) if *b => "TRUE".to_string(),
                Value::Boolean(_) => "FALSE".to_string(),
                Value::Integer(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::String(s) => s.clone(),
            }
            .as_ref(),
        )
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
