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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
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

/// A row of values
pub type Row = Vec<Value>;

/// A result set
pub struct ResultSet {
    // FIXME Shouldn't be public
    pub txn_id: Option<u64>,
    columns: Vec<String>,
    executor: Option<Box<dyn Executor>>,
}

impl ResultSet {
    /// Creates an empty result set, mostly used for transaction commit and rollback
    pub fn empty() -> Self {
        Self { txn_id: None, columns: Vec::new(), executor: None }
    }

    /// Creates a result set for a transaction begin
    pub fn from_begin(txn_id: u64) -> Self {
        Self { txn_id: Some(txn_id), columns: Vec::new(), executor: None }
    }

    /// Creates a result set from an executor
    pub fn from_executor(executor: Box<dyn Executor>) -> Self {
        Self { txn_id: None, columns: executor.columns(), executor: Some(executor) }
    }

    /// Fetches the columns of the result set
    pub fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    /// Fetches the active transaction ID after the statement was executed
    /// (i.e. Some after BEGIN but None after COMMIT/ROLLBACK)
    pub fn txn_id(&self) -> Option<u64> {
        self.txn_id
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
