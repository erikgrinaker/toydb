/// A value
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
                Value::Boolean(b) => {
                    if *b {
                        "TRUE".to_string()
                    } else {
                        "FALSE".to_string()
                    }
                }
                Value::Integer(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::String(s) => s.clone(),
            }
            .as_ref(),
        )
    }
}

/// A row of values
pub type Row = Vec<Value>;
