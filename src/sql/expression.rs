use super::types::Value;
use crate::Error;
use std::collections::HashMap;

/// An expression
#[derive(Debug)]
pub enum Expression {
    // Values
    Constant(Value),
    Field(String),

    // Logical operations
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Or(Box<Expression>, Box<Expression>),

    // Comparisons
    CompareEQ(Box<Expression>, Box<Expression>),
    CompareGT(Box<Expression>, Box<Expression>),
    CompareGTE(Box<Expression>, Box<Expression>),
    CompareLT(Box<Expression>, Box<Expression>),
    CompareLTE(Box<Expression>, Box<Expression>),
    CompareNE(Box<Expression>, Box<Expression>),

    // Mathematical operations
    Add(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
}

pub type Expressions = Vec<Expression>;

pub struct Environment {
    fields: HashMap<String, Value>,
}

impl Environment {
    pub fn new(fields: HashMap<String, Value>) -> Self {
        Self { fields }
    }

    pub fn empty() -> Self {
        Self::new(HashMap::new())
    }
}

impl Expression {
    /// Evaluates an expression to a value
    pub fn evaluate(&self, e: &Environment) -> Result<Value, Error> {
        use Value::*;
        Ok(match self {
            // Values
            Self::Constant(c) => c.clone(),
            Self::Field(f) => match e.fields.get(f) {
                Some(v) => v.clone(),
                None => return Err(Error::Value(format!("Unknown field {}", f))),
            },

            // Logical operations
            Self::And(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs && rhs),
                (lhs, rhs) => return Err(Error::Value(format!("Can't and {} and {}", lhs, rhs))),
            },
            Self::Not(expr) => match expr.evaluate(e)? {
                Boolean(b) => Boolean(!b),
                value => return Err(Error::Value(format!("Can't negate {}", value))),
            },
            Self::Or(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs || rhs),
                (lhs, rhs) => return Err(Error::Value(format!("Can't or {} and {}", lhs, rhs))),
            },

            // Comparison operations
            #[allow(clippy::float_cmp)] // Up to the user if they want to compare or not
            Self::CompareEQ(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Integer(lhs), Integer(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 == rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs == rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs == rhs),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Self::CompareGT(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Integer(lhs), Integer(rhs)) => Boolean(lhs > rhs),
                (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 > rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs > rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs > rhs),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Self::CompareGTE(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Integer(lhs), Integer(rhs)) => Boolean(lhs >= rhs),
                (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 >= rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs >= rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs >= rhs),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Self::CompareLT(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Integer(lhs), Integer(rhs)) => Boolean(lhs < rhs),
                (Integer(lhs), Float(rhs)) => Boolean((lhs as f64) < rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs < rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs < rhs),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Self::CompareLTE(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Integer(lhs), Integer(rhs)) => Boolean(lhs <= rhs),
                (Integer(lhs), Float(rhs)) => Boolean((lhs as f64) <= rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs <= rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs <= rhs),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            #[allow(clippy::float_cmp)] // Up to the user if they want to compare or not
            Self::CompareNE(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Integer(lhs), Integer(rhs)) => Boolean(lhs != rhs),
                (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 != rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs != rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs != rhs),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },

            // Mathematical operations
            Self::Add(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Integer(lhs), Integer(rhs)) => Integer(lhs + rhs),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 + rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs + rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs + rhs),
                (lhs, rhs) => return Err(Error::Value(format!("Can't add {} and {}", lhs, rhs))),
            },
            Self::Divide(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Integer(lhs), Integer(rhs)) => Integer(lhs / rhs),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 / rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs / rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs / rhs),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't divide {} and {}", lhs, rhs)))
                }
            },
            Self::Exponentiate(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                // FIXME Handle overflow
                (Integer(lhs), Integer(rhs)) => Integer(lhs.pow(rhs as u32)),
                (Integer(lhs), Float(rhs)) => Float((lhs as f64).powi(rhs as i32)),
                (Float(lhs), Integer(rhs)) => Float((lhs).powi(rhs as i32)),
                (Float(lhs), Float(rhs)) => Float((lhs).powf(rhs)),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't exponentiate {} and {}", lhs, rhs)))
                }
            },
            Self::Factorial(expr) => match expr.evaluate(e)? {
                Integer(i) => Integer((1..=i).fold(1, |a, b| a * b as i64)),
                value => return Err(Error::Value(format!("Can't take factorial of {}", value))),
            },
            Self::Modulo(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                // The % operator in Rust is remainder, not modulo, so we have to do a bit of
                // acrobatics to make it work right
                (Integer(lhs), Integer(rhs)) => Integer(((lhs % rhs) + rhs) % rhs),
                (Integer(lhs), Float(rhs)) => Float(((lhs as f64 % rhs) + rhs) % rhs),
                (Float(lhs), Integer(rhs)) => Float(((lhs % rhs as f64) + rhs as f64) % rhs as f64),
                (Float(lhs), Float(rhs)) => Float(((lhs % rhs) + rhs) % rhs),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't take modulo of {} and {}", lhs, rhs)))
                }
            },
            Self::Multiply(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Integer(lhs), Integer(rhs)) => Integer(lhs * rhs),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 * rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs * rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs * rhs),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't multiply {} and {}", lhs, rhs)))
                }
            },
            Self::Negate(expr) => match expr.evaluate(e)? {
                Integer(i) => Integer(-i),
                Float(f) => Float(-f),
                value => return Err(Error::Value(format!("Can't negate {}", value))),
            },
            Self::Subtract(lhs, rhs) => match (lhs.evaluate(e)?, rhs.evaluate(e)?) {
                (Integer(lhs), Integer(rhs)) => Integer(lhs - rhs),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 - rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs - rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs - rhs),
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't subtract {} and {}", lhs, rhs)))
                }
            },
        })
    }
}
