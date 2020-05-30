use super::{Row, Value};
use crate::error::{Error, Result};

use regex::Regex;
use serde_derive::{Deserialize, Serialize};
use std::mem::replace;

/// An expression, made up of constants and operations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    // Values
    Constant(Value),
    Field(usize, Option<(Option<String>, String)>),

    // Logical operations
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Or(Box<Expression>, Box<Expression>),

    // Comparisons operations (GTE, LTE, and NEQ are composite operations)
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),

    // Mathematical operations
    Add(Box<Expression>, Box<Expression>),
    Assert(Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),

    // String operations
    Like(Box<Expression>, Box<Expression>),
}

/// A list of expressions
pub type Expressions = Vec<Expression>;

impl Expression {
    /// Evaluates an expression to a value, given an environment
    pub fn evaluate(&self, row: Option<&Row>) -> Result<Value> {
        use Value::*;
        Ok(match self {
            // Constant values
            Self::Constant(c) => c.clone(),
            Self::Field(i, _) => row.and_then(|row| row.get(*i).cloned()).unwrap_or(Null),

            // Logical operations
            Self::And(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs && rhs),
                (Boolean(lhs), Null) if !lhs => Boolean(false),
                (Boolean(_), Null) => Null,
                (Null, Boolean(rhs)) if !rhs => Boolean(false),
                (Null, Boolean(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Can't and {} and {}", lhs, rhs))),
            },
            Self::Not(expr) => match expr.evaluate(row)? {
                Boolean(b) => Boolean(!b),
                Null => Null,
                value => return Err(Error::Value(format!("Can't negate {}", value))),
            },
            Self::Or(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs || rhs),
                (Boolean(lhs), Null) if lhs => Boolean(true),
                (Boolean(_), Null) => Null,
                (Null, Boolean(rhs)) if rhs => Boolean(true),
                (Null, Boolean(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Can't or {} and {}", lhs, rhs))),
            },

            // Comparison operations
            #[allow(clippy::float_cmp)] // Up to the user if they want to compare or not
            Self::Equal(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 == rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs == rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs == rhs),
                (String(lhs), String(rhs)) => Boolean(lhs == rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Self::GreaterThan(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                #[allow(clippy::bool_comparison)]
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs > rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs > rhs),
                (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 > rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs > rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs > rhs),
                (String(lhs), String(rhs)) => Boolean(lhs > rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Self::LessThan(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                #[allow(clippy::bool_comparison)]
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs < rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs < rhs),
                (Integer(lhs), Float(rhs)) => Boolean((lhs as f64) < rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs < rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs < rhs),
                (String(lhs), String(rhs)) => Boolean(lhs < rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Self::IsNull(expr) => match expr.evaluate(row)? {
                Null => Boolean(true),
                _ => Boolean(false),
            },

            // Mathematical operations
            Self::Add(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => Integer(
                    lhs.checked_add(rhs).ok_or_else(|| Error::Value("Integer overflow".into()))?,
                ),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 + rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Float(rhs)) => Float(lhs + rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs + rhs as f64),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Can't add {} and {}", lhs, rhs))),
            },
            Self::Assert(expr) => match expr.evaluate(row)? {
                Float(f) => Float(f),
                Integer(i) => Integer(i),
                Null => Null,
                expr => return Err(Error::Value(format!("Can't take the positive of {}", expr))),
            },
            Self::Divide(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(_), Integer(rhs)) if rhs == 0 => {
                    return Err(Error::Value("Can't divide by zero".into()))
                }
                (Integer(lhs), Integer(rhs)) => Integer(lhs / rhs),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 / rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs / rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs / rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't divide {} and {}", lhs, rhs)))
                }
            },
            Self::Exponentiate(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) if rhs >= 0 => Integer(
                    lhs.checked_pow(rhs as u32)
                        .ok_or_else(|| Error::Value("Integer overflow".into()))?,
                ),
                (Integer(lhs), Integer(rhs)) => Float((lhs as f64).powf(rhs as f64)),
                (Integer(lhs), Float(rhs)) => Float((lhs as f64).powf(rhs)),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float((lhs).powi(rhs as i32)),
                (Float(lhs), Float(rhs)) => Float((lhs).powf(rhs)),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't exponentiate {} and {}", lhs, rhs)))
                }
            },
            Self::Factorial(expr) => match expr.evaluate(row)? {
                Integer(i) if i < 0 => {
                    return Err(Error::Value("Can't take factorial of negative number".into()))
                }
                Integer(i) => Integer((1..=i).fold(1, |a, b| a * b as i64)),
                Null => Null,
                value => return Err(Error::Value(format!("Can't take factorial of {}", value))),
            },
            Self::Modulo(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                // This uses remainder semantics, like Postgres.
                (Integer(_), Integer(rhs)) if rhs == 0 => {
                    return Err(Error::Value("Can't divide by zero".into()))
                }
                (Integer(lhs), Integer(rhs)) => Integer(lhs % rhs),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 % rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs % rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs % rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't take modulo of {} and {}", lhs, rhs)))
                }
            },
            Self::Multiply(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => Integer(
                    lhs.checked_mul(rhs).ok_or_else(|| Error::Value("Integer overflow".into()))?,
                ),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 * rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs * rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs * rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't multiply {} and {}", lhs, rhs)))
                }
            },
            Self::Negate(expr) => match expr.evaluate(row)? {
                Integer(i) => Integer(-i),
                Float(f) => Float(-f),
                Null => Null,
                value => return Err(Error::Value(format!("Can't negate {}", value))),
            },
            Self::Subtract(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => Integer(
                    lhs.checked_sub(rhs).ok_or_else(|| Error::Value("Integer overflow".into()))?,
                ),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 - rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs - rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs - rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Can't subtract {} and {}", lhs, rhs)))
                }
            },

            // String operations
            Self::Like(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (String(lhs), String(rhs)) => Boolean(
                    Regex::new(&format!(
                        "^{}$",
                        regex::escape(&rhs)
                            .replace("%", ".*")
                            .replace(".*.*", "%")
                            .replace("_", ".")
                            .replace("..", "_")
                    ))?
                    .is_match(&lhs),
                ),
                (String(_), Null) => Null,
                (Null, String(_)) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Can't LIKE {} and {}", lhs, rhs))),
            },
        })
    }

    /// Walks the expression tree while calling a closure. Returns true as soon as the closure
    /// returns true. This is the inverse of walk().
    pub fn contains<F: Fn(&Expression) -> bool>(&self, visitor: &F) -> bool {
        !self.walk(&|e| !visitor(e))
    }

    /// Checks whether the expression is constant, i.e. that it does not contains field references.
    pub fn is_constant(&self) -> bool {
        !self.contains(&|expr| match expr {
            Self::Field(_, _) => true,
            _ => false,
        })
    }

    /// Replaces the expression with result of the closure. Helper function for transform().
    fn replace_with<F: Fn(Self) -> Result<Self>>(&mut self, f: F) -> Result<()> {
        // Temporarily replace expression with a null value, in case closure panics. May consider
        // replace_with crate if this hampers performance.
        let expr = replace(self, Expression::Constant(Value::Null));
        replace(self, f(expr)?);
        Ok(())
    }

    /// Transforms the expression tree by applying a closure before and after descending.
    pub fn transform<B, A>(mut self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Self) -> Result<Self>,
        A: Fn(Self) -> Result<Self>,
    {
        self = before(self)?;
        match &mut self {
            Self::Add(lhs, rhs)
            | Self::And(lhs, rhs)
            | Self::Divide(lhs, rhs)
            | Self::Equal(lhs, rhs)
            | Self::Exponentiate(lhs, rhs)
            | Self::GreaterThan(lhs, rhs)
            | Self::LessThan(lhs, rhs)
            | Self::Like(lhs, rhs)
            | Self::Modulo(lhs, rhs)
            | Self::Multiply(lhs, rhs)
            | Self::Or(lhs, rhs)
            | Self::Subtract(lhs, rhs) => {
                Self::replace_with(lhs, |e| e.transform(before, after))?;
                Self::replace_with(rhs, |e| e.transform(before, after))?;
            }

            Self::Assert(expr)
            | Self::Factorial(expr)
            | Self::IsNull(expr)
            | Self::Negate(expr)
            | Self::Not(expr) => Self::replace_with(expr, |e| e.transform(before, after))?,

            Self::Constant(_) | Self::Field(_, _) => {}
        };
        after(self)
    }

    /// Walks the expression tree, calling a closure for every node. Halts if closure returns false.
    pub fn walk<F: Fn(&Expression) -> bool>(&self, visitor: &F) -> bool {
        visitor(self)
            && match self {
                Self::Add(lhs, rhs)
                | Self::And(lhs, rhs)
                | Self::Divide(lhs, rhs)
                | Self::Equal(lhs, rhs)
                | Self::Exponentiate(lhs, rhs)
                | Self::GreaterThan(lhs, rhs)
                | Self::LessThan(lhs, rhs)
                | Self::Like(lhs, rhs)
                | Self::Modulo(lhs, rhs)
                | Self::Multiply(lhs, rhs)
                | Self::Or(lhs, rhs)
                | Self::Subtract(lhs, rhs) => lhs.walk(visitor) && rhs.walk(visitor),

                Self::Assert(expr)
                | Self::Factorial(expr)
                | Self::IsNull(expr)
                | Self::Negate(expr)
                | Self::Not(expr) => expr.walk(visitor),

                Self::Constant(_) | Self::Field(_, _) => true,
            }
    }
}
