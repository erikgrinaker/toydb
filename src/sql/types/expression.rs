use super::Value;
use crate::Error;

use regex::Regex;
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// An expression, made up of constants and operations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    // Values
    Constant(Value),
    Field(Option<String>, String),
    Column(usize),

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
    pub fn evaluate(&self, env: &Environment) -> Result<Value, Error> {
        use Value::*;
        Ok(match self {
            // Constant values
            Self::Constant(c) => c.clone(),
            Self::Field(r, f) => env.lookup_field(r.as_deref(), f)?,
            Self::Column(i) => env.lookup_index(*i)?,

            // Logical operations
            Self::And(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs && rhs),
                (Boolean(lhs), Null) if !lhs => Boolean(false),
                (Boolean(_), Null) => Null,
                (Null, Boolean(rhs)) if !rhs => Boolean(false),
                (Null, Boolean(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Can't and {} and {}", lhs, rhs))),
            },
            Self::Not(expr) => match expr.evaluate(env)? {
                Boolean(b) => Boolean(!b),
                Null => Null,
                value => return Err(Error::Value(format!("Can't negate {}", value))),
            },
            Self::Or(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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
            Self::Equal(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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
            Self::GreaterThan(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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
            Self::LessThan(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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
            Self::IsNull(expr) => match expr.evaluate(env)? {
                Null => Boolean(true),
                _ => Boolean(false),
            },

            // Mathematical operations
            Self::Add(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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
            Self::Assert(expr) => match expr.evaluate(env)? {
                Float(f) => Float(f),
                Integer(i) => Integer(i),
                Null => Null,
                expr => return Err(Error::Value(format!("Can't take the positive of {}", expr))),
            },
            Self::Divide(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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
            Self::Exponentiate(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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
            Self::Factorial(expr) => match expr.evaluate(env)? {
                Integer(i) if i < 0 => {
                    return Err(Error::Value("Can't take factorial of negative number".into()))
                }
                Integer(i) => Integer((1..=i).fold(1, |a, b| a * b as i64)),
                Null => Null,
                value => return Err(Error::Value(format!("Can't take factorial of {}", value))),
            },
            Self::Modulo(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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
            Self::Multiply(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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
            Self::Negate(expr) => match expr.evaluate(env)? {
                Integer(i) => Integer(-i),
                Float(f) => Float(-f),
                Null => Null,
                value => return Err(Error::Value(format!("Can't negate {}", value))),
            },
            Self::Subtract(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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
            Self::Like(lhs, rhs) => match (lhs.evaluate(env)?, rhs.evaluate(env)?) {
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

    /// Checks whether the expression is constant
    pub fn is_constant(&self) -> bool {
        self.walk(&|expr| match expr {
            Self::Column(_) => false,
            Self::Field(_, _) => false,
            _ => true,
        })
    }

    /// Transforms the expression tree, by applying a callback function to it
    /// before and/or after descending into it.
    pub fn transform<B, A>(mut self, pre: &B, post: &A) -> Result<Self, Error>
    where
        B: Fn(Self) -> Result<Self, Error>,
        A: Fn(Self) -> Result<Self, Error>,
    {
        self = pre(self)?;
        // FIXME Ugly having to explicitly reconstruct each variant, but unable to
        // find a way to dynamically construct or update without changing signatures
        // to use mutable references over ownership.
        self = match self {
            // Constants
            node @ Self::Constant(_) | node @ Self::Field(_, _) | node @ Self::Column(_) => node,

            // Logical operations
            Self::And(lhs, rhs) => {
                Self::And(lhs.transform(pre, post)?.into(), rhs.transform(pre, post)?.into())
            }
            Self::Not(expr) => Self::Not(expr.transform(pre, post)?.into()),
            Self::Or(lhs, rhs) => {
                Self::Or(lhs.transform(pre, post)?.into(), rhs.transform(pre, post)?.into())
            }

            // Comparisons
            Self::Equal(lhs, rhs) => {
                Self::Equal(lhs.transform(pre, post)?.into(), rhs.transform(pre, post)?.into())
            }
            Self::GreaterThan(lhs, rhs) => Self::GreaterThan(
                lhs.transform(pre, post)?.into(),
                rhs.transform(pre, post)?.into(),
            ),
            Self::LessThan(lhs, rhs) => {
                Self::LessThan(lhs.transform(pre, post)?.into(), rhs.transform(pre, post)?.into())
            }
            Self::IsNull(expr) => Self::IsNull(expr.transform(pre, post)?.into()),

            // Mathematical operations
            Self::Add(lhs, rhs) => {
                Self::Add(lhs.transform(pre, post)?.into(), rhs.transform(pre, post)?.into())
            }
            Self::Assert(expr) => Self::Assert(expr.transform(pre, post)?.into()),
            Self::Divide(lhs, rhs) => {
                Self::Divide(lhs.transform(pre, post)?.into(), rhs.transform(pre, post)?.into())
            }
            Self::Exponentiate(lhs, rhs) => Self::Exponentiate(
                lhs.transform(pre, post)?.into(),
                rhs.transform(pre, post)?.into(),
            ),
            Self::Factorial(expr) => Self::Factorial(expr.transform(pre, post)?.into()),
            Self::Modulo(lhs, rhs) => {
                Self::Modulo(lhs.transform(pre, post)?.into(), rhs.transform(pre, post)?.into())
            }
            Self::Multiply(lhs, rhs) => {
                Self::Multiply(lhs.transform(pre, post)?.into(), rhs.transform(pre, post)?.into())
            }
            Self::Negate(expr) => Self::Negate(expr.transform(pre, post)?.into()),
            Self::Subtract(lhs, rhs) => {
                Self::Subtract(lhs.transform(pre, post)?.into(), rhs.transform(pre, post)?.into())
            }

            // String operations
            Self::Like(lhs, rhs) => {
                Self::Like(lhs.transform(pre, post)?.into(), rhs.transform(pre, post)?.into())
            }
        };
        post(self)
    }

    /// Walks the expression tree depth-first, calling a closure for every element.
    /// If the closure returns false, walking is halted.
    pub fn walk<F: Fn(&Expression) -> bool>(&self, visitor: &F) -> bool {
        if match self {
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

            Self::Constant(_) | Self::Field(_, _) | Self::Column(_) => true,
        } {
            visitor(self)
        } else {
            false
        }
    }
}

/// An expression evaluation environment
pub struct Environment<'a> {
    // For lookups by index (i.e. column number)
    index: Vec<&'a Value>,
    // For qualified lookups, with both relation and field name
    qualified: HashMap<(&'a str, &'a str), &'a Value>,
    // For unqualified lookups. Multiple fields with same unqualified name are ambiguous.
    unqualified: HashMap<&'a str, &'a Value>,
    ambiguous: HashSet<&'a str>,
}

impl<'a> Environment<'a> {
    /// Creates a new environment
    pub fn new() -> Self {
        Self {
            index: Vec::new(),
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ambiguous: HashSet::new(),
        }
    }

    /// Appends a value to the environment
    pub fn append(&mut self, relation: Option<&'a str>, field: Option<&'a str>, value: &'a Value) {
        self.index.push(value);
        if let Some(field) = field {
            if self.unqualified.contains_key(field) {
                self.unqualified.remove(field);
                self.ambiguous.insert(field);
            } else {
                self.unqualified.insert(field, value);
            }
        }
        if let (Some(relation), Some(field)) = (relation, field) {
            self.qualified.insert((relation, field), value);
        }
    }

    /// Looks up a value by field
    pub fn lookup_field(&self, relation: Option<&'a str>, field: &'a str) -> Result<Value, Error> {
        if let Some(relation) = relation {
            match self.qualified.get(&(relation, field)) {
                Some(r) => Ok((*r).clone()),
                None => Err(Error::Value(format!("Unknown field {}.{}", relation, field))),
            }
        } else if self.ambiguous.contains(field) {
            Err(Error::Value(format!("Ambiguous field {}", field)))
        } else {
            match self.unqualified.get(field) {
                Some(r) => Ok((*r).clone()),
                None => Err(Error::Value(format!("Unknown field {}", field))),
            }
        }
    }

    /// Looks up a value by index
    pub fn lookup_index(&self, index: usize) -> Result<Value, Error> {
        match self.index.get(index) {
            Some(r) => Ok((*r).clone()),
            None => Err(Error::Value(format!("Index {} out of bounds", index))),
        }
    }
}
