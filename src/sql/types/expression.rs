use super::{Label, Row, Value};
use crate::errinput;
use crate::error::Result;

use serde::{Deserialize, Serialize};

/// An expression, made up of nested values and operators. Values can either be
/// constants or row field references.
///
/// Since this is a recursive data structure, we have to box each child
/// expression, which incurs a heap allocation. There are clever ways to get
/// around this, but we keep it simple.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// A constant value.
    Constant(Value),
    /// A field reference (row index) with optional label. The label is only
    /// used for display purposes.
    Field(usize, Option<Label>),

    /// Logical AND of two booleans: a AND b.
    And(Box<Expression>, Box<Expression>),
    /// Logical OR of two booleans: a OR b.
    Or(Box<Expression>, Box<Expression>),
    /// Logical NOT of a boolean: NOT a.
    Not(Box<Expression>),

    /// Equality comparison of two values: a = b.
    Equal(Box<Expression>, Box<Expression>),
    /// > comparison of two values: a > b.
    GreaterThan(Box<Expression>, Box<Expression>),
    /// < comparison of two values: a < b.
    LessThan(Box<Expression>, Box<Expression>),
    /// Returns true if the value is null: a IS NULL.
    IsNull(Box<Expression>),

    /// Adds two numbers: a + b.
    Add(Box<Expression>, Box<Expression>),
    /// Divides two numbers: a / b.
    Divide(Box<Expression>, Box<Expression>),
    /// Exponentiates two numbers, i.e. a ^ b.
    Exponentiate(Box<Expression>, Box<Expression>),
    /// Takes the factorial of a number: 4! = 4*3*2*1.
    Factorial(Box<Expression>),
    /// The identify function, which simply returns the same number.
    Identity(Box<Expression>),
    /// The remainder after dividing two numbers: a % b.
    Modulo(Box<Expression>, Box<Expression>),
    /// Multiplies two numbers: a * b.
    Multiply(Box<Expression>, Box<Expression>),
    /// Negates the given number: -a.
    Negate(Box<Expression>),
    /// Subtracts two numbers: a - b.
    Subtract(Box<Expression>, Box<Expression>),

    // Checks if a string matches a pattern: a LIKE b.
    Like(Box<Expression>, Box<Expression>),
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(value) => write!(f, "{value}"),
            Self::Field(index, None) => write!(f, "#{index}"),
            Self::Field(_, Some((None, name))) => write!(f, "{name}"),
            Self::Field(_, Some((Some(table), name))) => write!(f, "{table}.{name}"),

            Self::And(lhs, rhs) => write!(f, "{lhs} AND {rhs}"),
            Self::Or(lhs, rhs) => write!(f, "{lhs} OR {rhs}"),
            Self::Not(expr) => write!(f, "NOT {expr}"),

            Self::Equal(lhs, rhs) => write!(f, "{lhs} = {rhs}"),
            Self::GreaterThan(lhs, rhs) => write!(f, "{lhs} > {rhs}"),
            Self::LessThan(lhs, rhs) => write!(f, "{lhs} < {rhs}"),
            Self::IsNull(expr) => write!(f, "{expr} IS NULL"),

            Self::Add(lhs, rhs) => write!(f, "{lhs} + {rhs}"),
            Self::Divide(lhs, rhs) => write!(f, "{lhs} / {rhs}"),
            Self::Exponentiate(lhs, rhs) => write!(f, "{lhs} ^ {rhs}"),
            Self::Factorial(expr) => write!(f, "{expr}!"),
            Self::Identity(expr) => write!(f, "{expr}"),
            Self::Modulo(lhs, rhs) => write!(f, "{lhs} % {rhs}"),
            Self::Multiply(lhs, rhs) => write!(f, "{lhs} * {rhs}"),
            Self::Negate(expr) => write!(f, "-{expr}"),
            Self::Subtract(lhs, rhs) => write!(f, "{lhs} - {rhs}"),

            Self::Like(lhs, rhs) => write!(f, "{lhs} LIKE {rhs}"),
        }
    }
}

impl Expression {
    /// Evaluates an expression, returning a value. If a row is given, Field
    /// references will look up the row value at the field index. If no row is
    /// given, any field references yield NULL.
    pub fn evaluate(&self, row: Option<&Row>) -> Result<Value> {
        use Value::*;
        Ok(match self {
            // Constant values return itself.
            Self::Constant(value) => value.clone(),

            // Field references look up a row value. The planner must make sure
            // the field reference is valid.
            Self::Field(i, _) => row.map(|row| row[*i].clone()).unwrap_or(Null),

            // Logical AND. Inputs must be boolean or NULL. NULLs generally
            // yield NULL, except the special case NULL AND false == false.
            Self::And(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs && rhs),
                (Boolean(b), Null) | (Null, Boolean(b)) if !b => Boolean(false),
                (Boolean(_), Null) | (Null, Boolean(_)) | (Null, Null) => Null,
                (lhs, rhs) => return errinput!("can't and {lhs} and {rhs}"),
            },

            // Logical OR. Inputs must be boolean or NULL. NULLs generally
            // yield NULL, except the special case NULL OR true == true.
            Self::Or(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs || rhs),
                (Boolean(b), Null) | (Null, Boolean(b)) if b => Boolean(true),
                (Boolean(_), Null) | (Null, Boolean(_)) | (Null, Null) => Null,
                (lhs, rhs) => return errinput!("can't or {lhs} and {rhs}"),
            },

            // Logical NOT. Input must be boolean or NULL.
            Self::Not(expr) => match expr.evaluate(row)? {
                Boolean(b) => Boolean(!b),
                Null => Null,
                value => return errinput!("can't negate {value}"),
            },

            // Comparisons. Must be of same type, except floats and integers
            // which are interchangeable. NULLs yield NULL.
            //
            // Does not dispatch to Value.cmp() because sorting and comparisons
            // are different for f64 NaN and -0 values.
            #[allow(clippy::float_cmp)]
            Self::Equal(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 == rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs == rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs == rhs),
                (String(lhs), String(rhs)) => Boolean(lhs == rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => return errinput!("can't compare {lhs} and {rhs}"),
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
                (lhs, rhs) => return errinput!("can't compare {lhs} and {rhs}"),
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
                (lhs, rhs) => return errinput!("can't compare {lhs} and {rhs}"),
            },
            Self::IsNull(expr) => Boolean(expr.evaluate(row)? == Null),

            // Mathematical operations. Inputs must be numbers, but integers and
            // floats are interchangeable (float when mixed). NULLs yield NULL.
            Self::Add(lhs, rhs) => lhs.evaluate(row)?.checked_add(&rhs.evaluate(row)?)?,
            Self::Divide(lhs, rhs) => lhs.evaluate(row)?.checked_div(&rhs.evaluate(row)?)?,
            Self::Exponentiate(lhs, rhs) => lhs.evaluate(row)?.checked_pow(&rhs.evaluate(row)?)?,
            Self::Factorial(expr) => match expr.evaluate(row)? {
                Integer(i) if i < 0 => return errinput!("can't take factorial of negative number"),
                Integer(i) => Integer((1..=i).product()),
                Null => Null,
                value => return errinput!("can't take factorial of {value}"),
            },
            Self::Identity(expr) => match expr.evaluate(row)? {
                v @ (Integer(_) | Float(_) | Null) => v,
                expr => return errinput!("can't take the identity of {expr}"),
            },
            Self::Modulo(lhs, rhs) => lhs.evaluate(row)?.checked_rem(&rhs.evaluate(row)?)?,
            Self::Multiply(lhs, rhs) => lhs.evaluate(row)?.checked_mul(&rhs.evaluate(row)?)?,
            Self::Negate(expr) => match expr.evaluate(row)? {
                Integer(i) => Integer(-i),
                Float(f) => Float(-f),
                Null => Null,
                value => return errinput!("can't negate {value}"),
            },
            Self::Subtract(lhs, rhs) => lhs.evaluate(row)?.checked_sub(&rhs.evaluate(row)?)?,

            // LIKE pattern matching, using _ and % as single- and
            // multi-character wildcards. Can be escaped as __ and %%. Inputs
            // must be strings. NULLs yield NULL.
            Self::Like(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (String(lhs), String(rhs)) => {
                    let pattern = format!(
                        "^{}$",
                        regex::escape(&rhs)
                            .replace('%', ".*")
                            .replace('_', ".")
                            .replace(".*.*", "%") // escaped %%
                            .replace("..", "_") // escaped __
                    );
                    Boolean(regex::Regex::new(&pattern)?.is_match(&lhs))
                }
                (String(_), Null) | (Null, String(_)) | (Null, Null) => Null,
                (lhs, rhs) => return errinput!("can't LIKE {lhs} and {rhs}"),
            },
        })
    }

    /// Recursively walks the expression tree depth-first, calling the given
    /// closure until it returns false. Returns true otherwise.
    pub fn walk(&self, visitor: &impl Fn(&Expression) -> bool) -> bool {
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

                Self::Factorial(expr)
                | Self::Identity(expr)
                | Self::IsNull(expr)
                | Self::Negate(expr)
                | Self::Not(expr) => expr.walk(visitor),

                Self::Constant(_) | Self::Field(_, _) => true,
            }
    }

    /// Recursively walks the expression tree depth-first, calling the given
    /// closure until it returns true. Returns false otherwise. This is the
    /// inverse of walk().
    pub fn contains(&self, visitor: &impl Fn(&Expression) -> bool) -> bool {
        !self.walk(&|e| !visitor(e))
    }

    /// Transforms the expression tree by recursively applying the given
    /// closures depth-first to each node before/after descending.
    pub fn transform<B, A>(mut self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Self) -> Result<Self>,
        A: Fn(Self) -> Result<Self>,
    {
        // Helper for transforming boxed expressions.
        let transform = |mut expr: Box<Expression>| -> Result<Box<Expression>> {
            *expr = expr.transform(before, after)?;
            Ok(expr)
        };

        self = before(self)?;
        self = match self {
            Self::Add(lhs, rhs) => Self::Add(transform(lhs)?, transform(rhs)?),
            Self::And(lhs, rhs) => Self::And(transform(lhs)?, transform(rhs)?),
            Self::Divide(lhs, rhs) => Self::Divide(transform(lhs)?, transform(rhs)?),
            Self::Equal(lhs, rhs) => Self::Equal(transform(lhs)?, transform(rhs)?),
            Self::Exponentiate(lhs, rhs) => Self::Exponentiate(transform(lhs)?, transform(rhs)?),
            Self::GreaterThan(lhs, rhs) => Self::GreaterThan(transform(lhs)?, transform(rhs)?),
            Self::LessThan(lhs, rhs) => Self::LessThan(transform(lhs)?, transform(rhs)?),
            Self::Like(lhs, rhs) => Self::Like(transform(lhs)?, transform(rhs)?),
            Self::Modulo(lhs, rhs) => Self::Modulo(transform(lhs)?, transform(rhs)?),
            Self::Multiply(lhs, rhs) => Self::Multiply(transform(lhs)?, transform(rhs)?),
            Self::Or(lhs, rhs) => Self::Or(transform(lhs)?, transform(rhs)?),
            Self::Subtract(lhs, rhs) => Self::Subtract(transform(lhs)?, transform(rhs)?),

            Self::Factorial(expr) => Self::Factorial(transform(expr)?),
            Self::Identity(expr) => Self::Identity(transform(expr)?),
            Self::IsNull(expr) => Self::IsNull(transform(expr)?),
            Self::Negate(expr) => Self::Negate(transform(expr)?),
            Self::Not(expr) => Self::Not(transform(expr)?),

            expr @ (Self::Constant(_) | Self::Field(_, _)) => expr,
        };
        self = after(self)?;
        Ok(self)
    }

    // TODO: clean up the transformations below when the optimizer is cleaned up.

    /// Converts the expression into its negation normal form. This pushes NOT operators into the
    /// tree using De Morgan's laws, such that they never occur before other logical operators.
    pub fn into_nnf(self) -> Self {
        use Expression::*;
        // FIXME This should use a single match, but it's not possible to match on the boxed
        // children without box pattern syntax, which is unstable.
        self.transform(
            &|e| match e {
                Not(expr) => match *expr {
                    And(lhs, rhs) => Ok(Or(Not(lhs).into(), Not(rhs).into())),
                    Or(lhs, rhs) => Ok(And(Not(lhs).into(), Not(rhs).into())),
                    Not(inner) => Ok(*inner),
                    _ => Ok(Not(expr)),
                },
                _ => Ok(e),
            },
            &Ok,
        )
        .unwrap()
    }

    /// Converts the expression into conjunctive normal form, i.e. an AND of ORs. This is done by
    /// converting to negation normal form and then applying the distributive law:
    /// (x AND y) OR z = (x OR z) AND (y OR z).
    pub fn into_cnf(self) -> Self {
        use Expression::*;
        self.into_nnf()
            .transform(
                &|e| match e {
                    Or(lhs, rhs) => match (*lhs, *rhs) {
                        (And(ll, lr), r) => {
                            Ok(And(Or(ll, r.clone().into()).into(), Or(lr, r.into()).into()))
                        }
                        (l, And(rl, rr)) => {
                            Ok(And(Or(l.clone().into(), rl).into(), Or(l.into(), rr).into()))
                        }
                        (lhs, rhs) => Ok(Or(lhs.into(), rhs.into())),
                    },
                    e => Ok(e),
                },
                &Ok,
            )
            .unwrap()
    }

    /// Converts the expression into conjunctive normal form as a vector.
    pub fn into_cnf_vec(self) -> Vec<Self> {
        let mut cnf = Vec::new();
        let mut stack = vec![self.into_cnf()];
        while let Some(expr) = stack.pop() {
            match expr {
                Self::And(lhs, rhs) => {
                    stack.push(*rhs);
                    stack.push(*lhs);
                }
                expr => cnf.push(expr),
            }
        }
        cnf
    }

    /// Converts the expression into disjunctive normal form, i.e. an OR of ANDs. This is done by
    /// converting to negation normal form and then applying the distributive law:
    /// (x OR y) AND z = (x AND z) OR (y AND z).
    pub fn into_dnf(self) -> Self {
        use Expression::*;
        self.into_nnf()
            .transform(
                &|e| match e {
                    And(lhs, rhs) => match (*lhs, *rhs) {
                        (Or(ll, lr), r) => {
                            Ok(Or(And(ll, r.clone().into()).into(), And(lr, r.into()).into()))
                        }
                        (l, Or(rl, rr)) => {
                            Ok(Or(And(l.clone().into(), rl).into(), And(l.into(), rr).into()))
                        }
                        (lhs, rhs) => Ok(And(lhs.into(), rhs.into())),
                    },
                    e => Ok(e),
                },
                &Ok,
            )
            .unwrap()
    }

    /// Converts the expression into disjunctive normal form as a vector.
    pub fn into_dnf_vec(self) -> Vec<Self> {
        let mut dnf = Vec::new();
        let mut stack = vec![self.into_dnf()];
        while let Some(expr) = stack.pop() {
            match expr {
                Self::Or(lhs, rhs) => {
                    stack.push(*rhs);
                    stack.push(*lhs);
                }
                expr => dnf.push(expr),
            }
        }
        dnf
    }

    /// Creates an expression by joining a vector in conjunctive normal form as an And.
    pub fn from_cnf_vec(mut cnf: Vec<Expression>) -> Option<Self> {
        if !cnf.is_empty() {
            let mut expr = cnf.remove(0);
            for rhs in cnf {
                expr = Expression::And(expr.into(), rhs.into());
            }
            Some(expr)
        } else {
            None
        }
    }

    /// Creates an expression by joining a vector in disjunctive normal form as an Or.
    pub fn from_dnf_vec(mut dnf: Vec<Expression>) -> Option<Self> {
        if !dnf.is_empty() {
            let mut expr = dnf.remove(0);
            for rhs in dnf {
                expr = Expression::Or(expr.into(), rhs.into());
            }
            Some(expr)
        } else {
            None
        }
    }

    // Checks if the expression is a field lookup, and returns the list of values looked up.
    // Expressions must be a combination of =, IS NULL, OR to be converted.
    pub fn as_lookup(&self, field: usize) -> Option<Vec<Value>> {
        use Expression::*;
        // FIXME This should use a single match level, but since the child expressions are boxed
        // that would require box patterns, which are unstable.
        match &self {
            Equal(lhs, rhs) => match (&**lhs, &**rhs) {
                (Field(i, _), Constant(v)) if i == &field => Some(vec![v.clone()]),
                (Constant(v), Field(i, _)) if i == &field => Some(vec![v.clone()]),
                (_, _) => None,
            },
            IsNull(e) => match &**e {
                Field(i, _) if i == &field => Some(vec![Value::Null]),
                _ => None,
            },
            Or(lhs, rhs) => match (lhs.as_lookup(field), rhs.as_lookup(field)) {
                (Some(mut lvalues), Some(mut rvalues)) => {
                    lvalues.append(&mut rvalues);
                    Some(lvalues)
                }
                _ => None,
            },
            _ => None,
        }
    }

    // Creates an expression from a list of field lookup values.
    pub fn from_lookup(field: usize, label: Option<Label>, values: Vec<Value>) -> Self {
        if values.is_empty() {
            return Expression::Equal(
                Expression::Field(field, label).into(),
                Expression::Constant(Value::Null).into(),
            );
        }
        Self::from_dnf_vec(
            values
                .into_iter()
                .map(|v| {
                    Expression::Equal(
                        Expression::Field(field, label.clone()).into(),
                        Expression::Constant(v).into(),
                    )
                })
                .collect(),
        )
        .unwrap()
    }
}

impl From<Value> for Expression {
    fn from(value: Value) -> Self {
        Expression::Constant(value)
    }
}

impl From<Value> for Box<Expression> {
    fn from(value: Value) -> Self {
        Box::new(value.into())
    }
}
