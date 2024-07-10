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
    Field(usize, Label),

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
    /// Returns true if the value is null.
    IsNull(Box<Expression>),
    /// Returns true if the value is a f64 NaN.
    IsNaN(Box<Expression>),

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
            Self::Field(index, Label::None) => write!(f, "#{index}"),
            Self::Field(_, label) => write!(f, "{label}"),

            Self::And(lhs, rhs) => write!(f, "{lhs} AND {rhs}"),
            Self::Or(lhs, rhs) => write!(f, "{lhs} OR {rhs}"),
            Self::Not(expr) => write!(f, "NOT {expr}"),

            Self::Equal(lhs, rhs) => write!(f, "{lhs} = {rhs}"),
            Self::GreaterThan(lhs, rhs) => write!(f, "{lhs} > {rhs}"),
            Self::LessThan(lhs, rhs) => write!(f, "{lhs} < {rhs}"),
            Self::IsNull(expr) => write!(f, "{expr} IS NULL"),
            Self::IsNaN(expr) => write!(f, "{expr} IS NAN"),

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
            // which are interchangeable. NULLs yield NULL, NaNs yield NaN.
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
            Self::IsNaN(expr) => match expr.evaluate(row)? {
                Float(f) => Boolean(f.is_nan()),
                Null => Null,
                v => return errinput!("IS NAN can't be used with {}", v.datatype().unwrap()),
            },

            // Mathematical operations. Inputs must be numbers, but integers and
            // floats are interchangeable (float when mixed). NULLs yield NULL.
            Self::Add(lhs, rhs) => lhs.evaluate(row)?.checked_add(&rhs.evaluate(row)?)?,
            Self::Divide(lhs, rhs) => lhs.evaluate(row)?.checked_div(&rhs.evaluate(row)?)?,
            Self::Exponentiate(lhs, rhs) => lhs.evaluate(row)?.checked_pow(&rhs.evaluate(row)?)?,
            Self::Factorial(expr) => match expr.evaluate(row)? {
                Integer(i) if i < 0 => return errinput!("can't take factorial of negative number"),
                Integer(i) => (1..=i).try_fold(Integer(1), |p, i| p.checked_mul(&Integer(i)))?,
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
            // multi-character wildcards. Inputs must be strings. NULLs yield
            // NULL. There's no support for escaping an _ and %.
            Self::Like(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (String(lhs), String(rhs)) => {
                    let pattern =
                        format!("^{}$", regex::escape(&rhs).replace('%', ".*").replace('_', "."));
                    Boolean(regex::Regex::new(&pattern)?.is_match(&lhs))
                }
                (String(_), Null) | (Null, String(_)) | (Null, Null) => Null,
                (lhs, rhs) => return errinput!("can't LIKE {lhs} and {rhs}"),
            },
        })
    }

    /// Recursively walks the expression tree depth-first, calling the given
    /// closure until it returns false. Returns true otherwise.
    pub fn walk(&self, visitor: &mut impl FnMut(&Expression) -> bool) -> bool {
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
                | Self::IsNaN(expr)
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
        !self.walk(&mut |e| !visitor(e))
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
            Self::IsNaN(expr) => Self::IsNaN(transform(expr)?),
            Self::IsNull(expr) => Self::IsNull(transform(expr)?),
            Self::Negate(expr) => Self::Negate(transform(expr)?),
            Self::Not(expr) => Self::Not(transform(expr)?),

            expr @ (Self::Constant(_) | Self::Field(_, _)) => expr,
        };
        self = after(self)?;
        Ok(self)
    }

    /// Converts the expression into conjunctive normal form, i.e. an AND of
    /// ORs, which is useful when optimizing plans. This is done by converting
    /// to negation normal form and then applying De Morgan's distributive law.
    pub fn into_cnf(self) -> Self {
        use Expression::*;
        let transform = |expr| {
            // We can't use a single match, since it needs deref patterns.
            let Or(lhs, rhs) = expr else { return expr };
            match (*lhs, *rhs) {
                // (x AND y) OR z → (x OR z) AND (y OR z)
                (And(l, r), rhs) => And(Or(l, rhs.clone().into()).into(), Or(r, rhs.into()).into()),
                // x OR (y AND z) → (x OR y) AND (x OR z)
                (lhs, And(l, r)) => And(Or(lhs.clone().into(), l).into(), Or(lhs.into(), r).into()),
                // Otherwise, do nothing.
                (lhs, rhs) => Or(lhs.into(), rhs.into()),
            }
        };
        self.into_nnf().transform(&|e| Ok(transform(e)), &Ok).unwrap() // never fails
    }

    /// Converts the expression into negation normal form. This pushes NOT
    /// operators into the tree using De Morgan's laws, such that they're always
    /// below other logical operators. It is a useful intermediate form for
    /// applying other logical normalizations.
    pub fn into_nnf(self) -> Self {
        use Expression::*;
        let transform = |expr| {
            let Not(inner) = expr else { return expr };
            match *inner {
                // NOT (x AND y) → (NOT x) OR (NOT y)
                And(lhs, rhs) => Or(Not(lhs).into(), Not(rhs).into()),
                // NOT (x OR y) → (NOT x) AND (NOT y)
                Or(lhs, rhs) => And(Not(lhs).into(), Not(rhs).into()),
                // NOT NOT x → x
                Not(inner) => *inner,
                // Otherwise, do nothing.
                expr => Not(expr.into()),
            }
        };
        self.transform(&|e| Ok(transform(e)), &Ok).unwrap() // never fails
    }

    /// Converts the expression into conjunctive normal form as a vector of
    /// ANDed expressions (instead of nested ANDs).
    pub fn into_cnf_vec(self) -> Vec<Self> {
        let mut cnf = Vec::new();
        let mut stack = vec![self.into_cnf()];
        while let Some(expr) = stack.pop() {
            if let Self::And(lhs, rhs) = expr {
                stack.extend([*rhs, *lhs]); // put LHS last to process next
            } else {
                cnf.push(expr);
            }
        }
        cnf
    }

    /// Creates an expression by ANDing together a vector, or None if empty.
    pub fn and_vec(exprs: Vec<Expression>) -> Option<Self> {
        let mut iter = exprs.into_iter();
        let mut expr = iter.next()?;
        for rhs in iter {
            expr = Expression::And(expr.into(), rhs.into());
        }
        Some(expr)
    }

    /// Checks if an expression is a single field lookup (i.e. a disjunction of
    /// = or IS NULL/NAN referencing a single field), returning the field index.
    pub fn is_field_lookup(&self) -> Option<usize> {
        use Expression::*;
        match &self {
            // Equality comparisons with = between field and constant value can
            // use index lookups. NULL and NaN won't return any matches, but we
            // handle this in into_field_values().
            Equal(lhs, rhs) => match (lhs.as_ref(), rhs.as_ref()) {
                (Field(f, _), Constant(_)) | (Constant(_), Field(f, _)) => Some(*f),
                _ => None,
            },
            // IS NULL and IS NAN can use index lookups, since we index these.
            IsNull(expr) | IsNaN(expr) => match expr.as_ref() {
                Field(f, _) => Some(*f),
                _ => None,
            },
            // For OR branches, check if all branches are lookups on the same
            // field, i.e. foo = 1 OR foo = 2 OR foo = 3.
            Or(lhs, rhs) => match (lhs.is_field_lookup(), rhs.is_field_lookup()) {
                (Some(l), Some(r)) if l == r => Some(l),
                _ => None,
            },
            _ => None,
        }
    }

    /// Converts the expression into a set of single-field lookup values if possible.
    pub fn into_field_values(self) -> Option<(usize, Vec<Value>)> {
        use Expression::*;
        match self {
            Equal(lhs, rhs) => match (*lhs, *rhs) {
                // NULL and NAN index lookups are for IS NULL and IS NAN.
                // Equality comparisons with = shouldn't yield any results, so
                // just return an empty value set for these.
                (Field(f, _), Constant(v)) | (Constant(v), Field(f, _)) if v.is_undefined() => {
                    Some((f, Vec::new()))
                }
                (Field(f, _), Constant(v)) | (Constant(v), Field(f, _)) => Some((f, vec![v])),
                _ => None,
            },
            // IS NULL index lookups should look up NULL.
            IsNull(expr) => match *expr {
                Field(f, _) => Some((f, vec![Value::Null])),
                _ => None,
            },
            // IS NAN index lookups should look up NAN.
            IsNaN(expr) => match *expr {
                Field(f, _) => Some((f, vec![Value::Float(f64::NAN)])),
                _ => None,
            },
            Or(lhs, rhs) => match (lhs.into_field_values(), rhs.into_field_values()) {
                (Some((l, lvec)), Some((r, rvec))) if l == r => {
                    Some((l, lvec.into_iter().chain(rvec).collect()))
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Replaces field references with the given field.
    pub fn replace_field(self, from: usize, to: usize, label: &Label) -> Self {
        let transform = |expr| match expr {
            Expression::Field(i, _) if i == from => Expression::Field(to, label.clone()),
            expr => expr,
        };
        self.transform(&|e| Ok(transform(e)), &Ok).unwrap() // infallible
    }

    /// Shifts any field indexes by the given amount.
    pub fn shift_field(self, diff: isize) -> Self {
        let transform = |expr| match expr {
            Expression::Field(i, label) => Expression::Field((i as isize + diff) as usize, label),
            expr => expr,
        };
        self.transform(&|e| Ok(transform(e)), &Ok).unwrap() // infallible
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
