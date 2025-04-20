use std::fmt::Display;

use regex::Regex;
use serde::{Deserialize, Serialize};

use super::{Label, Row, Value};
use crate::errinput;
use crate::error::Result;
use crate::sql::planner::Node;

/// An expression, made up of nested operations and values. Values are either
/// constants, or column references which are looked up in rows. Evaluated to a
/// final value during query execution.
///
/// Since this is a recursive data structure, we have to box each child
/// expression, which incurs a heap allocation per expression node. There are
/// clever ways to avoid this, but we keep it simple.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// A constant value.
    Constant(Value),
    /// A column reference. Looks up the value in a row during evaluation.
    Column(usize),

    /// a AND b: logical AND of two booleans.
    And(Box<Expression>, Box<Expression>),
    /// a OR b: logical OR of two booleans.
    Or(Box<Expression>, Box<Expression>),
    /// NOT a: logical NOT of a boolean.
    Not(Box<Expression>),

    /// a = b: equality comparison of two values.
    Equal(Box<Expression>, Box<Expression>),
    /// Greater than comparison of two values: a > b.
    GreaterThan(Box<Expression>, Box<Expression>),
    /// a < b: less than comparison of two values.
    LessThan(Box<Expression>, Box<Expression>),
    /// a IS NULL or a IS NAN: checks for the given value.
    Is(Box<Expression>, Value),

    /// a + b: adds two numbers.
    Add(Box<Expression>, Box<Expression>),
    /// a / b: divides two numbers.
    Divide(Box<Expression>, Box<Expression>),
    /// a ^b: exponentiates two numbers.
    Exponentiate(Box<Expression>, Box<Expression>),
    /// a!: takes the factorial of a number (4! = 4*3*2*1).
    Factorial(Box<Expression>),
    /// +a: the identify function, which simply returns the same number.
    Identity(Box<Expression>),
    /// a * b: multiplies two numbers.
    Multiply(Box<Expression>, Box<Expression>),
    /// -a: negates the given number.
    Negate(Box<Expression>),
    /// a % b: the remainder after dividing two numbers.
    Remainder(Box<Expression>, Box<Expression>),
    /// √a: takes the square root of a number.
    SquareRoot(Box<Expression>),
    /// a - b: subtracts two numbers.
    Subtract(Box<Expression>, Box<Expression>),

    // a LIKE b: checks if a string matches a pattern.
    Like(Box<Expression>, Box<Expression>),
}

// NB: display can't look up column labels, and will print numeric column
// indexes instead. Use Expression::format() to print with labels.
impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format(&Node::Nothing { columns: Vec::new() }))
    }
}

impl Expression {
    /// Formats the expression, using the given plan node to look up labels for
    /// column references.
    pub fn format(&self, node: &Node) -> String {
        use Expression::*;

        // Precedence levels, for () grouping. Matches the parser precedence.
        fn precedence(expr: &Expression) -> u8 {
            match expr {
                Column(_) | Constant(_) | SquareRoot(_) => 11,
                Identity(_) | Negate(_) => 10,
                Factorial(_) => 9,
                Exponentiate(_, _) => 8,
                Multiply(_, _) | Divide(_, _) | Remainder(_, _) => 7,
                Add(_, _) | Subtract(_, _) => 6,
                GreaterThan(_, _) | LessThan(_, _) => 5,
                Equal(_, _) | Like(_, _) | Is(_, _) => 4,
                Not(_) => 3,
                And(_, _) => 2,
                Or(_, _) => 1,
            }
        }

        // Helper to format a boxed expression, grouping it with () if needed.
        let fmt = |expr: &Expression| {
            let mut string = expr.format(node);
            if precedence(expr) < precedence(self) {
                string = format!("({string})");
            }
            string
        };

        match self {
            Constant(value) => format!("{value}"),
            Column(index) => match node.column_label(*index) {
                Label::None => format!("#{index}"),
                label => format!("{label}"),
            },

            And(lhs, rhs) => format!("{} AND {}", fmt(lhs), fmt(rhs)),
            Or(lhs, rhs) => format!("{} OR {}", fmt(lhs), fmt(rhs)),
            Not(expr) => format!("NOT {}", fmt(expr)),

            Equal(lhs, rhs) => format!("{} = {}", fmt(lhs), fmt(rhs)),
            GreaterThan(lhs, rhs) => format!("{} > {}", fmt(lhs), fmt(rhs)),
            LessThan(lhs, rhs) => format!("{} < {}", fmt(lhs), fmt(rhs)),
            Is(expr, Value::Null) => format!("{} IS NULL", fmt(expr)),
            Is(expr, Value::Float(f)) if f.is_nan() => format!("{} IS NAN", fmt(expr)),
            Is(_, v) => panic!("unexpected IS value {v}"),

            Add(lhs, rhs) => format!("{} + {}", fmt(lhs), fmt(rhs)),
            Divide(lhs, rhs) => format!("{} / {}", fmt(lhs), fmt(rhs)),
            Exponentiate(lhs, rhs) => format!("{} ^ {}", fmt(lhs), fmt(rhs)),
            Factorial(expr) => format!("{}!", fmt(expr)),
            Identity(expr) => fmt(expr),
            Multiply(lhs, rhs) => format!("{} * {}", fmt(lhs), fmt(rhs)),
            Negate(expr) => format!("-{}", fmt(expr)),
            Remainder(lhs, rhs) => format!("{} % {}", fmt(lhs), fmt(rhs)),
            SquareRoot(expr) => format!("sqrt({})", fmt(expr)),
            Subtract(lhs, rhs) => format!("{} - {}", fmt(lhs), fmt(rhs)),

            Like(lhs, rhs) => format!("{} LIKE {}", fmt(lhs), fmt(rhs)),
        }
    }

    /// Evaluates an expression, returning a constant value. Column references
    /// are looked up in the given row (or panic if the row is None).
    pub fn evaluate(&self, row: Option<&Row>) -> Result<Value> {
        use Value::*;

        Ok(match self {
            // Constant values return themselves.
            Self::Constant(value) => value.clone(),

            // Column references look up a row value. The planner ensures that
            // only constant expressions are evaluated without a row.
            Self::Column(index) => row.and_then(|r| r.get(*index)).cloned().expect("invalid index"),

            // Logical AND. Inputs must be boolean or NULL. NULLs generally
            // yield NULL, except the special case NULL AND false == false.
            Self::And(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs && rhs),
                (Boolean(b), Null) | (Null, Boolean(b)) if !b => Boolean(false),
                (Boolean(_), Null) | (Null, Boolean(_)) | (Null, Null) => Null,
                (lhs, rhs) => return errinput!("can't AND {lhs} and {rhs}"),
            },

            // Logical OR. Inputs must be boolean or NULL. NULLs generally
            // yield NULL, except the special case NULL OR true == true.
            Self::Or(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs || rhs),
                (Boolean(b), Null) | (Null, Boolean(b)) if b => Boolean(true),
                (Boolean(_), Null) | (Null, Boolean(_)) | (Null, Null) => Null,
                (lhs, rhs) => return errinput!("can't OR {lhs} and {rhs}"),
            },

            // Logical NOT. Input must be boolean or NULL.
            Self::Not(expr) => match expr.evaluate(row)? {
                Boolean(b) => Boolean(!b),
                Null => Null,
                value => return errinput!("can't NOT {value}"),
            },

            // Comparisons. Must be of same type, except floats and integers
            // which are interchangeable. NULLs yield NULL, NaNs yield NaN.
            //
            // Does not dispatch to Value.cmp() because comparison and sorting
            // is different for Nulls and NaNs in SQL and code.
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

            Self::Is(expr, Null) => Boolean(expr.evaluate(row)? == Null),
            Self::Is(expr, Float(f)) if f.is_nan() => match expr.evaluate(row)? {
                Float(f) => Boolean(f.is_nan()),
                Null => Null,
                v => return errinput!("IS NAN can't be used with {}", v.datatype().unwrap()),
            },
            Self::Is(_, v) => panic!("invalid IS value {v}"), // enforced by parser

            // Mathematical operations. Inputs must be numbers, but integers and
            // floats are interchangeable (float when mixed). NULLs yield NULL.
            // Errors on integer overflow, but floats yield infinity or NaN.
            Self::Add(lhs, rhs) => lhs.evaluate(row)?.checked_add(&rhs.evaluate(row)?)?,
            Self::Divide(lhs, rhs) => lhs.evaluate(row)?.checked_div(&rhs.evaluate(row)?)?,
            Self::Exponentiate(lhs, rhs) => lhs.evaluate(row)?.checked_pow(&rhs.evaluate(row)?)?,
            Self::Factorial(expr) => match expr.evaluate(row)? {
                Integer(i @ 0..) => {
                    (1..=i).try_fold(Integer(1), |p, i| p.checked_mul(&Integer(i)))?
                }
                Null => Null,
                value => return errinput!("can't take factorial of {value}"),
            },
            Self::Identity(expr) => match expr.evaluate(row)? {
                value @ (Integer(_) | Float(_) | Null) => value,
                expr => return errinput!("can't take the identity of {expr}"),
            },
            Self::Multiply(lhs, rhs) => lhs.evaluate(row)?.checked_mul(&rhs.evaluate(row)?)?,
            Self::Negate(expr) => match expr.evaluate(row)? {
                Integer(i) => Integer(-i),
                Float(f) => Float(-f),
                Null => Null,
                value => return errinput!("can't negate {value}"),
            },
            Self::Remainder(lhs, rhs) => lhs.evaluate(row)?.checked_rem(&rhs.evaluate(row)?)?,
            Self::SquareRoot(expr) => match expr.evaluate(row)? {
                Integer(i @ 0..) => Float((i as f64).sqrt()),
                Float(f) => Float(f.sqrt()),
                Null => Null,
                value => return errinput!("can't take square root of {value}"),
            },
            Self::Subtract(lhs, rhs) => lhs.evaluate(row)?.checked_sub(&rhs.evaluate(row)?)?,

            // LIKE pattern matching, using _ and % as single- and
            // multi-character wildcards. Inputs must be strings. NULLs yield
            // NULL. There's no support for escaping an _ and %.
            Self::Like(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (String(lhs), String(rhs)) => {
                    // We could precompile the pattern if it's constant, instead
                    // of recompiling it for every row, but we keep it simple.
                    let pattern =
                        format!("^{}$", regex::escape(&rhs).replace('%', ".*").replace('_', "."));
                    Boolean(Regex::new(&pattern)?.is_match(&lhs))
                }
                (String(_), Null) | (Null, String(_)) | (Null, Null) => Null,
                (lhs, rhs) => return errinput!("can't LIKE {lhs} and {rhs}"),
            },
        })
    }

    /// Recursively walks the expression tree depth-first, calling the given
    /// closure until it returns false. Returns true otherwise.
    pub fn walk(&self, visitor: &mut impl FnMut(&Expression) -> bool) -> bool {
        if !visitor(self) {
            return false;
        }
        match self {
            Self::Add(lhs, rhs)
            | Self::And(lhs, rhs)
            | Self::Divide(lhs, rhs)
            | Self::Equal(lhs, rhs)
            | Self::Exponentiate(lhs, rhs)
            | Self::GreaterThan(lhs, rhs)
            | Self::LessThan(lhs, rhs)
            | Self::Like(lhs, rhs)
            | Self::Multiply(lhs, rhs)
            | Self::Or(lhs, rhs)
            | Self::Remainder(lhs, rhs)
            | Self::Subtract(lhs, rhs) => lhs.walk(visitor) && rhs.walk(visitor),

            Self::Factorial(expr)
            | Self::Identity(expr)
            | Self::Is(expr, _)
            | Self::Negate(expr)
            | Self::Not(expr)
            | Self::SquareRoot(expr) => expr.walk(visitor),

            Self::Constant(_) | Self::Column(_) => true,
        }
    }

    /// Recursively walks the expression tree depth-first, calling the given
    /// closure until it returns true. Returns false otherwise. This is the
    /// inverse of walk().
    pub fn contains(&self, visitor: &impl Fn(&Expression) -> bool) -> bool {
        !self.walk(&mut |e| !visitor(e))
    }

    /// Transforms the expression by recursively applying the given closures
    /// depth-first to each node before/after descending.
    pub fn transform(
        mut self,
        before: &impl Fn(Self) -> Result<Self>,
        after: &impl Fn(Self) -> Result<Self>,
    ) -> Result<Self> {
        // Helper for transforming boxed expressions.
        let xform = |mut expr: Box<Expression>| -> Result<Box<Expression>> {
            *expr = expr.transform(before, after)?;
            Ok(expr)
        };

        self = before(self)?;
        self = match self {
            Self::Add(lhs, rhs) => Self::Add(xform(lhs)?, xform(rhs)?),
            Self::And(lhs, rhs) => Self::And(xform(lhs)?, xform(rhs)?),
            Self::Divide(lhs, rhs) => Self::Divide(xform(lhs)?, xform(rhs)?),
            Self::Equal(lhs, rhs) => Self::Equal(xform(lhs)?, xform(rhs)?),
            Self::Exponentiate(lhs, rhs) => Self::Exponentiate(xform(lhs)?, xform(rhs)?),
            Self::GreaterThan(lhs, rhs) => Self::GreaterThan(xform(lhs)?, xform(rhs)?),
            Self::LessThan(lhs, rhs) => Self::LessThan(xform(lhs)?, xform(rhs)?),
            Self::Like(lhs, rhs) => Self::Like(xform(lhs)?, xform(rhs)?),
            Self::Multiply(lhs, rhs) => Self::Multiply(xform(lhs)?, xform(rhs)?),
            Self::Or(lhs, rhs) => Self::Or(xform(lhs)?, xform(rhs)?),
            Self::Remainder(lhs, rhs) => Self::Remainder(xform(lhs)?, xform(rhs)?),
            Self::SquareRoot(expr) => Self::SquareRoot(xform(expr)?),
            Self::Subtract(lhs, rhs) => Self::Subtract(xform(lhs)?, xform(rhs)?),

            Self::Factorial(expr) => Self::Factorial(xform(expr)?),
            Self::Identity(expr) => Self::Identity(xform(expr)?),
            Self::Is(expr, value) => Self::Is(xform(expr)?, value),
            Self::Negate(expr) => Self::Negate(xform(expr)?),
            Self::Not(expr) => Self::Not(xform(expr)?),

            expr @ (Self::Constant(_) | Self::Column(_)) => expr,
        };
        self = after(self)?;
        Ok(self)
    }

    /// Converts the expression into conjunctive normal form, i.e. an AND of
    /// ORs, useful during plan optimization. This is done by converting to
    /// negation normal form and then applying De Morgan's distributive law.
    pub fn into_cnf(self) -> Self {
        use Expression::{And, Or};

        let xform = |expr| {
            // Can't use a single match; needs deref patterns.
            let Or(lhs, rhs) = expr else {
                return expr;
            };
            match (*lhs, *rhs) {
                // (x AND y) OR z → (x OR z) AND (y OR z)
                (And(l, r), rhs) => And(Or(l, rhs.clone().into()).into(), Or(r, rhs.into()).into()),
                // x OR (y AND z) → (x OR y) AND (x OR z)
                (lhs, And(l, r)) => And(Or(lhs.clone().into(), l).into(), Or(lhs.into(), r).into()),
                // Otherwise, do nothing.
                (lhs, rhs) => Or(lhs.into(), rhs.into()),
            }
        };
        self.into_nnf().transform(&|e| Ok(xform(e)), &Ok).unwrap() // infallible
    }

    /// Converts the expression into conjunctive normal form as a vector of
    /// ANDed expressions (instead of nested ANDs).
    pub fn into_cnf_vec(self) -> Vec<Self> {
        let mut cnf = Vec::new();
        let mut stack = vec![self.into_cnf()];
        while let Some(expr) = stack.pop() {
            if let Self::And(lhs, rhs) = expr {
                stack.extend([*rhs, *lhs]); // push lhs last to pop it first
            } else {
                cnf.push(expr);
            }
        }
        cnf
    }

    /// Converts the expression into negation normal form. This pushes NOT
    /// operators into the tree using De Morgan's laws, such that they're always
    /// below other logical operators. It is a useful intermediate form for
    /// applying other logical normalizations.
    pub fn into_nnf(self) -> Self {
        use Expression::{And, Not, Or};

        let xform = |expr| {
            // Can't use a single match; needs deref patterns.
            let Not(inner) = expr else {
                return expr;
            };
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
        self.transform(&|e| Ok(xform(e)), &Ok).unwrap() // infallible
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

    /// Checks if an expression is a single column lookup (i.e. a disjunction of
    /// = or IS NULL/NAN for a single column), returning the column index.
    pub fn is_column_lookup(&self) -> Option<usize> {
        use Expression::*;

        match &self {
            // Column/constant equality can use index lookups. NULL and NaN are
            // handled in into_column_values().
            Equal(lhs, rhs) => match (lhs.as_ref(), rhs.as_ref()) {
                (Column(c), Constant(_)) | (Constant(_), Column(c)) => Some(*c),
                _ => None,
            },
            // IS NULL and IS NAN can use index lookups.
            Is(expr, _) => match expr.as_ref() {
                Column(c) => Some(*c),
                _ => None,
            },
            // All OR branches must be lookups on the same column:
            // id = 1 OR id = 2 OR id = 3.
            Or(lhs, rhs) => match (lhs.is_column_lookup(), rhs.is_column_lookup()) {
                (Some(l), Some(r)) if l == r => Some(l),
                _ => None,
            },
            _ => None,
        }
    }

    /// Extracts column lookup values for the given column. Panics if the
    /// expression isn't a lookup of the given column, i.e. is_column_lookup()
    /// must return true for the expression.
    pub fn into_column_values(self, index: usize) -> Vec<Value> {
        use Expression::*;

        match self {
            Equal(lhs, rhs) => match (*lhs, *rhs) {
                (Column(column), Constant(value)) | (Constant(value), Column(column)) => {
                    assert_eq!(column, index, "unexpected column");
                    // NULL and NAN index lookups are for IS NULL and IS NAN.
                    // Equality shouldn't match anything, return empty vec.
                    if value.is_undefined() { Vec::new() } else { vec![value] }
                }
                (lhs, rhs) => panic!("unexpected expression {:?}", Equal(lhs.into(), rhs.into())),
            },
            // IS NULL and IS NAN can use index lookups.
            Is(expr, value) => match *expr {
                Column(column) => {
                    assert_eq!(column, index, "unexpected column");
                    vec![value]
                }
                expr => panic!("unexpected expression {expr:?}"),
            },
            Or(lhs, rhs) => {
                let mut values = lhs.into_column_values(index);
                values.extend(rhs.into_column_values(index));
                values
            }
            expr => panic!("unexpected expression {expr:?}"),
        }
    }

    /// Replaces column references from → to.
    pub fn replace_column(self, from: usize, to: usize) -> Self {
        let xform = |expr| match expr {
            Expression::Column(i) if i == from => Expression::Column(to),
            expr => expr,
        };
        self.transform(&|e| Ok(xform(e)), &Ok).unwrap() // infallible
    }

    /// Shifts column references by the given amount (can be negative).
    pub fn shift_column(self, diff: isize) -> Self {
        let xform = |expr| match expr {
            Expression::Column(i) => Expression::Column((i as isize + diff) as usize),
            expr => expr,
        };
        self.transform(&|e| Ok(xform(e)), &Ok).unwrap() // infallible
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
