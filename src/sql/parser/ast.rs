use crate::sql::types::DataType;

use std::collections::BTreeMap;

/// The statement AST is the root node of the AST tree, which describes the
/// syntactic structure of a SQL query. It is passed to the planner, which
/// validates its contents and converts it into an execution plan.
#[derive(Debug)]
pub enum Statement {
    Begin {
        read_only: bool,
        as_of: Option<u64>,
    },
    Commit,
    Rollback,
    Explain(Box<Statement>),
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    Delete {
        table: String,
        r#where: Option<Expression>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Update {
        table: String,
        set: BTreeMap<String, Option<Expression>>, // None for DEFAULT value
        r#where: Option<Expression>,
    },
    Select {
        select: Vec<(Expression, Option<String>)>,
        from: Vec<From>,
        r#where: Option<Expression>,
        group_by: Vec<Expression>,
        having: Option<Expression>,
        order: Vec<(Expression, Order)>,
        offset: Option<Expression>,
        limit: Option<Expression>,
    },
}

/// A FROM item: a table or join.
#[derive(Debug)]
pub enum From {
    Table { name: String, alias: Option<String> },
    Join { left: Box<From>, right: Box<From>, r#type: JoinType, predicate: Option<Expression> },
}

/// A CREATE TABLE column definition.
#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub unique: bool,
    pub index: bool,
    pub references: Option<String>,
}

/// JOIN types.
#[derive(Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

impl JoinType {
    // If true, the join is an outer join -- rows with no join match are emitted
    // with a NULL match.
    pub fn is_outer(&self) -> bool {
        match self {
            Self::Left | Self::Right => true,
            Self::Cross | Self::Inner => false,
        }
    }
}

/// Sort orders.
#[derive(Debug)]
pub enum Order {
    Ascending,
    Descending,
}

/// Expressions. Can be nested.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Expression {
    /// A field reference, with an optional table qualifier.
    Field(Option<String>, String),
    /// A literal value.
    Literal(Literal),
    /// A function call (name and parameters).
    Function(String, Vec<Expression>),
    /// An operator.
    Operator(Operator),
}

/// Expression literals.
#[derive(Clone, Debug)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

/// To allow Expressions and Literals in e.g. hashmap lookups, implement simple
/// equality and hash for all types, including Null and f64::NAN. This is not
/// used for expression evaluation (handled by sql::types::Expression), where
/// these values should not be considered equal, only in lookups.
impl std::cmp::PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Boolean(l), Self::Boolean(r)) => l == r,
            (Self::Integer(l), Self::Integer(r)) => l == r,
            // Consider e.g. NaN equal to NaN for comparison purposes.
            (Self::Float(l), Self::Float(r)) => l.to_bits() == r.to_bits(),
            (Self::String(l), Self::String(r)) => l == r,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl std::cmp::Eq for Literal {}

impl std::hash::Hash for Literal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Boolean(v) => v.hash(state),
            Self::Integer(v) => v.hash(state),
            Self::Float(v) => v.to_be_bytes().hash(state),
            Self::String(v) => v.hash(state),
        }
    }
}

/// Expression operators.
///
/// Since this is a recursive data structure, we have to box each child
/// expression, which incurs a heap allocation. There are clever ways to get
/// around this, but we keep it simple.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Operator {
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Or(Box<Expression>, Box<Expression>),

    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    GreaterThanOrEqual(Box<Expression>, Box<Expression>),
    IsNaN(Box<Expression>),
    IsNull(Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    LessThanOrEqual(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),

    Add(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Identity(Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),

    Like(Box<Expression>, Box<Expression>),
}

impl Expression {
    /// Walks the expression tree depth-first, calling a closure for every node.
    /// Halts and returns false if the closure returns false.
    pub fn walk(&self, visitor: &mut impl FnMut(&Expression) -> bool) -> bool {
        use Operator::*;
        if !visitor(self) {
            return false;
        }
        match self {
            Self::Operator(Add(lhs, rhs))
            | Self::Operator(And(lhs, rhs))
            | Self::Operator(Divide(lhs, rhs))
            | Self::Operator(Equal(lhs, rhs))
            | Self::Operator(Exponentiate(lhs, rhs))
            | Self::Operator(GreaterThan(lhs, rhs))
            | Self::Operator(GreaterThanOrEqual(lhs, rhs))
            | Self::Operator(LessThan(lhs, rhs))
            | Self::Operator(LessThanOrEqual(lhs, rhs))
            | Self::Operator(Like(lhs, rhs))
            | Self::Operator(Modulo(lhs, rhs))
            | Self::Operator(Multiply(lhs, rhs))
            | Self::Operator(NotEqual(lhs, rhs))
            | Self::Operator(Or(lhs, rhs))
            | Self::Operator(Subtract(lhs, rhs)) => lhs.walk(visitor) && rhs.walk(visitor),

            Self::Operator(Factorial(expr))
            | Self::Operator(Identity(expr))
            | Self::Operator(IsNaN(expr))
            | Self::Operator(IsNull(expr))
            | Self::Operator(Negate(expr))
            | Self::Operator(Not(expr)) => expr.walk(visitor),

            Self::Function(_, exprs) => exprs.iter().any(|expr| expr.walk(visitor)),

            Self::Literal(_) | Self::Field(_, _) => true,
        }
    }

    /// Walks the expression tree depth-first while calling a closure until it
    /// returns true. This is the inverse of walk().
    pub fn contains(&self, visitor: &impl Fn(&Expression) -> bool) -> bool {
        !self.walk(&mut |expr| !visitor(expr))
    }

    /// Find and collects expressions for which the given closure returns true,
    /// adding them to c. Does not recurse into matching expressions.
    pub fn collect(&self, visitor: &impl Fn(&Expression) -> bool, c: &mut Vec<Expression>) {
        if visitor(self) {
            c.push(self.clone());
            return;
        }
        use Operator::*;
        match self {
            Self::Operator(Add(lhs, rhs))
            | Self::Operator(And(lhs, rhs))
            | Self::Operator(Divide(lhs, rhs))
            | Self::Operator(Equal(lhs, rhs))
            | Self::Operator(Exponentiate(lhs, rhs))
            | Self::Operator(GreaterThan(lhs, rhs))
            | Self::Operator(GreaterThanOrEqual(lhs, rhs))
            | Self::Operator(LessThan(lhs, rhs))
            | Self::Operator(LessThanOrEqual(lhs, rhs))
            | Self::Operator(Like(lhs, rhs))
            | Self::Operator(Modulo(lhs, rhs))
            | Self::Operator(Multiply(lhs, rhs))
            | Self::Operator(NotEqual(lhs, rhs))
            | Self::Operator(Or(lhs, rhs))
            | Self::Operator(Subtract(lhs, rhs)) => {
                lhs.collect(visitor, c);
                rhs.collect(visitor, c);
            }

            Self::Operator(Factorial(expr))
            | Self::Operator(Identity(expr))
            | Self::Operator(IsNaN(expr))
            | Self::Operator(IsNull(expr))
            | Self::Operator(Negate(expr))
            | Self::Operator(Not(expr)) => expr.collect(visitor, c),

            Self::Function(_, exprs) => exprs.iter().for_each(|expr| expr.collect(visitor, c)),

            Self::Literal(_) | Self::Field(_, _) => {}
        }
    }
}

impl core::convert::From<Literal> for Expression {
    fn from(literal: Literal) -> Self {
        Self::Literal(literal)
    }
}

impl core::convert::From<Operator> for Expression {
    fn from(op: Operator) -> Self {
        Self::Operator(op)
    }
}

impl core::convert::From<Operator> for Box<Expression> {
    fn from(value: Operator) -> Self {
        Box::new(value.into())
    }
}
