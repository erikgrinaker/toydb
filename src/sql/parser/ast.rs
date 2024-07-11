use crate::error::Result;
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
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    /// A field reference, with an optional table qualifier.
    Field(Option<String>, String),
    /// A column index (only used during planning to break off subtrees).
    /// TODO: get rid of this, planning shouldn't modify the AST.
    Column(usize),
    /// A literal value.
    Literal(Literal),
    /// A function call (name and parameters).
    Function(String, Vec<Expression>),
    /// An operator.
    Operator(Operator),
}

/// Expression literals.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

/// Expression operators.
///
/// Since this is a recursive data structure, we have to box each child
/// expression, which incurs a heap allocation. There are clever ways to get
/// around this, but we keep it simple.
#[derive(Clone, Debug, PartialEq)]
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
    /// Transforms the expression tree depth-first by applying a closure before
    /// and after descending.
    ///
    /// TODO: make closures non-mut.
    pub fn transform<B, A>(mut self, before: &mut B, after: &mut A) -> Result<Self>
    where
        B: FnMut(Self) -> Result<Self>,
        A: FnMut(Self) -> Result<Self>,
    {
        use Operator::*;
        self = before(self)?;

        // Helper for transforming a boxed expression.
        let mut transform = |mut expr: Box<Expression>| -> Result<Box<Expression>> {
            *expr = expr.transform(before, after)?;
            Ok(expr)
        };

        self = match self {
            Self::Literal(_) | Self::Field(_, _) | Self::Column(_) => self,

            Self::Function(name, exprs) => Self::Function(
                name,
                exprs.into_iter().map(|e| e.transform(before, after)).collect::<Result<_>>()?,
            ),

            Self::Operator(op) => Self::Operator(match op {
                Add(lhs, rhs) => Add(transform(lhs)?, transform(rhs)?),
                And(lhs, rhs) => And(transform(lhs)?, transform(rhs)?),
                Divide(lhs, rhs) => Divide(transform(lhs)?, transform(rhs)?),
                Equal(lhs, rhs) => Equal(transform(lhs)?, transform(rhs)?),
                Exponentiate(lhs, rhs) => Exponentiate(transform(lhs)?, transform(rhs)?),
                Factorial(expr) => Factorial(transform(expr)?),
                GreaterThan(lhs, rhs) => GreaterThan(transform(lhs)?, transform(rhs)?),
                GreaterThanOrEqual(lhs, rhs) => {
                    GreaterThanOrEqual(transform(lhs)?, transform(rhs)?)
                }
                Identity(expr) => Identity(transform(expr)?),
                IsNaN(expr) => IsNaN(transform(expr)?),
                IsNull(expr) => IsNull(transform(expr)?),
                LessThan(lhs, rhs) => LessThan(transform(lhs)?, transform(rhs)?),
                LessThanOrEqual(lhs, rhs) => LessThanOrEqual(transform(lhs)?, transform(rhs)?),
                Like(lhs, rhs) => Like(transform(lhs)?, transform(rhs)?),
                Modulo(lhs, rhs) => Modulo(transform(lhs)?, transform(rhs)?),
                Multiply(lhs, rhs) => Multiply(transform(lhs)?, transform(rhs)?),
                Negate(expr) => Negate(transform(expr)?),
                Not(expr) => Not(transform(expr)?),
                NotEqual(lhs, rhs) => NotEqual(transform(lhs)?, transform(rhs)?),
                Or(lhs, rhs) => Or(transform(lhs)?, transform(rhs)?),
                Subtract(lhs, rhs) => Subtract(transform(lhs)?, transform(rhs)?),
            }),
        };
        self = after(self)?;
        Ok(self)
    }

    /// Transforms an expression using a mutable reference.
    /// TODO: try to get rid of this and replace_with().
    pub fn transform_mut<B, A>(&mut self, before: &mut B, after: &mut A) -> Result<()>
    where
        B: FnMut(Self) -> Result<Self>,
        A: FnMut(Self) -> Result<Self>,
    {
        self.replace_with(|e| e.transform(before, after))
    }

    /// Replaces the expression with result of the closure. Helper function for
    /// transform().
    fn replace_with(&mut self, mut f: impl FnMut(Self) -> Result<Self>) -> Result<()> {
        // Temporarily replace expression with a null value, in case closure panics. May consider
        // replace_with crate if this hampers performance.
        let expr = std::mem::replace(self, Expression::Literal(Literal::Null));
        *self = f(expr)?;
        Ok(())
    }

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

            Self::Literal(_) | Self::Field(_, _) | Self::Column(_) => true,
        }
    }

    /// Walks the expression tree depth-first while calling a closure until it
    /// returns true. This is the inverse of walk().
    pub fn contains(&self, visitor: &impl Fn(&Expression) -> bool) -> bool {
        !self.walk(&mut |expr| !visitor(expr))
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
