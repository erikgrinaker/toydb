use super::super::types::DataType;
use crate::error::Result;

use std::collections::BTreeMap;
use std::mem::replace;

/// Statements
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Statement {
    Begin {
        readonly: bool,
        version: Option<u64>,
    },
    Commit,
    Rollback,
    Explain(Box<Statement>),

    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    DropTable(String),

    Delete {
        table: String,
        r#where: Option<WhereClause>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Expressions>,
    },
    Update {
        table: String,
        set: BTreeMap<String, Expression>,
        r#where: Option<WhereClause>,
    },

    Select {
        select: SelectClause,
        from: Option<FromClause>,
        r#where: Option<WhereClause>,
        group_by: Option<GroupByClause>,
        having: Option<HavingClause>,
        order: Vec<(Expression, Order)>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
}

/// A column
#[derive(Clone, Debug, PartialEq)]
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

/// A SELECT clause
#[derive(Clone, Debug, PartialEq)]
pub struct SelectClause {
    pub expressions: Vec<(Expression, Option<String>)>,
}

/// A FROM clause
#[derive(Clone, Debug, PartialEq)]
pub struct FromClause {
    pub items: Vec<FromItem>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FromItem {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        r#type: JoinType,
        predicate: Option<Expression>,
    },
}

/// A JOIN type
#[derive(Clone, Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

/// A WHERE clause
#[derive(Clone, Debug, PartialEq)]
pub struct WhereClause(pub Expression);

/// A GROUP BY clause
#[derive(Clone, Debug, PartialEq)]
pub struct GroupByClause(pub Vec<Expression>);

/// A HAVING clause
#[derive(Clone, Debug, PartialEq)]
pub struct HavingClause(pub Expression);

/// Sort orders
#[derive(Clone, Debug, PartialEq)]
pub enum Order {
    Ascending,
    Descending,
}

/// Expressions
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Field(Option<String>, String),
    Column(usize), // only used during plan building to break off expression subtrees
    Literal(Literal),
    Function(String, Expressions),
    Operation(Operation),
}

impl From<Literal> for Expression {
    fn from(literal: Literal) -> Self {
        Self::Literal(literal)
    }
}

impl From<Operation> for Expression {
    fn from(op: Operation) -> Self {
        Self::Operation(op)
    }
}

pub type Expressions = Vec<Expression>;

/// Literals
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

/// Operations (done by operators)
#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    // Logical operators
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Or(Box<Expression>, Box<Expression>),

    // Comparison operators
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    GreaterThanOrEqual(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    LessThanOrEqual(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),

    // Mathematical operators
    Add(Box<Expression>, Box<Expression>),
    Assert(Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),

    // String operators
    Like(Box<Expression>, Box<Expression>),
}

impl Expression {
    /// Walks the expression tree while calling a closure. Returns true as soon as the closure
    /// returns true. This is the inverse of walk().
    pub fn contains<F: Fn(&Expression) -> bool>(&self, visitor: &F) -> bool {
        !self.walk(&|e| !visitor(e))
    }

    /// Replaces the expression with result of the closure. Mosty helper function for transform().
    pub fn replace_with<F: FnMut(Self) -> Result<Self>>(&mut self, mut f: F) -> Result<()> {
        // Temporarily replace expression with a null value, in case closure panics. May consider
        // replace_with crate if this hampers performance.
        let expr = replace(self, Expression::Literal(Literal::Null));
        replace(self, f(expr)?);
        Ok(())
    }

    /// Transforms the expression tree by applying a closure before and after descending.
    pub fn transform<B, A>(mut self, before: &mut B, after: &mut A) -> Result<Self>
    where
        B: FnMut(Self) -> Result<Self>,
        A: FnMut(Self) -> Result<Self>,
    {
        use Operation::*;
        self = before(self)?;
        match &mut self {
            Self::Operation(Add(lhs, rhs))
            | Self::Operation(And(lhs, rhs))
            | Self::Operation(Divide(lhs, rhs))
            | Self::Operation(Equal(lhs, rhs))
            | Self::Operation(Exponentiate(lhs, rhs))
            | Self::Operation(GreaterThan(lhs, rhs))
            | Self::Operation(GreaterThanOrEqual(lhs, rhs))
            | Self::Operation(LessThan(lhs, rhs))
            | Self::Operation(LessThanOrEqual(lhs, rhs))
            | Self::Operation(Like(lhs, rhs))
            | Self::Operation(Modulo(lhs, rhs))
            | Self::Operation(Multiply(lhs, rhs))
            | Self::Operation(NotEqual(lhs, rhs))
            | Self::Operation(Or(lhs, rhs))
            | Self::Operation(Subtract(lhs, rhs)) => {
                Self::replace_with(lhs, |e| e.transform(before, after))?;
                Self::replace_with(rhs, |e| e.transform(before, after))?;
            }

            Self::Operation(Assert(expr))
            | Self::Operation(Factorial(expr))
            | Self::Operation(IsNull(expr))
            | Self::Operation(Negate(expr))
            | Self::Operation(Not(expr)) => {
                Self::replace_with(expr, |e| e.transform(before, after))?
            }

            Self::Function(_, exprs) => {
                for expr in exprs {
                    Self::replace_with(expr, |e| e.transform(before, after))?;
                }
            }

            Self::Literal(_) | Self::Field(_, _) | Self::Column(_) => {}
        };
        after(self)
    }

    /// Walks the expression tree, calling a closure for every node. Halts if closure returns false.
    pub fn walk<F: Fn(&Expression) -> bool>(&self, visitor: &F) -> bool {
        use Operation::*;
        visitor(self)
            && match self {
                Self::Operation(Add(lhs, rhs))
                | Self::Operation(And(lhs, rhs))
                | Self::Operation(Divide(lhs, rhs))
                | Self::Operation(Equal(lhs, rhs))
                | Self::Operation(Exponentiate(lhs, rhs))
                | Self::Operation(GreaterThan(lhs, rhs))
                | Self::Operation(GreaterThanOrEqual(lhs, rhs))
                | Self::Operation(LessThan(lhs, rhs))
                | Self::Operation(LessThanOrEqual(lhs, rhs))
                | Self::Operation(Like(lhs, rhs))
                | Self::Operation(Modulo(lhs, rhs))
                | Self::Operation(Multiply(lhs, rhs))
                | Self::Operation(NotEqual(lhs, rhs))
                | Self::Operation(Or(lhs, rhs))
                | Self::Operation(Subtract(lhs, rhs)) => lhs.walk(visitor) && rhs.walk(visitor),

                Self::Operation(Assert(expr))
                | Self::Operation(Factorial(expr))
                | Self::Operation(IsNull(expr))
                | Self::Operation(Negate(expr))
                | Self::Operation(Not(expr)) => expr.walk(visitor),

                Self::Function(_, exprs) => {
                    for expr in exprs {
                        if !expr.walk(visitor) {
                            return false;
                        }
                    }
                    true
                }

                Self::Literal(_) | Self::Field(_, _) | Self::Column(_) => true,
            }
    }
}
