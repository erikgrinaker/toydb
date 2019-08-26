/// Statements
#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    /// A SELECT statement
    Select {
        /// The select clause
        select: SelectClause,
    },
}

/// A SELECT clause
#[derive(Clone, Debug, PartialEq)]
pub struct SelectClause {
    /// The expressions to select
    pub expressions: Vec<Expression>,
    /// The expression labels, if any
    pub labels: Vec<Option<String>>,
}

/// Expressions
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Literal(Literal),
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
    Add(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Equals(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    LesserThan(Box<Expression>, Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
}
