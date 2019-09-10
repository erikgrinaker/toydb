use super::super::types;

/// Statements
#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    /// A CREATE TABLE statement
    CreateTable { name: String, columns: Vec<ColumnSpec> },
    /// A DELETE statement
    Delete { table: String, r#where: Option<WhereClause> },
    /// A DROP TABLE statement
    DropTable(String),
    /// An INSERT statement
    Insert { table: String, columns: Option<Vec<String>>, values: Vec<Expressions> },
    /// A SELECT statement
    Select { select: SelectClause, from: Option<FromClause>, r#where: Option<WhereClause> },
}

/// A column specification
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnSpec {
    pub name: String,
    pub datatype: types::DataType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
}

/// A SELECT clause
#[derive(Clone, Debug, PartialEq)]
pub struct SelectClause {
    /// The expressions to select. Empty list means everything, i.e. *.
    pub expressions: Vec<Expression>,
    /// The expression labels, if any
    pub labels: Vec<Option<String>>,
}

/// A FROM clause
#[derive(Clone, Debug, PartialEq)]
pub struct FromClause {
    pub tables: Vec<String>,
}

/// A WHERE clause
#[derive(Clone, Debug, PartialEq)]
pub struct WhereClause(pub Expression);

/// Expressions
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Field(String),
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
    CompareEQ(Box<Expression>, Box<Expression>),
    CompareGT(Box<Expression>, Box<Expression>),
    CompareGTE(Box<Expression>, Box<Expression>),
    CompareLT(Box<Expression>, Box<Expression>),
    CompareLTE(Box<Expression>, Box<Expression>),
    CompareNE(Box<Expression>, Box<Expression>),

    // Mathematical operators
    Add(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
}
