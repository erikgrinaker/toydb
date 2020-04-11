use super::super::types::DataType;

use std::collections::BTreeMap;

/// Statements
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Statement {
    /// A BEGIN statement
    Begin { readonly: bool, version: Option<u64> },
    /// A COMMIT statement
    Commit,
    /// A ROLLBACK statement
    Rollback,

    /// A CREATE TABLE statement
    CreateTable { name: String, columns: Vec<ColumnSpec> },
    /// A DROP TABLE statement
    DropTable(String),

    /// A DELETE statement
    Delete { table: String, r#where: Option<WhereClause> },
    /// An INSERT statement
    Insert { table: String, columns: Option<Vec<String>>, values: Vec<Expressions> },
    /// A SELECT statement
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
    /// An UPDATE statement
    Update { table: String, set: BTreeMap<String, Expression>, r#where: Option<WhereClause> },
}

/// A column specification
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnSpec {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub unique: bool,
    pub references: Option<String>,
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
