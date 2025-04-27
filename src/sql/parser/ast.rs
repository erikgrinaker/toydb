use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use crate::sql::types::DataType;

/// SQL statements are represented as an Abstract Syntax Tree (AST). The
/// statement is the root node of this tree, and describes the syntactic
/// structure of a SQL statement. It is built from a raw SQL string by the
/// parser, and passed on to the planner which validates it and builds an
/// execution plan from it.
#[derive(Debug)]
pub enum Statement {
    /// BEGIN: begins a new transaction.
    Begin {
        /// READ ONLY: if true, begin a read-only transaction.
        read_only: bool,
        /// AS OF: if given, the MVCC version to read at.
        as_of: Option<u64>,
    },
    /// COMMIT: commits a transaction.
    Commit,
    /// ROLLBACK: rolls back a transaction.
    Rollback,
    /// EXPLAIN: explains a SQL statement's execution plan.
    Explain(Box<Statement>),
    /// CREATE TABLE: creates a new table.
    CreateTable {
        /// The table name.
        name: String,
        /// Column specifications.
        columns: Vec<Column>,
    },
    /// DROP TABLE: drops a table.
    DropTable {
        /// The table to drop.
        name: String,
        /// IF EXISTS: if true, don't error if the table doesn't exist.
        if_exists: bool,
    },
    /// DELETE: deletes rows from a table.
    Delete {
        /// The table to delete from.
        table: String,
        /// WHERE: optional condition to match rows to delete.
        r#where: Option<Expression>,
    },
    /// INSERT INTO: inserts new rows into a table.
    Insert {
        /// Table to insert into.
        table: String,
        /// Columns to insert values into. If None, all columns are used.
        columns: Option<Vec<String>>,
        /// Row values to insert.
        values: Vec<Vec<Expression>>,
    },
    /// UPDATE: updates rows in a table.
    Update {
        table: String,
        set: BTreeMap<String, Option<Expression>>, // column → value, None for default value
        r#where: Option<Expression>,
    },
    /// SELECT: selects rows, possibly from a table.
    Select {
        /// Expressions to select, with an optional column alias.
        select: Vec<(Expression, Option<String>)>,
        /// FROM: tables to select from.
        from: Vec<From>,
        /// WHERE: optional condition to filter rows.
        r#where: Option<Expression>,
        /// GROUP BY: expressions to group and aggregate by.
        group_by: Vec<Expression>,
        /// HAVING: expression to filter groups by.
        having: Option<Expression>,
        /// ORDER BY: expresisions to sort by, with direction.
        order_by: Vec<(Expression, Direction)>,
        /// OFFSET: row offset to start from.
        offset: Option<Expression>,
        /// LIMIT: maximum number of rows to return.
        limit: Option<Expression>,
    },
}

/// A FROM item.
#[derive(Debug)]
pub enum From {
    /// A table.
    Table {
        /// The table name.
        name: String,
        /// An optional alias for the table.
        alias: Option<String>,
    },
    /// A join of two or more tables (may be nested).
    Join {
        /// The left table to join,
        left: Box<From>,
        /// The right table to join.
        right: Box<From>,
        /// The join type.
        r#type: JoinType,
        /// The join condition. None for a cross join.
        predicate: Option<Expression>,
    },
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
    // If true, the join is an outer join, where rows with no join matches are
    // emitted with a NULL match.
    pub fn is_outer(&self) -> bool {
        match self {
            Self::Left | Self::Right => true,
            Self::Cross | Self::Inner => false,
        }
    }
}

/// ORDER BY direction.
#[derive(Debug, Default)]
pub enum Direction {
    #[default]
    Ascending,
    Descending,
}

/// SQL expressions, e.g. `a + 7 > b`. Can be nested.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Expression {
    /// All columns, i.e. *.
    All,
    /// A column reference, optionally qualified with a table name.
    Column(Option<String>, String),
    /// A literal value.
    Literal(Literal),
    /// A function call (name and parameters).
    Function(String, Vec<Expression>),
    /// An operator.
    Operator(Operator),
}

/// Expression literal values.
#[derive(Clone, Debug)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

/// To allow using expressions and literals in e.g. hashmaps, implement simple
/// equality by value for all types, including Null and f64::NAN. This only
/// checks that the values are the same, and ignores SQL semantics for e.g. NULL
/// and NaN (which is handled by SQL expression evaluation).
impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean(l), Self::Boolean(r)) => l == r,
            (Self::Integer(l), Self::Integer(r)) => l == r,
            (Self::Float(l), Self::Float(r)) => l.to_bits() == r.to_bits(),
            (Self::String(l), Self::String(r)) => l == r,
            (_, _) => false,
        }
    }
}

impl Eq for Literal {}

impl Hash for Literal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Boolean(v) => v.hash(state),
            Self::Integer(v) => v.hash(state),
            Self::Float(v) => v.to_bits().hash(state),
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
    And(Box<Expression>, Box<Expression>), // a AND b
    Not(Box<Expression>),                  // NOT a
    Or(Box<Expression>, Box<Expression>),  // a OR b

    Equal(Box<Expression>, Box<Expression>),       // a = b
    GreaterThan(Box<Expression>, Box<Expression>), // a > b
    GreaterThanOrEqual(Box<Expression>, Box<Expression>), // a >= b
    Is(Box<Expression>, Literal),                  // IS NULL or IS NAN
    LessThan(Box<Expression>, Box<Expression>),    // a < b
    LessThanOrEqual(Box<Expression>, Box<Expression>), // a <= b
    NotEqual(Box<Expression>, Box<Expression>),    // a != b

    Add(Box<Expression>, Box<Expression>),          // a + b
    Divide(Box<Expression>, Box<Expression>),       // a / b
    Exponentiate(Box<Expression>, Box<Expression>), // a ^ b
    Factorial(Box<Expression>),                     // a!
    Identity(Box<Expression>),                      // +a
    Multiply(Box<Expression>, Box<Expression>),     // a * b
    Negate(Box<Expression>),                        // -a
    Remainder(Box<Expression>, Box<Expression>),    // a % b
    Subtract(Box<Expression>, Box<Expression>),     // a - b

    Like(Box<Expression>, Box<Expression>), // a LIKE b
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
            Self::Operator(op) => match op {
                Add(lhs, rhs)
                | And(lhs, rhs)
                | Divide(lhs, rhs)
                | Equal(lhs, rhs)
                | Exponentiate(lhs, rhs)
                | GreaterThan(lhs, rhs)
                | GreaterThanOrEqual(lhs, rhs)
                | LessThan(lhs, rhs)
                | LessThanOrEqual(lhs, rhs)
                | Like(lhs, rhs)
                | Multiply(lhs, rhs)
                | NotEqual(lhs, rhs)
                | Or(lhs, rhs)
                | Remainder(lhs, rhs)
                | Subtract(lhs, rhs) => lhs.walk(visitor) && rhs.walk(visitor),

                Factorial(expr) | Identity(expr) | Is(expr, _) | Negate(expr) | Not(expr) => {
                    expr.walk(visitor)
                }
            },

            Self::Function(_, exprs) => exprs.iter().any(|expr| expr.walk(visitor)),

            Self::All | Self::Column(_, _) | Self::Literal(_) => true,
        }
    }

    /// Walks the expression tree depth-first while calling a closure until it
    /// returns true. This is the inverse of walk().
    pub fn contains(&self, visitor: &impl Fn(&Expression) -> bool) -> bool {
        !self.walk(&mut |expr| !visitor(expr))
    }

    /// Find and collects expressions for which the given closure returns true,
    /// adding them to c. Does not recurse into matching expressions.
    pub fn collect(&self, visitor: &impl Fn(&Expression) -> bool, exprs: &mut Vec<Expression>) {
        use Operator::*;

        if visitor(self) {
            exprs.push(self.clone());
            return;
        }

        match self {
            Self::Operator(op) => match op {
                Add(lhs, rhs)
                | And(lhs, rhs)
                | Divide(lhs, rhs)
                | Equal(lhs, rhs)
                | Exponentiate(lhs, rhs)
                | GreaterThan(lhs, rhs)
                | GreaterThanOrEqual(lhs, rhs)
                | LessThan(lhs, rhs)
                | LessThanOrEqual(lhs, rhs)
                | Like(lhs, rhs)
                | Multiply(lhs, rhs)
                | NotEqual(lhs, rhs)
                | Or(lhs, rhs)
                | Remainder(lhs, rhs)
                | Subtract(lhs, rhs) => {
                    lhs.collect(visitor, exprs);
                    rhs.collect(visitor, exprs);
                }
                Factorial(expr) | Identity(expr) | Is(expr, _) | Negate(expr) | Not(expr) => {
                    expr.collect(visitor, exprs);
                }
            },

            Self::Function(_, args) => args.iter().for_each(|arg| arg.collect(visitor, exprs)),

            Self::All | Self::Column(_, _) | Self::Literal(_) => {}
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
