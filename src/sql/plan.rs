use super::executor::{Context, Executor};
use super::expression::{Environment, Expression, Expressions};
use super::optimizer;
use super::optimizer::Optimizer;
use super::parser::ast;
use super::schema;
use super::types::{ResultSet, Value};
use crate::Error;
use std::collections::BTreeMap;

/// A query plan
#[derive(Debug)]
pub struct Plan {
    root: Node,
}

impl Plan {
    /// Builds a plan from an AST statement
    pub fn build(statement: ast::Statement) -> Result<Self, Error> {
        Planner::new().build(statement)
    }

    /// Executes the plan, consuming it
    pub fn execute(self, mut ctx: Context) -> Result<ResultSet, Error> {
        Ok(ResultSet::from_executor(Executor::execute(&mut ctx, self.root)?))
    }

    /// Optimizes the plan, consuming it
    pub fn optimize(mut self) -> Result<Self, Error> {
        self.root = optimizer::ConstantFolder.optimize(self.root)?;
        Ok(self)
    }
}

/// A plan node
#[derive(Debug)]
pub enum Node {
    CreateTable { schema: schema::Table },
    Delete { table: String, source: Box<Self> },
    DropTable { name: String },
    Filter { source: Box<Self>, predicate: Expression },
    Insert { table: String, columns: Vec<String>, expressions: Vec<Expressions> },
    Limit { source: Box<Self>, limit: u64 },
    Nothing,
    Offset { source: Box<Self>, offset: u64 },
    Order { source: Box<Self>, orders: Vec<(Expression, Order)> },
    Projection { source: Box<Self>, labels: Vec<Option<String>>, expressions: Expressions },
    Scan { table: String },
    // Uses BTreeMap for test stability
    Update { table: String, source: Box<Self>, expressions: BTreeMap<String, Expression> },
}

impl Node {
    /// Recursively transforms nodes by applying functions before and after descending.
    pub fn transform<B, A>(mut self, pre: &B, post: &A) -> Result<Self, Error>
    where
        B: Fn(Self) -> Result<Self, Error>,
        A: Fn(Self) -> Result<Self, Error>,
    {
        self = pre(self)?;
        self = match self {
            n @ Self::CreateTable { .. } => n,
            n @ Self::DropTable { .. } => n,
            n @ Self::Insert { .. } => n,
            n @ Self::Nothing => n,
            n @ Self::Scan { .. } => n,
            Self::Delete { table, source } => {
                Self::Delete { table, source: source.transform(pre, post)?.into() }
            }
            Self::Filter { source, predicate } => {
                Self::Filter { source: source.transform(pre, post)?.into(), predicate }
            }
            Self::Limit { source, limit } => {
                Self::Limit { source: source.transform(pre, post)?.into(), limit }
            }
            Self::Offset { source, offset } => {
                Self::Offset { source: source.transform(pre, post)?.into(), offset }
            }
            Self::Order { source, orders } => {
                Self::Order { source: source.transform(pre, post)?.into(), orders }
            }
            Self::Projection { source, labels, expressions } => Self::Projection {
                source: source.transform(pre, post)?.into(),
                labels,
                expressions,
            },
            Self::Update { table, source, expressions } => {
                Self::Update { table, source: source.transform(pre, post)?.into(), expressions }
            }
        };
        post(self)
    }

    /// Transforms all expressions in a node by calling .transform() on them
    /// with the given functions.
    pub fn transform_expressions<B, A>(self, pre: &B, post: &A) -> Result<Self, Error>
    where
        B: Fn(Expression) -> Result<Expression, Error>,
        A: Fn(Expression) -> Result<Expression, Error>,
    {
        Ok(match self {
            n @ Self::CreateTable { .. } => n,
            n @ Self::Delete { .. } => n,
            n @ Self::DropTable { .. } => n,
            n @ Self::Limit { .. } => n,
            n @ Self::Nothing => n,
            n @ Self::Offset { .. } => n,
            n @ Self::Scan { .. } => n,
            Self::Filter { source, predicate } => {
                Self::Filter { source, predicate: predicate.transform(pre, post)? }
            }
            Self::Insert { table, columns, expressions } => Self::Insert {
                table,
                columns,
                expressions: expressions
                    .into_iter()
                    .map(|exprs| exprs.into_iter().map(|e| e.transform(pre, post)).collect())
                    .collect::<Result<_, Error>>()?,
            },
            Self::Order { source, orders } => Self::Order {
                source,
                orders: orders
                    .into_iter()
                    .map(|(e, o)| e.transform(pre, post).map(|e| (e, o)))
                    .collect::<Result<_, Error>>()?,
            },
            Self::Projection { source, labels, expressions } => Self::Projection {
                source,
                labels,
                expressions: expressions
                    .into_iter()
                    .map(|e| e.transform(pre, post))
                    .collect::<Result<_, Error>>()?,
            },
            Self::Update { table, source, expressions } => Self::Update {
                table,
                source,
                expressions: expressions
                    .into_iter()
                    .map(|(k, e)| e.transform(pre, post).map(|e| (k, e)))
                    .collect::<Result<_, Error>>()?,
            },
        })
    }
}

/// A sort order
#[derive(Debug, PartialEq)]
pub enum Order {
    Ascending,
    Descending,
}

impl From<ast::Order> for Order {
    fn from(o: ast::Order) -> Self {
        match o {
            ast::Order::Ascending => Self::Ascending,
            ast::Order::Descending => Self::Descending,
        }
    }
}

/// The plan builder
struct Planner {}

impl Planner {
    /// Creates a new planner
    fn new() -> Self {
        Self {}
    }

    /// Builds a plan tree for an AST statement
    fn build(&self, statement: ast::Statement) -> Result<Plan, Error> {
        Ok(Plan { root: self.build_statement(statement)? })
    }

    /// Builds a plan node for a statement
    fn build_statement(&self, statement: ast::Statement) -> Result<Node, Error> {
        Ok(match statement {
            ast::Statement::CreateTable { name, columns } => {
                Node::CreateTable { schema: self.build_schema_table(name, columns)? }
            }
            ast::Statement::Delete { table, r#where } => {
                let mut source = Node::Scan { table: table.clone() };
                if let Some(ast::WhereClause(expr)) = r#where {
                    source = Node::Filter { source: Box::new(source), predicate: expr.into() };
                }
                Node::Delete { table, source: Box::new(source) }
            }
            ast::Statement::DropTable(name) => Node::DropTable { name },
            ast::Statement::Insert { table, columns, values } => Node::Insert {
                table,
                columns: columns.unwrap_or_else(Vec::new),
                expressions: values
                    .into_iter()
                    .map(|exprs| exprs.into_iter().map(|expr| expr.into()).collect())
                    .collect(),
            },
            ast::Statement::Select { select, from, r#where, order, limit, offset } => {
                let mut n: Node = match from {
                    // FIXME Handle multiple FROM tables
                    Some(from) => Node::Scan { table: from.tables[0].clone() },
                    None if select.expressions.is_empty() => {
                        return Err(Error::Value("Can't select * without a table".into()))
                    }
                    None => Node::Nothing,
                };
                if let Some(ast::WhereClause(expr)) = r#where {
                    n = Node::Filter { source: Box::new(n), predicate: expr.into() };
                };
                if !select.expressions.is_empty() {
                    n = Node::Projection {
                        source: Box::new(n),
                        labels: select.labels,
                        expressions: self.build_expressions(select.expressions)?,
                    };
                };
                // FIXME Because the projection doesn't retain original table values, we can't
                // order by fields which are not in the result set.
                if !order.is_empty() {
                    n = Node::Order {
                        source: Box::new(n),
                        orders: order.into_iter().map(|(e, o)| (e.into(), o.into())).collect(),
                    };
                }
                // FIXME Limit and offset need to check that the expression is constant
                if let Some(expr) = offset {
                    n = Node::Offset {
                        source: Box::new(n),
                        offset: match Expression::from(expr).evaluate(&Environment::empty())? {
                            Value::Integer(i) => i as u64,
                            v => {
                                return Err(Error::Value(format!("Invalid value {} for offset", v)))
                            }
                        },
                    }
                }
                if let Some(expr) = limit {
                    n = Node::Limit {
                        source: Box::new(n),
                        limit: match Expression::from(expr).evaluate(&Environment::empty())? {
                            Value::Integer(i) => i as u64,
                            v => {
                                return Err(Error::Value(format!("Invalid value {} for limit", v)))
                            }
                        },
                    }
                }
                n
            }
            ast::Statement::Update { table, set, r#where } => {
                let mut source = Node::Scan { table: table.clone() };
                if let Some(ast::WhereClause(expr)) = r#where {
                    source = Node::Filter { source: Box::new(source), predicate: expr.into() };
                }
                Node::Update {
                    table,
                    source: Box::new(source),
                    expressions: set.into_iter().map(|(c, e)| (c, e.into())).collect(),
                }
            }
        })
    }

    /// Builds a plan expression from an AST expression
    fn build_expression(&self, expr: ast::Expression) -> Result<Expression, Error> {
        Ok(expr.into())
    }

    /// Builds an array of plan expressions from AST expressions
    fn build_expressions(&self, exprs: Vec<ast::Expression>) -> Result<Vec<Expression>, Error> {
        exprs.into_iter().map(|e| self.build_expression(e)).collect()
    }

    /// Builds a table schema from an AST CreateTable node
    fn build_schema_table(
        &self,
        name: String,
        cols: Vec<ast::ColumnSpec>,
    ) -> Result<schema::Table, Error> {
        match cols.iter().filter(|c| c.primary_key).count() {
            0 => return Err(Error::Value(format!("No primary key defined for table {}", name))),
            n if n > 1 => {
                return Err(Error::Value(format!(
                    "{} primary keys defined for table {}, must set exactly 1",
                    n, name
                )))
            }
            _ => {}
        };
        let table = schema::Table {
            name,
            primary_key: cols.iter().position(|c| c.primary_key).unwrap_or(0),
            columns: cols
                .into_iter()
                .map(|spec| schema::Column {
                    name: spec.name,
                    datatype: spec.datatype,
                    nullable: spec.nullable.unwrap_or(!spec.primary_key),
                })
                .collect(),
        };
        table.validate()?;
        Ok(table)
    }
}

/// Helpers to convert AST expressions into plan expressions
impl From<ast::Expression> for Expression {
    fn from(expr: ast::Expression) -> Self {
        match expr {
            ast::Expression::Literal(l) => Expression::Constant(l.into()),
            ast::Expression::Field(name) => Expression::Field(name),
            ast::Expression::Operation(op) => match op {
                // Logical operators
                ast::Operation::And(lhs, rhs) => Self::And(lhs.into(), rhs.into()),
                ast::Operation::Not(expr) => Self::Not(expr.into()),
                ast::Operation::Or(lhs, rhs) => Self::Or(lhs.into(), rhs.into()),

                // Comparison operators
                ast::Operation::CompareEQ(lhs, rhs) => Self::CompareEQ(lhs.into(), rhs.into()),
                ast::Operation::CompareGT(lhs, rhs) => Self::CompareGT(lhs.into(), rhs.into()),
                ast::Operation::CompareGTE(lhs, rhs) => Self::CompareGTE(lhs.into(), rhs.into()),
                ast::Operation::CompareLT(lhs, rhs) => Self::CompareLT(lhs.into(), rhs.into()),
                ast::Operation::CompareLTE(lhs, rhs) => Self::CompareLTE(lhs.into(), rhs.into()),
                ast::Operation::CompareNE(lhs, rhs) => Self::CompareNE(lhs.into(), rhs.into()),

                // Mathematical operators
                ast::Operation::Add(lhs, rhs) => Self::Add(lhs.into(), rhs.into()),
                ast::Operation::Divide(lhs, rhs) => Self::Divide(lhs.into(), rhs.into()),
                ast::Operation::Exponentiate(lhs, rhs) => {
                    Self::Exponentiate(lhs.into(), rhs.into())
                }
                ast::Operation::Factorial(expr) => Self::Factorial(expr.into()),
                ast::Operation::Modulo(lhs, rhs) => Self::Modulo(lhs.into(), rhs.into()),
                ast::Operation::Multiply(lhs, rhs) => Self::Multiply(lhs.into(), rhs.into()),
                ast::Operation::Negate(expr) => Self::Negate(expr.into()),
                ast::Operation::Subtract(lhs, rhs) => Self::Subtract(lhs.into(), rhs.into()),
            },
        }
    }
}

impl From<Box<ast::Expression>> for Box<Expression> {
    fn from(expr: Box<ast::Expression>) -> Self {
        Box::new((*expr).into())
    }
}

impl From<ast::Literal> for Value {
    fn from(literal: ast::Literal) -> Self {
        match literal {
            ast::Literal::Null => Value::Null,
            ast::Literal::Boolean(b) => b.into(),
            ast::Literal::Float(f) => f.into(),
            ast::Literal::Integer(i) => i.into(),
            ast::Literal::String(s) => s.into(),
        }
    }
}
