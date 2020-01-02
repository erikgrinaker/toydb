mod plan;

pub use plan::{Node, Order, Plan};

use super::parser::ast;
use super::types::expression::{Environment, Expression};
use super::types::schema;
use super::types::Value;
use crate::Error;

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
            ast::Statement::Begin { .. } | ast::Statement::Commit | ast::Statement::Rollback => {
                return Err(Error::Internal(format!(
                    "Unexpected transaction statement {:?}",
                    statement
                )))
            }
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
                ast::Operation::CompareNE(lhs, rhs) => Self::CompareNE(lhs.into(), rhs.into()),
                ast::Operation::CompareGT(lhs, rhs) => Self::CompareGT(lhs.into(), rhs.into()),
                ast::Operation::CompareGTE(lhs, rhs) => Self::CompareGTE(lhs.into(), rhs.into()),
                ast::Operation::CompareLT(lhs, rhs) => Self::CompareLT(lhs.into(), rhs.into()),
                ast::Operation::CompareLTE(lhs, rhs) => Self::CompareLTE(lhs.into(), rhs.into()),
                ast::Operation::CompareNull(expr) => Self::CompareNull(expr.into()),

                // Mathematical operators
                ast::Operation::Assert(expr) => Self::Assert(expr.into()),
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
