mod plan;

pub use plan::{Direction, Node, Plan};

use super::parser::ast;
use super::types::expression::Expression;
use super::types::schema;
use super::types::Value;
use crate::Error;

use std::collections::HashMap;

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
                let mut source = Node::Scan { table: table.clone(), alias: None };
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
                    Some(from) => {
                        let mut items = from.items.into_iter().rev();
                        let mut node = match items.next() {
                            Some(item) => self.build_from_item(item)?,
                            None => return Err(Error::Value("No from items given".into())),
                        };
                        for item in items {
                            node = Node::NestedLoopJoin {
                                outer: Box::new(self.build_from_item(item)?),
                                inner: Box::new(node),
                                predicate: None,
                                pad: false,
                                flip: false,
                            }
                        }
                        node
                    }
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
                if let Some(expr) = offset {
                    let expr = Expression::from(expr);
                    if !expr.is_constant() {
                        return Err(Error::Value("Offset must be constant value".into()));
                    }
                    n = Node::Offset {
                        source: Box::new(n),
                        offset: match expr.evaluate(&HashMap::new())? {
                            Value::Integer(i) if i >= 0 => i as u64,
                            v => {
                                return Err(Error::Value(format!("Invalid value {} for offset", v)))
                            }
                        },
                    }
                }
                if let Some(expr) = limit {
                    let expr = Expression::from(expr);
                    if !expr.is_constant() {
                        return Err(Error::Value("Limit must be constant value".into()));
                    }
                    n = Node::Limit {
                        source: Box::new(n),
                        limit: match expr.evaluate(&HashMap::new())? {
                            Value::Integer(i) if i >= 0 => i as u64,
                            v => {
                                return Err(Error::Value(format!("Invalid value {} for limit", v)))
                            }
                        },
                    }
                }
                n
            }
            ast::Statement::Update { table, set, r#where } => {
                let mut source = Node::Scan { table: table.clone(), alias: None };
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

    /// Builds FROM items
    fn build_from_item(&self, item: ast::FromItem) -> Result<Node, Error> {
        Ok(match item {
            ast::FromItem::Table { name, alias } => Node::Scan { table: name, alias },
            ast::FromItem::Join { left, right, r#type, predicate } => match r#type {
                ast::JoinType::Cross | ast::JoinType::Inner => Node::NestedLoopJoin {
                    outer: Box::new(self.build_from_item(*left)?),
                    inner: Box::new(self.build_from_item(*right)?),
                    predicate: predicate.map(|e| e.into()),
                    pad: false,
                    flip: false,
                },
                ast::JoinType::Left => Node::NestedLoopJoin {
                    outer: Box::new(self.build_from_item(*left)?),
                    inner: Box::new(self.build_from_item(*right)?),
                    predicate: predicate.map(|e| e.into()),
                    pad: true,
                    flip: false,
                },
                ast::JoinType::Right => Node::NestedLoopJoin {
                    outer: Box::new(self.build_from_item(*left)?),
                    inner: Box::new(self.build_from_item(*right)?),
                    predicate: predicate.map(|e| e.into()),
                    pad: true,
                    flip: true,
                },
            },
        })
    }

    /// Builds a table schema from an AST CreateTable node
    fn build_schema_table(
        &self,
        name: String,
        columns: Vec<ast::ColumnSpec>,
    ) -> Result<schema::Table, Error> {
        schema::Table::new(
            name,
            columns
                .into_iter()
                .map(|c| {
                    let nullable = c.nullable.unwrap_or(!c.primary_key);
                    let default = if let Some(expr) = c.default {
                        let expr = self.build_expression(expr)?;
                        if !expr.is_constant() {
                            return Err(Error::Value(format!(
                                "Default expression for column {} must be constant",
                                c.name
                            )));
                        }
                        Some(expr.evaluate(&HashMap::new())?)
                    } else if nullable {
                        Some(Value::Null)
                    } else {
                        None
                    };

                    Ok(schema::Column {
                        name: c.name,
                        datatype: c.datatype,
                        primary_key: c.primary_key,
                        nullable,
                        default,
                        unique: c.unique || c.primary_key,
                        references: c.references,
                    })
                })
                .collect::<Result<_, Error>>()?,
        )
    }
}

/// Helpers to convert AST nodes into plan nodes
impl From<ast::Expression> for Expression {
    fn from(expr: ast::Expression) -> Self {
        match expr {
            ast::Expression::Literal(l) => Expression::Constant(l.into()),
            ast::Expression::Field(rel, name) => Expression::Field(rel, name),
            ast::Expression::Operation(op) => match op {
                // Logical operators
                ast::Operation::And(lhs, rhs) => Self::And(lhs.into(), rhs.into()),
                ast::Operation::Not(expr) => Self::Not(expr.into()),
                ast::Operation::Or(lhs, rhs) => Self::Or(lhs.into(), rhs.into()),

                // Comparison operators
                ast::Operation::Equal(lhs, rhs) => Self::Equal(lhs.into(), rhs.into()),
                ast::Operation::GreaterThan(lhs, rhs) => Self::GreaterThan(lhs.into(), rhs.into()),
                ast::Operation::GreaterThanOrEqual(lhs, rhs) => Self::Or(
                    Self::GreaterThan(lhs.clone().into(), rhs.clone().into()).into(),
                    Self::Equal(lhs.into(), rhs.into()).into(),
                ),
                ast::Operation::IsNull(expr) => Self::IsNull(expr.into()),
                ast::Operation::LessThan(lhs, rhs) => Self::LessThan(lhs.into(), rhs.into()),
                ast::Operation::LessThanOrEqual(lhs, rhs) => Self::Or(
                    Self::LessThan(lhs.clone().into(), rhs.clone().into()).into(),
                    Self::Equal(lhs.into(), rhs.into()).into(),
                ),
                ast::Operation::Like(lhs, rhs) => Self::Like(lhs.into(), rhs.into()),
                ast::Operation::NotEqual(lhs, rhs) => {
                    Self::Not(Self::Equal(lhs.into(), rhs.into()).into())
                }

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
