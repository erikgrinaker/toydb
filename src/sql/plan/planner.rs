use super::super::parser::ast;
use super::super::schema;
use super::super::storage::Storage;
use super::super::types::Value;
use super::expression::Expression;
use super::node::{Node, DDL};
use super::Plan;
use crate::Error;

pub struct Planner {
    storage: Box<Storage>,
}

impl Planner {
    /// Creates a new planner
    pub fn new(storage: Box<Storage>) -> Self {
        Self { storage }
    }

    /// Builds a plan tree for an AST statement
    pub fn build(&self, statement: ast::Statement) -> Result<Plan, Error> {
        let root = self.build_statement(statement)?;
        Ok(Plan {
            columns: match &root {
                Node::Projection { labels, .. } => labels.clone(),
                Node::DDL { .. } => vec![],
                _ => panic!("Not implemented"),
            },
            root,
        })
    }

    /// Builds a plan node for a statement
    fn build_statement(&self, statement: ast::Statement) -> Result<Node, Error> {
        Ok(match statement {
            ast::Statement::CreateTable { name, columns } => Node::DDL {
                storage: self.storage.clone(),
                ddl: DDL::CreateTable(self.build_schema_table(name, columns)?),
            },
            ast::Statement::DropTable(name) => {
                Node::DDL { storage: self.storage.clone(), ddl: DDL::DropTable(name) }
            }
            ast::Statement::Select { select } => Node::Projection {
                labels: select
                    .labels
                    .into_iter()
                    .map(|l| l.unwrap_or_else(|| "?".into()))
                    .collect(),
                expressions: self.build_expressions(select.expressions)?,
                source: Box::new(Node::Nothing { done: false }),
            },
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
        columnspecs: Vec<ast::ColumnSpec>,
    ) -> Result<schema::Table, Error> {
        let primary_keys: Vec<&ast::ColumnSpec> =
            columnspecs.iter().filter(|c| c.primary_key).collect();
        if primary_keys.is_empty() {
            return Err(Error::Value(format!("No primary key defined for table {}", name)));
        } else if primary_keys.len() > 1 {
            return Err(Error::Value(format!(
                "{} primary keys defined for table {}, must set exactly 1",
                primary_keys.len(),
                name
            )));
        }
        let primary_key = primary_keys[0];
        if let Some(true) = primary_key.nullable {
            return Err(Error::Value("Primary key cannot be nullable".into()));
        }

        Ok(schema::Table {
            name,
            primary_key: primary_key.name.clone(),
            columns: columnspecs
                .into_iter()
                .map(|spec| schema::Column {
                    name: spec.name,
                    datatype: spec.datatype,
                    nullable: spec.nullable.unwrap_or(!spec.primary_key),
                })
                .collect(),
        })
    }
}

/// Helpers to convert AST expressions into plan expressions
impl From<ast::Expression> for Expression {
    fn from(expr: ast::Expression) -> Self {
        match expr {
            ast::Expression::Literal(l) => Expression::Constant(l.into()),
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
