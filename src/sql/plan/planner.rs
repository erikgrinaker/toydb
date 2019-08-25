use super::super::parser::ast;
use super::super::types::Value;
use super::expression::Expression;
use super::node::Node;
use super::Plan;
use crate::Error;

pub struct Planner {}

impl Planner {
    /// Creates a new planner
    pub fn new() -> Self {
        Self {}
    }

    /// Builds a plan tree for an AST statement
    pub fn build(&self, statement: ast::Statement) -> Result<Plan, Error> {
        let root = self.build_statement(statement)?;
        Ok(Plan {
            columns: match &root {
                Node::Projection { labels, .. } => labels.clone(),
                _ => panic!("Not implemented"),
            },
            root,
        })
    }

    /// Builds a plan node for a statement
    fn build_statement(&self, statement: ast::Statement) -> Result<Node, Error> {
        Ok(match statement {
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
}

/// Helpers to convert AST expressions into plan expressions
impl From<ast::Expression> for Expression {
    fn from(expr: ast::Expression) -> Self {
        match expr {
            ast::Expression::Literal(l) => Expression::Constant(l.into()),
            ast::Expression::Operation(op) => match op {
                ast::Operation::Add(lhs, rhs) => Self::Add(lhs.into(), rhs.into()),
                ast::Operation::Divide(lhs, rhs) => Self::Divide(lhs.into(), rhs.into()),
                ast::Operation::Exponentiate(lhs, rhs) => Self::Exponentiate(lhs.into(), rhs.into()),
                ast::Operation::Factorial(e) => Self::Factorial(e.into()),
                ast::Operation::Modulo(lhs, rhs) => Self::Modulo(lhs.into(), rhs.into()),
                ast::Operation::Multiply(lhs, rhs) => Self::Multiply(lhs.into(), rhs.into()),
                ast::Operation::Negate(e) => Self::Negate(e.into()),
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
