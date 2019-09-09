use super::executor::{Context, Executor};
use super::expression::{Expression, Expressions};
use super::parser::ast;
use super::schema;
use super::types::{ResultSet, Value};
use crate::Error;

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
}

/// A plan node
#[derive(Debug)]
pub enum Node {
    CreateTable { schema: schema::Table },
    DropTable { name: String },
    Filter { source: Box<Self>, predicate: Expression },
    Insert { table: String, columns: Vec<String>, expressions: Vec<Expressions> },
    Nothing,
    Projection { source: Box<Self>, labels: Vec<Option<String>>, expressions: Expressions },
    Scan { table: String },
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
            ast::Statement::DropTable(name) => Node::DropTable { name },
            ast::Statement::Insert { table, columns, values } => Node::Insert {
                table,
                columns: columns.unwrap_or_else(Vec::new),
                expressions: values
                    .into_iter()
                    .map(|exprs| exprs.into_iter().map(|expr| expr.into()).collect())
                    .collect(),
            },
            ast::Statement::Select { select, from, r#where } => {
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
                n
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
