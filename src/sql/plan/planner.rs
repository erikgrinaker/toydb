use super::super::parser::ast;
use super::super::schema;
use super::super::types::{Environment, Expression, Expressions, Value};
use super::{Aggregate, Aggregates, Node, Plan};
use crate::Error;

/// The plan builder
pub struct Planner {}

impl Planner {
    /// Creates a new planner
    pub fn new() -> Self {
        Self {}
    }

    /// Builds a plan tree for an AST statement
    pub fn build(&self, statement: ast::Statement) -> Result<Plan, Error> {
        Ok(Plan(self.build_statement(statement)?))
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
                    source = Node::Filter {
                        source: Box::new(source),
                        predicate: self.build_expression(expr)?,
                    };
                }
                Node::Delete { table, source: Box::new(source) }
            }
            ast::Statement::DropTable(name) => Node::DropTable { name },
            ast::Statement::Explain(stmt) => Node::Explain(Box::new(self.build_statement(*stmt)?)),
            ast::Statement::Insert { table, columns, values } => Node::Insert {
                table,
                columns: columns.unwrap_or_else(Vec::new),
                expressions: values
                    .into_iter()
                    .map(|exprs| self.build_expressions(exprs))
                    .collect::<Result<_, Error>>()?,
            },
            ast::Statement::Select {
                select,
                from,
                r#where,
                group_by,
                having,
                order,
                limit,
                offset,
            } => {
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
                    n = Node::Filter {
                        source: Box::new(n),
                        predicate: self.build_expression(expr)?,
                    };
                };
                if !select.expressions.is_empty() {
                    let (mut pre, aggregates, mut post) =
                        self.build_aggregate_expressions(select.expressions)?;
                    let mut pre_labels: Vec<Option<String>> =
                        std::iter::repeat(None).take(pre.len()).collect();
                    if let Some(ast::GroupByClause(exprs)) = group_by {
                        for group in self.build_expressions(exprs)? {
                            if let Expression::Field(None, label) = &group {
                                // If the group is a field reference to an AS label in the output,
                                // move the SELECT expression to the pre-projection and reference it
                                // in post unless it contains an aggregate. This handles e.g.:
                                // SELECT a * 2 AS p, SUM(b) FROM t GROUP BY p
                                if let Some(index) =
                                    select.labels.iter().position(|l| l.as_deref() == Some(label))
                                {
                                    if !post[index].walk(&|expr| match expr {
                                        Expression::Column(i) if i < &aggregates.len() => false,
                                        _ => true,
                                    }) {
                                        return Err(Error::Value(
                                            "cannot group by aggregate column".into(),
                                        ));
                                    }
                                    pre.push(std::mem::replace(
                                        &mut post[index],
                                        Expression::Column(pre.len()),
                                    ));
                                    pre_labels.push(Some(label.clone()));
                                } else {
                                    pre.push(group);
                                    pre_labels.push(None);
                                }
                            } else {
                                // If the group expression is exactly equal as a SELECT expression,
                                // push them both to the pre-projection.
                                let mut pushed = false;
                                while let Some(index) = post.iter().position(|e| e == &group) {
                                    pre.push(std::mem::replace(
                                        &mut post[index],
                                        Expression::Column(pre.len()),
                                    ));
                                    pre_labels.push(None);
                                    pushed = true;
                                }
                                if !pushed {
                                    pre.push(group);
                                    pre_labels.push(None);
                                }
                            }
                        }
                    }
                    if !pre.is_empty() {
                        n = Node::Projection {
                            source: Box::new(n),
                            labels: pre_labels,
                            expressions: pre,
                        };
                    }
                    if !aggregates.is_empty() {
                        n = Node::Aggregation { source: Box::new(n), aggregates };
                    }
                    n = Node::Projection {
                        source: Box::new(n),
                        labels: select.labels,
                        expressions: post,
                    }
                };

                if let Some(ast::HavingClause(expr)) = having {
                    n = Node::Filter {
                        source: Box::new(n),
                        predicate: self.build_expression(expr)?,
                    };
                };

                // FIXME Because the projection doesn't retain original table values, we can't
                // order by fields which are not in the result set.
                if !order.is_empty() {
                    n = Node::Order {
                        source: Box::new(n),
                        orders: order
                            .into_iter()
                            .map(|(e, o)| self.build_expression(e).map(|e| (e, o.into())))
                            .collect::<Result<_, _>>()?,
                    };
                }
                if let Some(expr) = offset {
                    let expr = self.build_expression(expr)?;
                    if !expr.is_constant() {
                        return Err(Error::Value("Offset must be constant value".into()));
                    }
                    n = Node::Offset {
                        source: Box::new(n),
                        offset: match expr.evaluate(&Environment::new())? {
                            Value::Integer(i) if i >= 0 => i as u64,
                            v => {
                                return Err(Error::Value(format!("Invalid value {} for offset", v)))
                            }
                        },
                    }
                }
                if let Some(expr) = limit {
                    let expr = self.build_expression(expr)?;
                    if !expr.is_constant() {
                        return Err(Error::Value("Limit must be constant value".into()));
                    }
                    n = Node::Limit {
                        source: Box::new(n),
                        limit: match expr.evaluate(&Environment::new())? {
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
                    source = Node::Filter {
                        source: Box::new(source),
                        predicate: self.build_expression(expr)?,
                    };
                }
                Node::Update {
                    table,
                    source: Box::new(source),
                    expressions: set
                        .into_iter()
                        .map(|(c, e)| self.build_expression(e).map(|e| (c, e)))
                        .collect::<Result<_, _>>()?,
                }
            }
        })
    }

    /// Builds FROM items
    fn build_from_item(&self, item: ast::FromItem) -> Result<Node, Error> {
        Ok(match item {
            ast::FromItem::Table { name, alias } => Node::Scan { table: name, alias },
            ast::FromItem::Join { left, right, r#type, predicate } => match r#type {
                ast::JoinType::Cross | ast::JoinType::Inner => Node::NestedLoopJoin {
                    outer: Box::new(self.build_from_item(*left)?),
                    inner: Box::new(self.build_from_item(*right)?),
                    predicate: predicate.map(|e| self.build_expression(e)).transpose()?,
                    pad: false,
                    flip: false,
                },
                ast::JoinType::Left => Node::NestedLoopJoin {
                    outer: Box::new(self.build_from_item(*left)?),
                    inner: Box::new(self.build_from_item(*right)?),
                    predicate: predicate.map(|e| self.build_expression(e)).transpose()?,
                    pad: true,
                    flip: false,
                },
                ast::JoinType::Right => Node::NestedLoopJoin {
                    outer: Box::new(self.build_from_item(*left)?),
                    inner: Box::new(self.build_from_item(*right)?),
                    predicate: predicate.map(|e| self.build_expression(e)).transpose()?,
                    pad: true,
                    flip: true,
                },
            },
        })
    }

    /// Builds an expression from an AST expression
    fn build_expression(&self, expr: ast::Expression) -> Result<Expression, Error> {
        ExpressionBuilder::build(expr)
    }

    /// Builds expressions from AST expressions
    fn build_expressions(&self, exprs: ast::Expressions) -> Result<Expressions, Error> {
        ExpressionBuilder::build_many(exprs)
    }

    /// Builds partitioned aggregate expressions from AST expressions
    fn build_aggregate_expressions(
        &self,
        exprs: ast::Expressions,
    ) -> Result<(Expressions, Vec<Aggregate>, Expressions), Error> {
        ExpressionBuilder::build_aggregates(exprs)
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
                        Some(expr.evaluate(&Environment::new())?)
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

/// Builds expressions.
struct ExpressionBuilder {
    agg_allow: bool,
    aggs: Vec<Aggregate>,
    agg_args: Expressions,
}

impl ExpressionBuilder {
    /// Builds a single expression.
    fn build(expr: ast::Expression) -> Result<Expression, Error> {
        Ok(*ExpressionBuilder { agg_allow: false, aggs: Vec::new(), agg_args: Vec::new() }
            .make(Box::new(expr))?)
    }

    /// Builds multiple expressions.
    fn build_many(exprs: ast::Expressions) -> Result<Expressions, Error> {
        exprs.into_iter().map(Self::build).collect()
    }

    /// Builds aggregate expressions as a pre-projection, aggregation, and post-projection. All
    /// non-constant, non-aggregate expressions are pushed to the pre-projection as grouping
    /// expressions (caller should validate against GROUP BY), following expressions referenced by
    /// the aggregates.
    fn build_aggregates(
        exprs: ast::Expressions,
    ) -> Result<(Expressions, Aggregates, Expressions), Error> {
        let mut builder =
            ExpressionBuilder { agg_allow: true, aggs: Vec::new(), agg_args: Vec::new() };

        let mut post = Vec::new();
        for expr in exprs.into_iter() {
            post.push(*builder.make(Box::new(expr))?);
        }
        let pre = builder.agg_args;
        let aggs = builder.aggs;

        Ok((pre, aggs, post))
    }

    /// Used internally, with more convenient signature.
    #[allow(clippy::boxed_local)]
    fn make(&mut self, expr: Box<ast::Expression>) -> Result<Box<Expression>, Error> {
        use Expression::*;
        Ok(Box::new(match *expr {
            ast::Expression::Literal(l) => Constant(match l {
                ast::Literal::Null => Value::Null,
                ast::Literal::Boolean(b) => Value::Boolean(b),
                ast::Literal::Integer(i) => Value::Integer(i),
                ast::Literal::Float(f) => Value::Float(f),
                ast::Literal::String(s) => Value::String(s),
            }),
            ast::Expression::Field(rel, name) => Field(rel, name),
            ast::Expression::Function(name, mut args) => match name.as_str() {
                "avg" | "count" | "max" | "min" | "sum" if args.len() == 1 => {
                    *self.make_aggregate(&name, Box::new(args.remove(0)))?
                }
                _ => {
                    return Err(Error::Value(format!(
                        "Unknown function {} with {} arguments",
                        name,
                        args.len()
                    )))
                }
            },
            ast::Expression::Operation(op) => match op {
                // Logical operators
                ast::Operation::And(lhs, rhs) => And(self.make(lhs)?, self.make(rhs)?),
                ast::Operation::Not(expr) => Not(self.make(expr)?),
                ast::Operation::Or(lhs, rhs) => Or(self.make(lhs)?, self.make(rhs)?),

                // Comparison operators
                ast::Operation::Equal(lhs, rhs) => Equal(self.make(lhs)?, self.make(rhs)?),
                ast::Operation::GreaterThan(lhs, rhs) => {
                    GreaterThan(self.make(lhs)?, self.make(rhs)?)
                }
                ast::Operation::GreaterThanOrEqual(lhs, rhs) => Or(
                    GreaterThan(self.make(lhs.clone())?, self.make(rhs.clone())?).into(),
                    Equal(self.make(lhs)?, self.make(rhs)?).into(),
                ),
                ast::Operation::IsNull(expr) => IsNull(self.make(expr)?),
                ast::Operation::LessThan(lhs, rhs) => LessThan(self.make(lhs)?, self.make(rhs)?),
                ast::Operation::LessThanOrEqual(lhs, rhs) => Or(
                    LessThan(self.make(lhs.clone())?, self.make(rhs.clone())?).into(),
                    Equal(self.make(lhs)?, self.make(rhs)?).into(),
                ),
                ast::Operation::Like(lhs, rhs) => Like(self.make(lhs)?, self.make(rhs)?),
                ast::Operation::NotEqual(lhs, rhs) => {
                    Not(Equal(self.make(lhs)?, self.make(rhs)?).into())
                }

                // Mathematical operators
                ast::Operation::Assert(expr) => Assert(self.make(expr)?),
                ast::Operation::Add(lhs, rhs) => Add(self.make(lhs)?, self.make(rhs)?),
                ast::Operation::Divide(lhs, rhs) => Divide(self.make(lhs)?, self.make(rhs)?),
                ast::Operation::Exponentiate(lhs, rhs) => {
                    Exponentiate(self.make(lhs)?, self.make(rhs)?)
                }
                ast::Operation::Factorial(expr) => Factorial(self.make(expr)?),
                ast::Operation::Modulo(lhs, rhs) => Modulo(self.make(lhs)?, self.make(rhs)?),
                ast::Operation::Multiply(lhs, rhs) => Multiply(self.make(lhs)?, self.make(rhs)?),
                ast::Operation::Negate(expr) => Negate(self.make(expr)?),
                ast::Operation::Subtract(lhs, rhs) => Subtract(self.make(lhs)?, self.make(rhs)?),
            },
        }))
    }

    fn make_aggregate(
        &mut self,
        name: &str,
        expr: Box<ast::Expression>,
    ) -> Result<Box<Expression>, Error> {
        if !self.agg_allow {
            return Err(Error::Value(format!(
                "Aggregate function {}() not allowed here",
                name.to_uppercase()
            )));
        }
        self.aggs.push(match name {
            "avg" => Aggregate::Average,
            "count" => Aggregate::Count,
            "max" => Aggregate::Max,
            "min" => Aggregate::Min,
            "sum" => Aggregate::Sum,
            _ => {
                return Err(Error::Internal(format!(
                    "Invalid aggregate function {}()",
                    name.to_uppercase()
                )))
            }
        });
        self.agg_allow = false;
        let arg = *self.make(expr)?;
        self.agg_args.push(arg);
        self.agg_allow = true;
        Ok(Box::new(Expression::Column(self.aggs.len() - 1)))
    }
}
