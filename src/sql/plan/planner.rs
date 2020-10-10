use super::super::parser::ast;
use super::super::schema::{Catalog, Column, Table};
use super::super::types::{Expression, Value};
use super::{Aggregate, Direction, Node, Plan};
use crate::error::{Error, Result};

use std::collections::{HashMap, HashSet};
use std::mem::replace;

/// A query plan builder.
pub struct Planner<'a, C: Catalog> {
    catalog: &'a mut C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    /// Creates a new planner.
    pub fn new(catalog: &'a mut C) -> Self {
        Self { catalog }
    }

    /// Builds a plan for an AST statement.
    pub fn build(&mut self, statement: ast::Statement) -> Result<Plan> {
        Ok(Plan(self.build_statement(statement)?))
    }

    /// Builds a plan node for a statement.
    fn build_statement(&self, statement: ast::Statement) -> Result<Node> {
        Ok(match statement {
            // Transaction control and explain statements should have been handled by session.
            ast::Statement::Begin { .. } | ast::Statement::Commit | ast::Statement::Rollback => {
                return Err(Error::Internal(format!(
                    "Unexpected transaction statement {:?}",
                    statement
                )))
            }

            ast::Statement::Explain(_) => {
                return Err(Error::Internal("Unexpected explain statement".into()))
            }

            // DDL statements (schema changes).
            ast::Statement::CreateTable { name, columns } => Node::CreateTable {
                schema: Table::new(
                    name,
                    columns
                        .into_iter()
                        .map(|c| {
                            let nullable = c.nullable.unwrap_or(!c.primary_key);
                            let default = match c.default {
                                Some(expr) => Some(self.evaluate_constant(expr)?),
                                None if nullable => Some(Value::Null),
                                None => None,
                            };
                            Ok(Column {
                                name: c.name,
                                datatype: c.datatype,
                                primary_key: c.primary_key,
                                nullable,
                                default,
                                index: c.index && !c.primary_key,
                                unique: c.unique || c.primary_key,
                                references: c.references,
                            })
                        })
                        .collect::<Result<_>>()?,
                )?,
            },

            ast::Statement::DropTable(table) => Node::DropTable { table },

            // DML statements (mutations).
            ast::Statement::Delete { table, r#where } => {
                let scope = &mut Scope::from_table(self.catalog.must_read_table(&table)?)?;
                Node::Delete {
                    table: table.clone(),
                    source: Box::new(Node::Scan {
                        table,
                        alias: None,
                        filter: r#where.map(|e| self.build_expression(scope, e)).transpose()?,
                    }),
                }
            }

            ast::Statement::Insert { table, columns, values } => Node::Insert {
                table,
                columns: columns.unwrap_or_else(Vec::new),
                expressions: values
                    .into_iter()
                    .map(|exprs| {
                        exprs
                            .into_iter()
                            .map(|expr| self.build_expression(&mut Scope::constant(), expr))
                            .collect::<Result<_>>()
                    })
                    .collect::<Result<_>>()?,
            },

            ast::Statement::Update { table, set, r#where } => {
                let scope = &mut Scope::from_table(self.catalog.must_read_table(&table)?)?;
                Node::Update {
                    table: table.clone(),
                    source: Box::new(Node::Scan {
                        table,
                        alias: None,
                        filter: r#where.map(|e| self.build_expression(scope, e)).transpose()?,
                    }),
                    expressions: set
                        .into_iter()
                        .map(|(c, e)| {
                            Ok((
                                scope.resolve(None, &c)?,
                                Some(c),
                                self.build_expression(scope, e)?,
                            ))
                        })
                        .collect::<Result<_>>()?,
                }
            }

            // Queries.
            ast::Statement::Select {
                mut select,
                from,
                r#where,
                group_by,
                mut having,
                mut order,
                offset,
                limit,
            } => {
                let scope = &mut Scope::new();

                // Build FROM clause.
                let mut node = if !from.is_empty() {
                    self.build_from_clause(scope, from)?
                } else if select.is_empty() {
                    return Err(Error::Value("Can't select * without a table".into()));
                } else {
                    Node::Nothing
                };

                // Build WHERE clause.
                if let Some(expr) = r#where {
                    node = Node::Filter {
                        source: Box::new(node),
                        predicate: self.build_expression(scope, expr)?,
                    };
                };

                // Build SELECT clause.
                let mut hidden = 0;
                if !select.is_empty() {
                    // Inject hidden SELECT columns for fields and aggregates used in ORDER BY and
                    // HAVING expressions but not present in existing SELECT output. These will be
                    // removed again by a later projection.
                    if let Some(ref mut expr) = having {
                        hidden += self.inject_hidden(expr, &mut select)?;
                    }
                    for (expr, _) in order.iter_mut() {
                        hidden += self.inject_hidden(expr, &mut select)?;
                    }

                    // Extract any aggregate functions and GROUP BY expressions, replacing them with
                    // Column placeholders. Aggregations are handled by evaluating group expressions
                    // and aggregate function arguments in a pre-projection, passing the results
                    // to an aggregation node, and then evaluating the final SELECT expressions
                    // in the post-projection. For example:
                    //
                    // SELECT (MAX(rating * 100) - MIN(rating * 100)) / 100
                    // FROM movies
                    // GROUP BY released - 2000
                    //
                    // Results in the following nodes:
                    //
                    // - Projection: rating * 100, rating * 100, released - 2000
                    // - Aggregation: max(#0), min(#1) group by #2
                    // - Projection: (#0 - #1) / 100
                    let aggregates = self.extract_aggregates(&mut select)?;
                    let groups = self.extract_groups(&mut select, group_by, aggregates.len())?;
                    if !aggregates.is_empty() || !groups.is_empty() {
                        node = self.build_aggregation(scope, node, groups, aggregates)?;
                    }

                    // Build the remaining non-aggregate projection.
                    let expressions: Vec<(Expression, Option<String>)> = select
                        .into_iter()
                        .map(|(e, l)| Ok((self.build_expression(scope, e)?, l)))
                        .collect::<Result<_>>()?;
                    scope.project(&expressions)?;
                    node = Node::Projection { source: Box::new(node), expressions };
                };

                // Build HAVING clause.
                if let Some(expr) = having {
                    node = Node::Filter {
                        source: Box::new(node),
                        predicate: self.build_expression(scope, expr)?,
                    };
                };

                // Build ORDER clause.
                if !order.is_empty() {
                    node = Node::Order {
                        source: Box::new(node),
                        orders: order
                            .into_iter()
                            .map(|(e, o)| {
                                Ok((
                                    self.build_expression(scope, e)?,
                                    match o {
                                        ast::Order::Ascending => Direction::Ascending,
                                        ast::Order::Descending => Direction::Descending,
                                    },
                                ))
                            })
                            .collect::<Result<_>>()?,
                    };
                }

                // Build OFFSET clause.
                if let Some(expr) = offset {
                    node = Node::Offset {
                        source: Box::new(node),
                        offset: match self.evaluate_constant(expr)? {
                            Value::Integer(i) if i >= 0 => Ok(i as u64),
                            v => Err(Error::Value(format!("Invalid offset {}", v))),
                        }?,
                    }
                }

                // Build LIMIT clause.
                if let Some(expr) = limit {
                    node = Node::Limit {
                        source: Box::new(node),
                        limit: match self.evaluate_constant(expr)? {
                            Value::Integer(i) if i >= 0 => Ok(i as u64),
                            v => Err(Error::Value(format!("Invalid limit {}", v))),
                        }?,
                    }
                }

                // Remove any hidden columns.
                if hidden > 0 {
                    node = Node::Projection {
                        source: Box::new(node),
                        expressions: (0..(scope.len() - hidden))
                            .map(|i| (Expression::Field(i, None), None))
                            .collect(),
                    }
                }

                node
            }
        })
    }

    /// Builds a FROM clause consisting of several items. Each item is either a single table or a
    /// join of an arbitrary number of tables. All of the items are joined, since e.g. 'SELECT * FROM
    /// a, b' is an implicit join of a and b.
    fn build_from_clause(&self, scope: &mut Scope, from: Vec<ast::FromItem>) -> Result<Node> {
        let base_scope = scope.clone();
        let mut items = from.into_iter();
        let mut node = match items.next() {
            Some(item) => self.build_from_item(scope, item)?,
            None => return Err(Error::Value("No from items given".into())),
        };
        for item in items {
            let mut right_scope = base_scope.clone();
            let right = self.build_from_item(&mut right_scope, item)?;
            node = Node::NestedLoopJoin {
                left: Box::new(node),
                left_size: scope.len(),
                right: Box::new(right),
                predicate: None,
                outer: false,
            };
            scope.merge(right_scope)?;
        }
        Ok(node)
    }

    /// Builds FROM items, which can either be a single table or a chained join of multiple tables,
    /// e.g. 'SELECT * FROM a LEFT JOIN b ON b.a_id = a.id'. Any tables will be stored in
    /// self.tables keyed by their query name (i.e. alias if given, otherwise name). The table can
    /// only be referenced by the query name (so if alias is given, cannot reference by name).
    fn build_from_item(&self, scope: &mut Scope, item: ast::FromItem) -> Result<Node> {
        Ok(match item {
            ast::FromItem::Table { name, alias } => {
                scope.add_table(
                    alias.clone().unwrap_or_else(|| name.clone()),
                    self.catalog.must_read_table(&name)?,
                )?;
                Node::Scan { table: name, alias, filter: None }
            }

            ast::FromItem::Join { left, right, r#type, predicate } => {
                // Right outer joins are built as a left outer join with an additional projection
                // to swap the resulting columns.
                let (left, right) = match r#type {
                    ast::JoinType::Right => (right, left),
                    _ => (left, right),
                };
                let left = Box::new(self.build_from_item(scope, *left)?);
                let left_size = scope.len();
                let right = Box::new(self.build_from_item(scope, *right)?);
                let predicate = predicate.map(|e| self.build_expression(scope, e)).transpose()?;
                let outer = match r#type {
                    ast::JoinType::Cross | ast::JoinType::Inner => false,
                    ast::JoinType::Left | ast::JoinType::Right => true,
                };
                let mut node = Node::NestedLoopJoin { left, left_size, right, predicate, outer };
                if matches!(r#type, ast::JoinType::Right) {
                    let expressions = (left_size..scope.len())
                        .chain(0..left_size)
                        .map(|i| Ok((Expression::Field(i, scope.get_label(i)?), None)))
                        .collect::<Result<Vec<_>>>()?;
                    scope.project(&expressions)?;
                    node = Node::Projection { source: Box::new(node), expressions }
                }
                node
            }
        })
    }

    /// Builds an aggregation node. All aggregate parameters and GROUP BY expressions are evaluated
    /// in a pre-projection, whose results are fed into an Aggregate node. This node computes the
    /// aggregates for the given groups, passing the group values through directly.
    fn build_aggregation(
        &self,
        scope: &mut Scope,
        source: Node,
        groups: Vec<(ast::Expression, Option<String>)>,
        aggregations: Vec<(Aggregate, ast::Expression)>,
    ) -> Result<Node> {
        let mut aggregates = Vec::new();
        let mut expressions = Vec::new();
        for (aggregate, expr) in aggregations {
            aggregates.push(aggregate);
            expressions.push((self.build_expression(scope, expr)?, None));
        }
        for (expr, label) in groups {
            expressions.push((self.build_expression(scope, expr)?, label));
        }
        scope.project(
            &expressions
                .iter()
                .cloned()
                .enumerate()
                .map(|(i, (e, l))| {
                    if i < aggregates.len() {
                        // We pass null values here since we don't want field references to hit
                        // the fields in scope before the aggregation.
                        (Expression::Constant(Value::Null), None)
                    } else {
                        (e, l)
                    }
                })
                .collect::<Vec<_>>(),
        )?;
        let node = Node::Aggregation {
            source: Box::new(Node::Projection { source: Box::new(source), expressions }),
            aggregates,
        };
        Ok(node)
    }

    /// Extracts aggregate functions from an AST expression tree. This finds the aggregate
    /// function calls, replaces them with ast::Expression::Column(i), maps the aggregate functions
    /// to aggregates, and returns them along with their argument expressions.
    fn extract_aggregates(
        &self,
        exprs: &mut [(ast::Expression, Option<String>)],
    ) -> Result<Vec<(Aggregate, ast::Expression)>> {
        let mut aggregates = Vec::new();
        for (expr, _) in exprs {
            expr.transform_mut(
                &mut |mut e| match &mut e {
                    ast::Expression::Function(f, args) if args.len() == 1 => {
                        if let Some(aggregate) = self.aggregate_from_name(f) {
                            aggregates.push((aggregate, args.remove(0)));
                            Ok(ast::Expression::Column(aggregates.len() - 1))
                        } else {
                            Ok(e)
                        }
                    }
                    _ => Ok(e),
                },
                &mut |e| Ok(e),
            )?;
        }
        for (_, expr) in &aggregates {
            if self.is_aggregate(expr) {
                return Err(Error::Value("Aggregate functions can't be nested".into()));
            }
        }
        Ok(aggregates)
    }

    /// Extracts group by expressions, and replaces them with column references with the given
    /// offset. These can be either an arbitray expression, a reference to a SELECT column, or the
    /// same expression as a SELECT column. The following are all valid:
    ///
    /// SELECT released / 100 AS century, COUNT(*) FROM movies GROUP BY century
    /// SELECT released / 100, COUNT(*) FROM movies GROUP BY released / 100
    /// SELECT COUNT(*) FROM movies GROUP BY released / 100
    fn extract_groups(
        &self,
        exprs: &mut Vec<(ast::Expression, Option<String>)>,
        group_by: Vec<ast::Expression>,
        offset: usize,
    ) -> Result<Vec<(ast::Expression, Option<String>)>> {
        let mut groups = Vec::new();
        for g in group_by {
            // Look for references to SELECT columns with AS labels
            if let ast::Expression::Field(None, label) = &g {
                if let Some(i) = exprs.iter().position(|(_, l)| l.as_deref() == Some(label)) {
                    groups.push((
                        replace(&mut exprs[i].0, ast::Expression::Column(offset + groups.len())),
                        exprs[i].1.clone(),
                    ));
                    continue;
                }
            }
            // Look for expressions exactly equal to the group expression
            if let Some(i) = exprs.iter().position(|(e, _)| e == &g) {
                groups.push((
                    replace(&mut exprs[i].0, ast::Expression::Column(offset + groups.len())),
                    exprs[i].1.clone(),
                ));
                continue;
            }
            // Otherwise, just use the group expression directly
            groups.push((g, None))
        }
        // Make sure no group expressions contain Column references, which would be placed here
        // during extract_aggregates().
        for (expr, _) in &groups {
            if self.is_aggregate(expr) {
                return Err(Error::Value("Group expression cannot contain aggregates".into()));
            }
        }
        Ok(groups)
    }

    /// Injects hidden expressions into SELECT expressions. This is used for ORDER BY and HAVING, in
    /// order to apply these to fields or aggregates that are not present in the SELECT output, e.g.
    /// to order on a column that is not selected. This is done by replacing the relevant parts of
    /// the given expression with Column references to either existing columns or new, hidden
    /// columns in the select expressions. Returns the number of hidden columns added.
    fn inject_hidden(
        &self,
        expr: &mut ast::Expression,
        select: &mut Vec<(ast::Expression, Option<String>)>,
    ) -> Result<usize> {
        // Replace any identical expressions or label references with column references.
        for (i, (sexpr, label)) in select.iter().enumerate() {
            if expr == sexpr {
                *expr = ast::Expression::Column(i);
                continue;
            }
            if let Some(label) = label {
                expr.transform_mut(
                    &mut |e| match e {
                        ast::Expression::Field(None, ref l) if l == label => {
                            Ok(ast::Expression::Column(i))
                        }
                        e => Ok(e),
                    },
                    &mut |e| Ok(e),
                )?;
            }
        }
        // Any remaining aggregate functions and field references must be extracted as hidden
        // columns.
        let mut hidden = 0;
        expr.transform_mut(
            &mut |e| match &e {
                ast::Expression::Function(f, a) if self.aggregate_from_name(f).is_some() => {
                    if let ast::Expression::Column(c) = a[0] {
                        if self.is_aggregate(&select[c].0) {
                            return Err(Error::Value(
                                "Aggregate function cannot reference aggregate".into(),
                            ));
                        }
                    }
                    select.push((e, None));
                    hidden += 1;
                    Ok(ast::Expression::Column(select.len() - 1))
                }
                ast::Expression::Field(_, _) => {
                    select.push((e, None));
                    hidden += 1;
                    Ok(ast::Expression::Column(select.len() - 1))
                }
                _ => Ok(e),
            },
            &mut |e| Ok(e),
        )?;
        Ok(hidden)
    }

    /// Returns the aggregate corresponding to the given aggregate function name.
    fn aggregate_from_name(&self, name: &str) -> Option<Aggregate> {
        match name {
            "avg" => Some(Aggregate::Average),
            "count" => Some(Aggregate::Count),
            "max" => Some(Aggregate::Max),
            "min" => Some(Aggregate::Min),
            "sum" => Some(Aggregate::Sum),
            _ => None,
        }
    }

    /// Checks whether a given expression is an aggregate expression.
    fn is_aggregate(&self, expr: &ast::Expression) -> bool {
        expr.contains(&|e| match e {
            ast::Expression::Function(f, _) => self.aggregate_from_name(f).is_some(),
            _ => false,
        })
    }

    /// Builds an expression from an AST expression
    fn build_expression(&self, scope: &mut Scope, expr: ast::Expression) -> Result<Expression> {
        use Expression::*;
        Ok(match expr {
            ast::Expression::Literal(l) => Constant(match l {
                ast::Literal::Null => Value::Null,
                ast::Literal::Boolean(b) => Value::Boolean(b),
                ast::Literal::Integer(i) => Value::Integer(i),
                ast::Literal::Float(f) => Value::Float(f),
                ast::Literal::String(s) => Value::String(s),
            }),
            ast::Expression::Column(i) => Field(i, scope.get_label(i)?),
            ast::Expression::Field(table, name) => {
                Field(scope.resolve(table.as_deref(), &name)?, Some((table, name)))
            }
            ast::Expression::Function(name, _) => {
                return Err(Error::Value(format!("Unknown function {}", name,)))
            }
            ast::Expression::Operation(op) => match op {
                // Logical operators
                ast::Operation::And(lhs, rhs) => And(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
                ast::Operation::Not(expr) => Not(self.build_expression(scope, *expr)?.into()),
                ast::Operation::Or(lhs, rhs) => Or(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),

                // Comparison operators
                ast::Operation::Equal(lhs, rhs) => Equal(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
                ast::Operation::GreaterThan(lhs, rhs) => GreaterThan(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
                ast::Operation::GreaterThanOrEqual(lhs, rhs) => Or(
                    GreaterThan(
                        self.build_expression(scope, *lhs.clone())?.into(),
                        self.build_expression(scope, *rhs.clone())?.into(),
                    )
                    .into(),
                    Equal(
                        self.build_expression(scope, *lhs)?.into(),
                        self.build_expression(scope, *rhs)?.into(),
                    )
                    .into(),
                ),
                ast::Operation::IsNull(expr) => IsNull(self.build_expression(scope, *expr)?.into()),
                ast::Operation::LessThan(lhs, rhs) => LessThan(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
                ast::Operation::LessThanOrEqual(lhs, rhs) => Or(
                    LessThan(
                        self.build_expression(scope, *lhs.clone())?.into(),
                        self.build_expression(scope, *rhs.clone())?.into(),
                    )
                    .into(),
                    Equal(
                        self.build_expression(scope, *lhs)?.into(),
                        self.build_expression(scope, *rhs)?.into(),
                    )
                    .into(),
                ),
                ast::Operation::Like(lhs, rhs) => Like(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
                ast::Operation::NotEqual(lhs, rhs) => Not(Equal(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                )
                .into()),

                // Mathematical operators
                ast::Operation::Assert(expr) => Assert(self.build_expression(scope, *expr)?.into()),
                ast::Operation::Add(lhs, rhs) => Add(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
                ast::Operation::Divide(lhs, rhs) => Divide(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
                ast::Operation::Exponentiate(lhs, rhs) => Exponentiate(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
                ast::Operation::Factorial(expr) => {
                    Factorial(self.build_expression(scope, *expr)?.into())
                }
                ast::Operation::Modulo(lhs, rhs) => Modulo(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
                ast::Operation::Multiply(lhs, rhs) => Multiply(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
                ast::Operation::Negate(expr) => Negate(self.build_expression(scope, *expr)?.into()),
                ast::Operation::Subtract(lhs, rhs) => Subtract(
                    self.build_expression(scope, *lhs)?.into(),
                    self.build_expression(scope, *rhs)?.into(),
                ),
            },
        })
    }

    /// Builds and evaluates a constant AST expression.
    fn evaluate_constant(&self, expr: ast::Expression) -> Result<Value> {
        self.build_expression(&mut Scope::constant(), expr)?.evaluate(None)
    }
}

/// Manages names available to expressions and executors, and maps them onto columns/fields.
#[derive(Clone, Debug)]
pub struct Scope {
    // If true, the scope is constant and cannot contain any variables.
    constant: bool,
    // Currently visible tables, by query name (i.e. alias or actual name).
    tables: HashMap<String, Table>,
    // Column labels, if any (qualified by table name when available)
    columns: Vec<(Option<String>, Option<String>)>,
    // Qualified names to column indexes.
    qualified: HashMap<(String, String), usize>,
    // Unqualified names to column indexes, if unique.
    unqualified: HashMap<String, usize>,
    // Unqialified ambiguous names.
    ambiguous: HashSet<String>,
}

impl Scope {
    /// Creates a new, empty scope.
    fn new() -> Self {
        Self {
            constant: false,
            tables: HashMap::new(),
            columns: Vec::new(),
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ambiguous: HashSet::new(),
        }
    }

    /// Creates a constant scope.
    fn constant() -> Self {
        let mut scope = Self::new();
        scope.constant = true;
        scope
    }

    /// Creates a scope from a table.
    fn from_table(table: Table) -> Result<Self> {
        let mut scope = Self::new();
        scope.add_table(table.name.clone(), table)?;
        Ok(scope)
    }

    /// Adds a column to the scope.
    #[allow(clippy::map_entry)]
    fn add_column(&mut self, table: Option<String>, label: Option<String>) {
        if let Some(l) = label.clone() {
            if let Some(t) = table.clone() {
                self.qualified.insert((t, l.clone()), self.columns.len());
            }
            if !self.ambiguous.contains(&l) {
                if !self.unqualified.contains_key(&l) {
                    self.unqualified.insert(l, self.columns.len());
                } else {
                    self.unqualified.remove(&l);
                    self.ambiguous.insert(l);
                }
            }
        }
        self.columns.push((table, label));
    }

    /// Adds a table to the scope.
    fn add_table(&mut self, label: String, table: Table) -> Result<()> {
        if self.constant {
            return Err(Error::Internal("Can't modify constant scope".into()));
        }
        if self.tables.contains_key(&label) {
            return Err(Error::Value(format!("Duplicate table name {}", label)));
        }
        for column in &table.columns {
            self.add_column(Some(label.clone()), Some(column.name.clone()));
        }
        self.tables.insert(label, table);
        Ok(())
    }

    /// Fetches a column from the scope by index.
    fn get_column(&self, index: usize) -> Result<(Option<String>, Option<String>)> {
        if self.constant {
            return Err(Error::Value(format!(
                "Expression must be constant, found column {}",
                index
            )));
        }
        self.columns
            .get(index)
            .cloned()
            .ok_or_else(|| Error::Value(format!("Column index {} not found", index)))
    }

    /// Fetches a column label by index, if any.
    fn get_label(&self, index: usize) -> Result<Option<(Option<String>, String)>> {
        Ok(match self.get_column(index)? {
            (table, Some(name)) => Some((table, name)),
            _ => None,
        })
    }

    /// Merges two scopes, by appending the given scope to self.
    fn merge(&mut self, scope: Scope) -> Result<()> {
        if self.constant {
            return Err(Error::Internal("Can't modify constant scope".into()));
        }
        for (label, table) in scope.tables {
            if self.tables.contains_key(&label) {
                return Err(Error::Value(format!("Duplicate table name {}", label)));
            }
            self.tables.insert(label, table);
        }
        for (table, label) in scope.columns {
            self.add_column(table, label);
        }
        Ok(())
    }

    /// Resolves a name, optionally qualified by a table name.
    fn resolve(&self, table: Option<&str>, name: &str) -> Result<usize> {
        if self.constant {
            return Err(Error::Value(format!(
                "Expression must be constant, found field {}",
                if let Some(table) = table { format!("{}.{}", table, name) } else { name.into() }
            )));
        }
        if let Some(table) = table {
            if !self.tables.contains_key(table) {
                return Err(Error::Value(format!("Unknown table {}", table)));
            }
            self.qualified
                .get(&(table.into(), name.into()))
                .copied()
                .ok_or_else(|| Error::Value(format!("Unknown field {}.{}", table, name)))
        } else if self.ambiguous.contains(name) {
            Err(Error::Value(format!("Ambiguous field {}", name)))
        } else {
            self.unqualified
                .get(name)
                .copied()
                .ok_or_else(|| Error::Value(format!("Unknown field {}", name)))
        }
    }

    /// Number of columns in the current scope.
    fn len(&self) -> usize {
        self.columns.len()
    }

    /// Projects the scope. This takes a set of expressions and labels in the current scope,
    /// and returns a new scope for the projection.
    fn project(&mut self, projection: &[(Expression, Option<String>)]) -> Result<()> {
        if self.constant {
            return Err(Error::Internal("Can't modify constant scope".into()));
        }
        let mut new = Self::new();
        new.tables = self.tables.clone();
        for (expr, label) in projection {
            match (expr, label) {
                (_, Some(label)) => new.add_column(None, Some(label.clone())),
                (Expression::Field(_, Some((Some(table), name))), _) => {
                    new.add_column(Some(table.clone()), Some(name.clone()))
                }
                (Expression::Field(_, Some((None, name))), _) => {
                    if let Some(i) = self.unqualified.get(name) {
                        let (table, name) = self.columns[*i].clone();
                        new.add_column(table, name);
                    }
                }
                (Expression::Field(i, None), _) => {
                    let (table, label) = self.columns.get(*i).cloned().unwrap_or((None, None));
                    new.add_column(table, label)
                }
                _ => new.add_column(None, None),
            }
        }
        *self = new;
        Ok(())
    }
}
