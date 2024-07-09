#![allow(clippy::module_inception)]

use super::{Aggregate, Direction, Node, Plan};
use crate::errinput;
use crate::error::Result;
use crate::sql::engine::Catalog;
use crate::sql::parser::ast;
use crate::sql::types::{Column, Expression, Label, Table, Value};

use itertools::Itertools as _;
use std::collections::{BTreeMap, HashMap, HashSet};

/// A statement plan builder. Takes a statement AST from the parser and builds
/// an execution plan for it, using the catalog for schema information.
pub struct Planner<'a, C: Catalog> {
    catalog: &'a C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    /// Creates a new planner.
    pub fn new(catalog: &'a C) -> Self {
        Self { catalog }
    }

    /// Builds a plan for an AST statement.
    pub fn build(&mut self, statement: ast::Statement) -> Result<Plan> {
        use ast::Statement::*;
        match statement {
            CreateTable { name, columns } => self.build_create_table(name, columns),
            DropTable { name, if_exists } => Ok(Plan::DropTable { table: name, if_exists }),
            Delete { table, r#where } => self.build_delete(table, r#where),
            Insert { table, columns, values } => self.build_insert(table, columns, values),
            Update { table, set, r#where } => self.build_update(table, set, r#where),
            Select { select, from, r#where, group_by, having, order, offset, limit } => {
                self.build_select(select, from, r#where, group_by, having, order, offset, limit)
            }

            // Transaction and explain statements are handled by Session.
            Begin { .. } | Commit | Rollback | Explain(_) => {
                panic!("unexpected statement {statement:?}")
            }
        }
    }

    /// Builds a CREATE TABLE plan.
    fn build_create_table(&self, name: String, columns: Vec<ast::Column>) -> Result<Plan> {
        let Some(primary_key) = columns.iter().position(|c| c.primary_key) else {
            return errinput!("no primary key for table {name}");
        };
        if columns.iter().filter(|c| c.primary_key).count() > 1 {
            return errinput!("multiple primary keys for table {name}");
        }
        let columns = columns
            .into_iter()
            .map(|c| {
                let nullable = c.nullable.unwrap_or(!c.primary_key);
                Ok(Column {
                    name: c.name,
                    datatype: c.datatype,
                    nullable,
                    default: match c.default {
                        Some(expr) => Some(Self::evaluate_constant(expr)?),
                        None if nullable => Some(Value::Null),
                        None => None,
                    },
                    unique: c.unique || c.primary_key,
                    index: (c.index || c.unique || c.references.is_some()) && !c.primary_key,
                    references: c.references,
                })
            })
            .collect::<Result<_>>()?;
        Ok(Plan::CreateTable { schema: Table { name, primary_key, columns } })
    }

    /// Builds a DELETE plan.
    fn build_delete(&self, table: String, r#where: Option<ast::Expression>) -> Result<Plan> {
        let table = self.catalog.must_get_table(&table)?;
        let scope = Scope::from_table(&table)?;
        let filter = r#where.map(|e| Self::build_expression(e, &scope)).transpose()?;
        Ok(Plan::Delete {
            table: table.name.clone(),
            primary_key: table.primary_key,
            source: Node::Scan { table, alias: None, filter },
        })
    }

    /// Builds an INSERT plan.
    fn build_insert(
        &self,
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<ast::Expression>>,
    ) -> Result<Plan> {
        let table = self.catalog.must_get_table(&table)?;
        let mut column_map = None;
        if let Some(columns) = columns {
            let column_map = column_map.insert(HashMap::new());
            for (vidx, name) in columns.into_iter().enumerate() {
                let Some(cidx) = table.columns.iter().position(|c| c.name == name) else {
                    return errinput!("unknown column {name} in table {}", table.name);
                };
                if column_map.insert(cidx, vidx).is_some() {
                    return errinput!("column {name} given multiple times");
                }
            }
        }
        let scope = Scope::new();
        let rows = values
            .into_iter()
            .map(|exprs| {
                exprs.into_iter().map(|expr| Self::build_expression(expr, &scope)).collect()
            })
            .collect::<Result<_>>()?;
        Ok(Plan::Insert { table, column_map, source: Node::Values { rows } })
    }

    /// Builds an UPDATE plan.
    fn build_update(
        &self,
        table: String,
        set: BTreeMap<String, Option<ast::Expression>>,
        r#where: Option<ast::Expression>,
    ) -> Result<Plan> {
        let table = self.catalog.must_get_table(&table)?;
        let scope = Scope::from_table(&table)?;
        let filter = r#where.map(|e| Self::build_expression(e, &scope)).transpose()?;
        let mut expressions = Vec::with_capacity(set.len());
        for (column, expr) in set {
            let index = scope.get_column_index(None, &column)?;
            let expr = match expr {
                Some(expr) => Self::build_expression(expr, &scope)?,
                None => match &table.columns[index].default {
                    Some(default) => Expression::Constant(default.clone()),
                    None => return errinput!("column {column} has no default value"),
                },
            };
            expressions.push((index, column, expr));
        }
        Ok(Plan::Update {
            table: table.name.clone(),
            primary_key: table.primary_key,
            source: Node::Scan { table, alias: None, filter },
            expressions,
        })
    }

    /// Builds a SELECT plan.
    #[allow(clippy::too_many_arguments)]
    fn build_select(
        &self,
        mut select: Vec<(ast::Expression, Option<String>)>,
        from: Vec<ast::From>,
        r#where: Option<ast::Expression>,
        group_by: Vec<ast::Expression>,
        mut having: Option<ast::Expression>,
        mut order: Vec<(ast::Expression, ast::Order)>,
        offset: Option<ast::Expression>,
        limit: Option<ast::Expression>,
    ) -> Result<Plan> {
        let mut scope = Scope::new();

        // Build FROM clause.
        let mut node = if !from.is_empty() {
            self.build_from_clause(&mut scope, from)?
        } else if select.is_empty() {
            return errinput!("can't select * without a table");
        } else {
            Node::EmptyRow
        };

        // Build WHERE clause.
        if let Some(expr) = r#where {
            let predicate = Self::build_expression(expr, &scope)?;
            node = Node::Filter { source: Box::new(node), predicate };
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
                node = self.build_aggregation(&mut scope, node, groups, aggregates)?;
            }

            // Build the remaining non-aggregate projection.
            let labels = select.iter().map(|(_, l)| Label::maybe_name(l.clone())).collect_vec();
            let expressions = select
                .into_iter()
                .map(|(e, _)| Self::build_expression(e, &scope))
                .collect::<Result<Vec<_>>>()?;
            scope.project(&expressions, &labels)?;
            node = Node::Projection { source: Box::new(node), expressions, labels };
        };

        // Build HAVING clause.
        if let Some(expr) = having {
            let predicate = Self::build_expression(expr, &scope)?;
            node = Node::Filter { source: Box::new(node), predicate };
        };

        // Build ORDER clause.
        if !order.is_empty() {
            let orders = order
                .into_iter()
                .map(|(e, o)| Ok((Self::build_expression(e, &scope)?, Direction::from(o))))
                .collect::<Result<_>>()?;
            node = Node::Order { source: Box::new(node), orders };
        }

        // Build OFFSET clause.
        if let Some(expr) = offset {
            let offset = match Self::evaluate_constant(expr)? {
                Value::Integer(offset) if offset >= 0 => Ok(offset as usize),
                value => errinput!("invalid offset {value}"),
            }?;
            node = Node::Offset { source: Box::new(node), offset }
        }

        // Build LIMIT clause.
        if let Some(expr) = limit {
            let limit = match Self::evaluate_constant(expr)? {
                Value::Integer(limit) if limit >= 0 => Ok(limit as usize),
                value => errinput!("invalid limit {value}"),
            }?;
            node = Node::Limit { source: Box::new(node), limit }
        }

        // Add a final projection that removes hidden columns, only passing
        // through the originally requested columns.
        if hidden > 0 {
            let columns = scope.len() - hidden;
            let labels = vec![Label::None; columns];
            let expressions = (0..columns).map(|i| Expression::Field(i, Label::None)).collect_vec();
            scope.project(&expressions, &labels)?;
            node = Node::Projection { source: Box::new(node), labels, expressions }
        }

        Ok(Plan::Select { root: node, labels: scope.columns })
    }

    /// Builds a FROM clause consisting of one or more items. Each item is
    /// either a table or a join of two or more tables. All items are implicitly
    /// joined, e.g. "SELECT * FROM a, b" is an implicit full join of a and b.
    fn build_from_clause(&self, scope: &mut Scope, from: Vec<ast::From>) -> Result<Node> {
        // Build the first FROM item. A FROM clause must have at least one.
        let mut items = from.into_iter();
        let mut node = match items.next() {
            Some(from) => self.build_from(from, scope)?,
            None => return errinput!("no from items given"),
        };

        // Build and implicitly join additional items.
        for from in items {
            let left_size = scope.len();
            let right = self.build_from(from, scope)?;
            let right_size = scope.len() - left_size;
            node = Node::NestedLoopJoin {
                left: Box::new(node),
                left_size,
                right: Box::new(right),
                right_size,
                predicate: None,
                outer: false,
            };
        }
        Ok(node)
    }

    /// Builds FROM items, which can either be a single table or a chained join
    /// of multiple tables, e.g. "SELECT * FROM a LEFT JOIN b ON b.a_id = a.id".
    fn build_from(&self, from: ast::From, parent_scope: &mut Scope) -> Result<Node> {
        // Each from item is built in its own scope, such that a join node only
        // sees the columns of its children. It's then merged into the parent.
        let mut scope = Scope::new();

        let node = match from {
            // A full table scan.
            ast::From::Table { name, alias } => {
                let table = self.catalog.must_get_table(&name)?;
                scope.add_table(alias.as_ref().unwrap_or(&name), &table)?;
                Node::Scan { table, alias, filter: None }
            }

            // A two-way join. The left or right nodes may be chained joins.
            ast::From::Join { mut left, mut right, r#type, predicate } => {
                // Right joins are built as a left join with an additional
                // projection to swap the resulting columns.
                if matches!(r#type, ast::JoinType::Right) {
                    (left, right) = (right, left)
                }

                // Build the left and right nodes.
                let left = Box::new(self.build_from(*left, &mut scope)?);
                let left_size = scope.len();
                let right = Box::new(self.build_from(*right, &mut scope)?);
                let right_size = scope.len() - left_size;

                // Build the join node.
                let predicate = predicate.map(|e| Self::build_expression(e, &scope)).transpose()?;
                let outer = r#type.is_outer();
                let mut node =
                    Node::NestedLoopJoin { left, left_size, right, right_size, predicate, outer };

                // For right joins, build a projection to swap the columns.
                if matches!(r#type, ast::JoinType::Right) {
                    let labels = vec![Label::None; scope.len()];
                    let expressions = (left_size..scope.len())
                        .chain(0..left_size)
                        .map(|i| Ok(Expression::Field(i, scope.get_column_label(i)?)))
                        .collect::<Result<Vec<_>>>()?;
                    scope.project(&expressions, &labels)?;
                    node = Node::Projection { source: Box::new(node), expressions, labels }
                }
                node
            }
        };

        parent_scope.merge(scope)?;
        Ok(node)
    }

    /// Builds an aggregation node. All aggregate parameters and GROUP BY expressions are evaluated
    /// in a pre-projection, whose results are fed into an Aggregate node. This node computes the
    /// aggregates for the given groups, passing the group values through directly.
    ///
    /// TODO: revisit this.
    fn build_aggregation(
        &self,
        scope: &mut Scope,
        source: Node,
        groups: Vec<(ast::Expression, Option<String>)>,
        aggregations: Vec<(Aggregate, ast::Expression)>,
    ) -> Result<Node> {
        let mut aggregates = Vec::new();
        let mut expressions = Vec::new();
        let mut labels = Vec::new();
        for (aggregate, expr) in aggregations {
            aggregates.push(aggregate);
            expressions.push(Self::build_expression(expr, scope)?);
            labels.push(Label::None);
        }
        for (expr, label) in groups {
            expressions.push(Self::build_expression(expr, scope)?);
            labels.push(Label::maybe_name(label));
        }
        scope.project(
            &expressions
                .iter()
                .cloned()
                .enumerate()
                .map(|(i, e)| {
                    if i < aggregates.len() {
                        // We pass null values here since we don't want field references to hit
                        // the fields in scope before the aggregation.
                        Expression::Constant(Value::Null)
                    } else {
                        e
                    }
                })
                .collect::<Vec<_>>(),
            &labels,
        )?;
        let node = Node::Aggregation {
            source: Box::new(Node::Projection { source: Box::new(source), labels, expressions }),
            group_by: scope.len() - aggregates.len(),
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
                        if let Some(aggregate) = Aggregate::from_name(f) {
                            aggregates.push((aggregate, args.remove(0)));
                            Ok(ast::Expression::Column(aggregates.len() - 1))
                        } else {
                            Ok(e)
                        }
                    }
                    _ => Ok(e),
                },
                &mut Ok,
            )?;
        }
        for (_, expr) in &aggregates {
            if Self::is_aggregate(expr) {
                return errinput!("aggregate functions can't be nested");
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
        exprs: &mut [(ast::Expression, Option<String>)],
        group_by: Vec<ast::Expression>,
        offset: usize,
    ) -> Result<Vec<(ast::Expression, Option<String>)>> {
        let mut groups = Vec::new();
        for g in group_by {
            // Look for references to SELECT columns with AS labels
            if let ast::Expression::Field(None, label) = &g {
                if let Some(i) = exprs.iter().position(|(_, l)| l.as_deref() == Some(label)) {
                    groups.push((
                        std::mem::replace(
                            &mut exprs[i].0,
                            ast::Expression::Column(offset + groups.len()),
                        ),
                        exprs[i].1.clone(),
                    ));
                    continue;
                }
            }
            // Look for expressions exactly equal to the group expression
            if let Some(i) = exprs.iter().position(|(e, _)| e == &g) {
                groups.push((
                    std::mem::replace(
                        &mut exprs[i].0,
                        ast::Expression::Column(offset + groups.len()),
                    ),
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
            if Self::is_aggregate(expr) {
                return errinput!("group expression cannot contain aggregates");
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
        //
        // TODO: instead of trying to deduplicate here, before the optimizer has
        // normalized expressions and such, we should just go ahead and add new
        // columns for all fields and expressions, and have a separate optimizer
        // that looks at duplicate expressions in a single projection and
        // collapses them, rewriting downstream field references.
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
                    &mut Ok,
                )?;
            }
        }
        // Any remaining aggregate functions and field references must be extracted as hidden
        // columns.
        let mut hidden = 0;
        expr.transform_mut(
            &mut |e| match &e {
                ast::Expression::Function(f, a) if Aggregate::from_name(f).is_some() => {
                    if let ast::Expression::Column(c) = a[0] {
                        if Self::is_aggregate(&select[c].0) {
                            return errinput!("aggregate function cannot reference aggregate");
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
            &mut Ok,
        )?;
        Ok(hidden)
    }

    /// Builds an expression from an AST expression.
    pub fn build_expression(expr: ast::Expression, scope: &Scope) -> Result<Expression> {
        use Expression::*;

        // Helper for building a boxed expression.
        let build = |expr: Box<ast::Expression>| -> Result<Box<Expression>> {
            Ok(Box::new(Self::build_expression(*expr, scope)?))
        };

        Ok(match expr {
            ast::Expression::Literal(l) => Constant(match l {
                ast::Literal::Null => Value::Null,
                ast::Literal::Boolean(b) => Value::Boolean(b),
                ast::Literal::Integer(i) => Value::Integer(i),
                ast::Literal::Float(f) => Value::Float(f),
                ast::Literal::String(s) => Value::String(s),
            }),
            ast::Expression::Column(i) => Field(i, scope.get_column_label(i)?),
            ast::Expression::Field(table, name) => Field(
                scope.get_column_index(table.as_deref(), &name)?,
                Label::maybe_qualified(table, name),
            ),
            // All functions are currently aggregate functions, which should be
            // processed separately.
            // TODO: consider adding some basic functions for fun.
            ast::Expression::Function(name, _) => return errinput!("unknown function {name}"),
            ast::Expression::Operator(op) => match op {
                ast::Operator::And(lhs, rhs) => And(build(lhs)?, build(rhs)?),
                ast::Operator::Not(expr) => Not(build(expr)?),
                ast::Operator::Or(lhs, rhs) => Or(build(lhs)?, build(rhs)?),

                ast::Operator::Equal(lhs, rhs) => Equal(build(lhs)?, build(rhs)?),
                ast::Operator::GreaterThan(lhs, rhs) => GreaterThan(build(lhs)?, build(rhs)?),
                ast::Operator::GreaterThanOrEqual(lhs, rhs) => Or(
                    GreaterThan(build(lhs.clone())?, build(rhs.clone())?).into(),
                    Equal(build(lhs)?, build(rhs)?).into(),
                ),
                ast::Operator::IsNaN(expr) => IsNaN(build(expr)?),
                ast::Operator::IsNull(expr) => IsNull(build(expr)?),
                ast::Operator::LessThan(lhs, rhs) => LessThan(build(lhs)?, build(rhs)?),
                ast::Operator::LessThanOrEqual(lhs, rhs) => Or(
                    LessThan(build(lhs.clone())?, build(rhs.clone())?).into(),
                    Equal(build(lhs)?, build(rhs)?).into(),
                ),
                ast::Operator::Like(lhs, rhs) => Like(build(lhs)?, build(rhs)?),
                ast::Operator::NotEqual(lhs, rhs) => Not(Equal(build(lhs)?, build(rhs)?).into()),

                ast::Operator::Add(lhs, rhs) => Add(build(lhs)?, build(rhs)?),
                ast::Operator::Divide(lhs, rhs) => Divide(build(lhs)?, build(rhs)?),
                ast::Operator::Exponentiate(lhs, rhs) => Exponentiate(build(lhs)?, build(rhs)?),
                ast::Operator::Factorial(expr) => Factorial(build(expr)?),
                ast::Operator::Identity(expr) => Identity(build(expr)?),
                ast::Operator::Modulo(lhs, rhs) => Modulo(build(lhs)?, build(rhs)?),
                ast::Operator::Multiply(lhs, rhs) => Multiply(build(lhs)?, build(rhs)?),
                ast::Operator::Negate(expr) => Negate(build(expr)?),
                ast::Operator::Subtract(lhs, rhs) => Subtract(build(lhs)?, build(rhs)?),
            },
        })
    }

    /// Builds and evaluates a constant AST expression.
    fn evaluate_constant(expr: ast::Expression) -> Result<Value> {
        Self::build_expression(expr, &Scope::new())?.evaluate(None)
    }

    /// Checks whether a given expression is an aggregate expression.
    fn is_aggregate(expr: &ast::Expression) -> bool {
        expr.contains(
            &|e| matches!(e, ast::Expression::Function(f, _) if Aggregate::from_name(f).is_some()),
        )
    }
}

/// A scope maps column/table names to input column indexes for expressions,
/// and keeps track of output column names.
///
/// Expression evaluation generally happens in the context of an input row. This
/// row may come directly from a single table, or it may be the result of a long
/// chain of joins and projections. The scope keeps track of which columns are
/// currently visible and what names they have. During expression planning, the
/// scope is used to resolve column names to column indexes, which are placed in
/// the plan and used during execution.
pub struct Scope {
    /// The currently visible columns. If empty, only constant expressions can
    /// be used (no field references).
    columns: Vec<Label>,
    /// Index of currently visible tables, by query name (e.g. may be aliased).
    tables: HashSet<String>,
    /// Index of fully qualified table.column names to column indexes. Qualified
    /// names are always unique within a scope.
    qualified: HashMap<(String, String), usize>,
    /// Index of unqualified column names to column indexes. If a name points
    /// to multiple columns, lookups will fail with an ambiguous name error.
    unqualified: HashMap<String, Vec<usize>>,
}

impl Scope {
    /// Creates a new, empty scope.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            tables: HashSet::new(),
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
        }
    }

    /// Creates a scope from a table, using the table's original name.
    fn from_table(table: &Table) -> Result<Self> {
        let mut scope = Self::new();
        scope.add_table(&table.name, table)?;
        Ok(scope)
    }

    /// Adds a table to the scope. The label is either the table's original name
    /// or an alias, and must be unique. All table columns are added, in order.
    fn add_table(&mut self, label: &str, table: &Table) -> Result<()> {
        if self.tables.contains(label) {
            return errinput!("duplicate table name {label}");
        }
        for column in &table.columns {
            self.add_column(Label::Qualified(label.to_string(), column.name.clone()))
        }
        self.tables.insert(label.to_string());
        Ok(())
    }

    /// Adds a column and label to the scope.
    fn add_column(&mut self, label: Label) {
        let index = self.len();
        if let Label::Qualified(table, column) = &label {
            self.qualified.insert((table.clone(), column.clone()), index);
        }
        if let Label::Qualified(_, name) | Label::Unqualified(name) = &label {
            self.unqualified.entry(name.clone()).or_default().push(index)
        }
        self.columns.push(label)
    }

    /// Looks up a column index by name, if possible.
    fn get_column_index(&self, table: Option<&str>, name: &str) -> Result<usize> {
        if self.columns.is_empty() {
            let field = table.map(|t| format!("{t}.{name}")).unwrap_or(name.to_string());
            return errinput!("expression must be constant, found field {field}");
        }
        if let Some(table) = table {
            if !self.tables.contains(table) {
                return errinput!("unknown table {table}");
            }
            self.qualified
                .get(&(table.to_string(), name.to_string()))
                .copied()
                .ok_or(errinput!("unknown field {table}.{name}"))
        } else if let Some(indexes) = self.unqualified.get(name) {
            if indexes.len() > 1 {
                return errinput!("ambiguous field {name}");
            }
            Ok(indexes[0])
        } else {
            errinput!("unknown field {name}")
        }
    }

    /// Fetches a column label by index, if any.
    /// TODO: this can possibly be removed.
    fn get_column_label(&self, index: usize) -> Result<Label> {
        if self.columns.is_empty() {
            return errinput!("expression must be constant, found column index {index}");
        }
        match self.columns.get(index) {
            Some(label) => Ok(label.clone()),
            None => errinput!("column index {index} not found"),
        }
    }

    /// Resolves a name, optionally qualified by a table name.
    /// Number of columns currently in the scope.
    fn len(&self) -> usize {
        self.columns.len()
    }

    /// Merges two scopes, by appending the given scope to self.
    fn merge(&mut self, scope: Scope) -> Result<()> {
        for table in scope.tables {
            if self.tables.contains(&table) {
                return errinput!("duplicate table name {table}");
            }
            self.tables.insert(table);
        }
        for label in scope.columns {
            self.add_column(label);
        }
        Ok(())
    }

    /// Projects the scope via the given expressions, creating a new scope with
    /// one column per expression. These may be a simple field reference (e.g.
    /// "SELECT b, a FROM table"), which passes through the corresponding column
    /// from the original scope and retains its qualified and unqualified names.
    /// Otherwise, for non-trivial field references, a new column is created for
    /// the expression. An explicit label may also be given for each column.
    /// Table sets are retained.
    fn project(&mut self, expressions: &[Expression], labels: &[Label]) -> Result<()> {
        assert_eq!(expressions.len(), labels.len());

        // Extract the old columns and clear the indexes. Keep the table index.
        let columns = std::mem::take(&mut self.columns);
        self.qualified.clear();
        self.unqualified.clear();

        // Map projected field references to old columns.
        let mut field_map: HashMap<usize, usize> = HashMap::new();
        for (i, expr) in expressions.iter().enumerate() {
            if let Expression::Field(f, _) = expr {
                field_map.insert(i, *f);
            }
        }

        // Construct the new columns and labels.
        for (i, label) in labels.iter().enumerate() {
            if label.is_some() {
                self.add_column(label.clone()); // explicit label
            } else if let Some(f) = field_map.get(&i) {
                self.add_column(columns[*f].clone()) // field reference
            } else {
                self.add_column(Label::None) // unlabeled expression
            }
        }
        Ok(())
    }
}
