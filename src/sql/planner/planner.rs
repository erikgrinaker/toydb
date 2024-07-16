#![allow(clippy::module_inception)]

use super::plan::remap_sources;
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
            Select { select, from, r#where, group_by, having, order_by, offset, limit } => {
                self.build_select(select, from, r#where, group_by, having, order_by, offset, limit)
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
            let index = scope.lookup_column(None, &column)?;
            let expr = match expr {
                Some(expr) => Self::build_expression(expr, &scope)?,
                None => match &table.columns[index].default {
                    Some(default) => Expression::Constant(default.clone()),
                    None => return errinput!("column {column} has no default value"),
                },
            };
            expressions.push((index, expr));
        }
        Ok(Plan::Update {
            table: table.clone(),
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
        having: Option<ast::Expression>,
        order_by: Vec<(ast::Expression, ast::Order)>,
        offset: Option<ast::Expression>,
        limit: Option<ast::Expression>,
    ) -> Result<Plan> {
        let mut scope = Scope::new();

        // Build FROM clause.
        let mut node = if !from.is_empty() {
            self.build_from_clause(&mut scope, from)?
        } else if select.is_empty() {
            return errinput!("SELECT * requires a FROM clause");
        } else {
            // For a constant SELECT, emit a single empty row to project with.
            // This allows using aggregate functions and WHERE as normal.
            Node::Values { rows: vec![vec![]] }
        };

        // Build WHERE clause.
        if let Some(expr) = r#where {
            let predicate = Self::build_expression(expr, &scope)?;
            node = Node::Filter { source: Box::new(node), predicate };
        };

        // Build aggregate functions and GROUP BY clause.
        let aggregates = Self::collect_aggregates(&select, &having, &order_by);
        if !group_by.is_empty() || !aggregates.is_empty() {
            // For SELECT * with aggregates, expand as explicit columns to
            // verify that they're all used in GROUP BY as well.
            if select.is_empty() {
                select = (0..node.size())
                    .map(|i| (scope.get_label(i).into_ast_field().expect("no FROM label"), None))
                    .collect();
            }
            node = self.build_aggregate(&mut scope, node, group_by, aggregates)?;
        }

        // Build SELECT clause.
        if !select.is_empty() {
            // Prepare the post-projection scope.
            let mut child_scope = scope.project(&select);

            // Build the SELECT column expressions and aliases.
            let mut expressions = Vec::with_capacity(select.len());
            let mut labels = Vec::with_capacity(select.len());
            for (expr, alias) in select {
                expressions.push(Self::build_expression(expr, &scope)?);
                labels.push(Label::maybe_name(alias));
            }

            // Add hidden columns for HAVING and ORDER BY columns not in SELECT.
            let hidden = self.build_select_hidden(&scope, &mut child_scope, &having, &order_by);
            labels.extend(std::iter::repeat(Label::None).take(hidden.len()));
            expressions.extend(hidden);

            scope = child_scope;
            node = Node::Projection { source: Box::new(node), expressions, aliases: labels };
        };

        // Build HAVING clause.
        if let Some(having) = having {
            if scope.aggregates.is_empty() {
                return errinput!("HAVING requires GROUP BY or aggregate function");
            }
            let predicate = Self::build_expression(having, &scope)?;
            node = Node::Filter { source: Box::new(node), predicate };
        };

        // Build ORDER BY clause.
        if !order_by.is_empty() {
            let orders = order_by
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

        // Remove any hidden columns before emitting the result.
        if let Some(targets) = scope.remap_hidden() {
            node = Node::Remap { source: Box::new(node), targets }
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
            let right = self.build_from(from, scope)?;
            node = Node::NestedLoopJoin {
                left: Box::new(node),
                right: Box::new(right),
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
                // Right joins are built as a left join then column swap.
                if matches!(r#type, ast::JoinType::Right) {
                    (left, right) = (right, left)
                }

                // Build the left and right nodes.
                let left = Box::new(self.build_from(*left, &mut scope)?);
                let right = Box::new(self.build_from(*right, &mut scope)?);
                let (left_size, right_size) = (left.size(), right.size());

                // Build the join node.
                let predicate = predicate.map(|e| Self::build_expression(e, &scope)).transpose()?;
                let outer = r#type.is_outer();
                let mut node = Node::NestedLoopJoin { left, right, predicate, outer };

                // For right joins, swap the columns.
                if matches!(r#type, ast::JoinType::Right) {
                    let size = left_size + right_size;
                    let targets = (0..size).map(|i| Some((i + right_size) % size)).collect_vec();
                    scope = scope.remap(&targets);
                    node = Node::Remap { source: Box::new(node), targets }
                }
                node
            }
        };

        parent_scope.merge(scope)?;
        Ok(node)
    }

    /// Builds an aggregate node, which computes aggregates for a set of GROUP
    /// BY buckets. The aggregate functions have been collected from the SELECT,
    /// HAVING, and ORDER BY clauses.
    ///
    /// The ast::Expression for each aggregate function and GROUP BY expression
    /// is tracked in the Scope and mapped to the column index. Later nodes
    /// (i.e. SELECT, HAVING, and ORDER BY) can look up the column index of
    /// aggregate expressions while building expressions. Consider e.g.:
    ///
    /// SELECT SUM(a) / COUNT(*) FROM t GROUP BY b % 10 HAVING b % 10 >= 5 ORDER BY MAX(c)
    ///
    /// This will build an Aggregate node for SUM(a), COUNT(*), MAX(c) bucketed
    /// by b % 10. The SELECT can look up up SUM(a) and COUNT(*) to compute the
    /// division, and HAVING can look up b % 10 to compute the predicate.
    fn build_aggregate(
        &self,
        scope: &mut Scope,
        source: Node,
        mut group_by: Vec<ast::Expression>,
        mut aggregates: Vec<ast::Expression>,
    ) -> Result<Node> {
        // Construct a child scope with the group_by and aggregate AST
        // expressions, for lookups. Discard duplicate expressions.
        let mut child_scope = scope.spawn();
        group_by.retain(|expr| child_scope.add_aggregate(expr, scope).is_some());
        aggregates.retain(|expr| child_scope.add_aggregate(expr, scope).is_some());

        // Build the node from the remaining unique expressions.
        let group_by = group_by
            .into_iter()
            .map(|expr| Self::build_expression(expr, scope))
            .collect::<Result<_>>()?;
        let aggregates = aggregates
            .into_iter()
            .map(|expr| Self::build_aggregate_function(scope, expr))
            .collect::<Result<_>>()?;

        *scope = child_scope;
        Ok(Node::Aggregate { source: Box::new(source), group_by, aggregates })
    }

    /// Builds an aggregate function from an AST expression.
    fn build_aggregate_function(scope: &Scope, expr: ast::Expression) -> Result<Aggregate> {
        let ast::Expression::Function(name, mut args) = expr else {
            panic!("aggregate expression must be function");
        };
        if args.len() != 1 {
            return errinput!("{name} takes 1 argument");
        }
        if args[0].contains(&|expr| Self::is_aggregate_function(expr)) {
            return errinput!("aggregate functions can't be nested");
        }
        let expr = Self::build_expression(args.remove(0), scope)?;
        Ok(match name.as_str() {
            "avg" => Aggregate::Average(expr),
            "count" => Aggregate::Count(expr),
            "min" => Aggregate::Min(expr),
            "max" => Aggregate::Max(expr),
            "sum" => Aggregate::Sum(expr),
            name => return errinput!("unknown aggregate function {name}"),
        })
    }

    /// Checks whether a given AST expression is an aggregate function.
    fn is_aggregate_function(expr: &ast::Expression) -> bool {
        if let ast::Expression::Function(name, _) = expr {
            return ["avg", "count", "max", "min", "sum"].contains(&name.as_str());
        }
        false
    }

    /// Collects aggregate functions from SELECT, HAVING, and ORDER BY clauses.
    fn collect_aggregates(
        select: &[(ast::Expression, Option<String>)],
        having: &Option<ast::Expression>,
        order_by: &[(ast::Expression, ast::Order)],
    ) -> Vec<ast::Expression> {
        let select = select.iter().map(|(expr, _)| expr);
        let having = having.iter();
        let order_by = order_by.iter().map(|(expr, _)| expr);
        let mut aggregates = Vec::new();
        for expr in select.chain(having).chain(order_by) {
            expr.collect(&|e| Self::is_aggregate_function(e), &mut aggregates)
        }
        aggregates
    }

    /// Builds hidden columns for a projection to pass through fields that are
    /// used by downstream nodes. Consider e.g.:
    ///
    /// SELECT id FROM table ORDER BY value
    ///
    /// The ORDER BY node is evaluated after the SELECT projection (it may need
    /// to order on projected columns), but "value" isn't projected and thus
    /// isn't available to the ORDER BY node. We add a hidden "value" column to
    /// the projection to satisfy the ORDER BY.
    ///
    /// Hidden columns are stripped before returning the result to the client.
    fn build_select_hidden(
        &self,
        scope: &Scope,
        child_scope: &mut Scope,
        having: &Option<ast::Expression>,
        order_by: &[(ast::Expression, ast::Order)],
    ) -> Vec<Expression> {
        let mut hidden = Vec::new();
        for expr in having.iter().chain(order_by.iter().map(|(expr, _)| expr)) {
            expr.walk(&mut |expr| {
                // If this is an aggregate or GROUP BY expression that isn't
                // already available in the child scope, pass it through.
                if let Some(index) = scope.lookup_aggregate(expr) {
                    if child_scope.lookup_aggregate(expr).is_none() {
                        child_scope.add_passthrough(scope, index, true);
                        hidden.push(Expression::Field(index, scope.get_label(index)));
                        return true;
                    }
                }

                // Look for field references that don't exist post-projection.
                let ast::Expression::Field(table, column) = expr else {
                    return true;
                };
                if child_scope.lookup_column(table.as_deref(), column).is_ok() {
                    return true;
                }
                // If the parent lookup fails too, ignore the error. It will be
                // surfaced during expression building.
                let Ok(index) = scope.lookup_column(table.as_deref(), column) else {
                    return true;
                };
                // Add the hidden column to the projection.
                let label = Label::maybe_qualified(table.clone(), column.clone());
                child_scope.add_passthrough(scope, index, true);
                hidden.push(Expression::Field(index, label));
                true
            });
        }
        hidden
    }

    /// Builds an expression from an AST expression.
    pub fn build_expression(expr: ast::Expression, scope: &Scope) -> Result<Expression> {
        use Expression::*;

        // Look up aggregate functions or GROUP BY expressions. These were added
        // to the parent scope when building the Aggregate node, if any.
        if let Some(index) = scope.lookup_aggregate(&expr) {
            return Ok(Field(index, scope.get_label(index)));
        }

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
            ast::Expression::Field(table, name) => Field(
                scope.lookup_column(table.as_deref(), &name)?,
                Label::maybe_qualified(table, name),
            ),
            // Currently, all functions are aggregates, and processed above.
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
}

/// A scope maps column/table names to input column indexes, for lookups during
/// expression construction. It also tracks output column names, aggregate and
/// GROUP BY expressions, and hidden columns.
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
    /// Index of aggregate and GROUP BY expressions to column indexes. This is
    /// used to track output columns of Aggregate nodes and look them up from
    /// expressions in downstream SELECT, HAVING, and ORDER BY clauses. If the
    /// node contains an (inner) Aggregate node, this is never empty.
    aggregates: HashMap<ast::Expression, usize>,
    /// Hidden columns. These are used to pass e.g. ORDER BY and HAVING
    /// expressions through SELECT projection nodes if the expressions aren't
    /// already projected. They should be removed before emitting results.
    hidden: HashSet<usize>,
}

impl Scope {
    /// Creates a new, empty scope.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            tables: HashSet::new(),
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            aggregates: HashMap::new(),
            hidden: HashSet::new(),
        }
    }

    /// Creates a scope from a table, using the table's original name.
    fn from_table(table: &Table) -> Result<Self> {
        let mut scope = Self::new();
        scope.add_table(&table.name, table)?;
        Ok(scope)
    }

    /// Creates a new child scope that inherits from the parent scope.
    pub fn spawn(&self) -> Self {
        let mut child = Scope::new();
        child.tables = self.tables.clone(); // retain table names
        child
    }

    /// Adds a table to the scope. The label is either the table's original name
    /// or an alias, and must be unique. All table columns are added, in order.
    fn add_table(&mut self, label: &str, table: &Table) -> Result<()> {
        if self.tables.contains(label) {
            return errinput!("duplicate table name {label}");
        }
        for column in &table.columns {
            self.add_column(Label::Qualified(label.to_string(), column.name.clone()));
        }
        self.tables.insert(label.to_string());
        Ok(())
    }

    /// Adds a column and label to the scope. Returns the column index.
    fn add_column(&mut self, label: Label) -> usize {
        let index = self.columns.len();
        if let Label::Qualified(table, column) = &label {
            self.qualified.insert((table.clone(), column.clone()), index);
        }
        if let Label::Qualified(_, name) | Label::Unqualified(name) = &label {
            self.unqualified.entry(name.clone()).or_default().push(index)
        }
        self.columns.push(label);
        index
    }

    /// Looks up a column index by name, if possible.
    fn lookup_column(&self, table: Option<&str>, name: &str) -> Result<usize> {
        let fmtname = || table.map(|table| format!("{table}.{name}")).unwrap_or(name.to_string());
        if self.columns.is_empty() {
            return errinput!("expression must be constant, found field {}", fmtname());
        }
        if let Some(table) = table {
            if !self.tables.contains(table) {
                return errinput!("unknown table {table}");
            }
            if let Some(index) = self.qualified.get(&(table.to_string(), name.to_string())) {
                return Ok(*index);
            }
        } else if let Some(indexes) = self.unqualified.get(name) {
            if indexes.len() > 1 {
                return errinput!("ambiguous field {name}");
            }
            return Ok(indexes[0]);
        }
        if !self.aggregates.is_empty() {
            return errinput!(
                "field {} must be used in an aggregate or GROUP BY expression",
                fmtname()
            );
        }
        errinput!("unknown field {}", fmtname())
    }

    /// Fetches a column label by index, or Label::None if it doesn't exist.
    fn get_label(&self, index: usize) -> Label {
        self.columns.get(index).cloned().unwrap_or(Label::None)
    }

    /// Adds an aggregate expression to the scope, returning the new column
    /// index or None if the expression already exists. This is either an
    /// aggregate function or a GROUP BY expression. It is used to look up the
    /// aggregate output column from e.g. SELECT, HAVING, and ORDER BY.
    fn add_aggregate(&mut self, expr: &ast::Expression, parent: &Scope) -> Option<usize> {
        if self.aggregates.contains_key(expr) {
            return None;
        }

        // If this is a simple column reference (i.e. GROUP BY foo), pass
        // through the column label from the parent scope for lookups.
        let mut label = Label::None;
        if let ast::Expression::Field(table, column) = expr {
            // Ignore errors, they will be emitted when building the expression.
            if let Ok(index) = parent.lookup_column(table.as_deref(), column.as_str()) {
                label = parent.get_label(index);
            }
        }

        let index = self.add_column(label);
        self.aggregates.insert(expr.clone(), index);
        Some(index)
    }

    /// Looks up an aggregate column index by aggregate function or GROUP BY
    /// expression.
    fn lookup_aggregate(&self, expr: &ast::Expression) -> Option<usize> {
        self.aggregates.get(expr).copied()
    }

    /// Adds a column that passes through a column from the parent scope,
    /// retaining its properties. If hide is true, the column is hidden.
    fn add_passthrough(&mut self, parent: &Scope, parent_index: usize, hide: bool) -> usize {
        let label = parent.get_label(parent_index);
        let index = self.add_column(label);
        for (expr, i) in &parent.aggregates {
            if *i == parent_index {
                self.aggregates.entry(expr.clone()).or_insert(index);
            }
        }
        if hide || parent.hidden.contains(&parent_index) {
            self.hidden.insert(index);
        }
        index
    }

    /// Merges two scopes, by appending the given scope to self.
    fn merge(&mut self, scope: Scope) -> Result<()> {
        for table in scope.tables {
            if self.tables.contains(&table) {
                return errinput!("duplicate table name {table}");
            }
            self.tables.insert(table);
        }
        let offset = self.columns.len();
        for label in scope.columns {
            self.add_column(label);
        }
        for (expr, index) in scope.aggregates {
            self.aggregates.entry(expr).or_insert(index + offset);
        }
        self.hidden.extend(scope.hidden.into_iter().map(|index| index + offset));
        Ok(())
    }

    /// Projects the scope via the given expressions and aliases, creating a new
    /// child scope with one column per expression. These may be a simple field
    /// reference (e.g. "SELECT a, b FROM table"), which passes through the
    /// corresponding column from the original scope and retains its qualified
    /// and unqualified names. Otherwise, for non-trivial field references, a
    /// new column is created for the expression. Explicit aliases may be given.
    fn project(&self, expressions: &[(ast::Expression, Option<String>)]) -> Self {
        let mut child = self.spawn();
        for (expr, alias) in expressions {
            // Use the alias if given, or look up any column references.
            let mut label = Label::None;
            if let Some(alias) = alias {
                label = Label::Unqualified(alias.clone());
            } else if let ast::Expression::Field(table, column) = expr {
                // Ignore errors, they will be surfaced by build_expression().
                if let Ok(index) = self.lookup_column(table.as_deref(), column.as_str()) {
                    label = self.get_label(index);
                }
            }

            let index = child.add_column(label);

            // If this is an aggregate query, then all projected expressions
            // must also be aggregates by definition.
            if !self.aggregates.is_empty() {
                child.aggregates.entry(expr.clone()).or_insert(index);
            }
        }
        child
    }

    /// Remaps the scope using the given targets.
    fn remap(&self, targets: &[Option<usize>]) -> Self {
        let mut child = self.spawn();
        for index in remap_sources(targets).into_iter().flatten() {
            child.add_passthrough(self, index, false);
        }
        child
    }

    /// Removes hidden columns from the scope, returning their indexes or None
    /// if no columns are hidden.
    fn remove_hidden(&mut self) -> Option<HashSet<usize>> {
        if self.hidden.is_empty() {
            return None;
        }
        let hidden = std::mem::take(&mut self.hidden);
        let mut index = 0;
        self.columns.retain(|_| {
            let retain = !hidden.contains(&index);
            index += 1;
            retain
        });
        self.qualified.retain(|_, index| !hidden.contains(index));
        self.unqualified.iter_mut().for_each(|(_, vec)| vec.retain(|i| !hidden.contains(i)));
        self.unqualified.retain(|_, vec| !vec.is_empty());
        self.aggregates.retain(|_, index| !hidden.contains(index));
        Some(hidden)
    }

    /// Removes hidden columns from the scope and returns the remaining column
    /// indexes as a Remap targets vector, or None if no columns are hidden. A
    /// Remap targets vector maps parent column indexes to child column indexes,
    /// or None if a column should be dropped.
    fn remap_hidden(&mut self) -> Option<Vec<Option<usize>>> {
        let size = self.columns.len();
        let hidden = self.remove_hidden()?;
        let mut targets = vec![None; size];
        let mut index = 0;
        for (old_index, target) in targets.iter_mut().enumerate() {
            if !hidden.contains(&old_index) {
                *target = Some(index);
                index += 1;
            }
        }
        Some(targets)
    }
}
