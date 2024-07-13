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
            let index = scope.lookup_column(None, &column)?;
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
        select: Vec<(ast::Expression, Option<String>)>,
        from: Vec<ast::From>,
        r#where: Option<ast::Expression>,
        group_by: Vec<ast::Expression>,
        having: Option<ast::Expression>,
        order: Vec<(ast::Expression, ast::Order)>,
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

        // Build aggregate functions and GROUP BY clause.
        let aggregates = Self::collect_aggregates(&select, &having, &order);
        if !group_by.is_empty() || !aggregates.is_empty() {
            node = self.build_aggregate(&mut scope, node, group_by, aggregates)?;
        }

        // Build SELECT clause.
        let mut hidden = 0;
        if !select.is_empty() {
            let labels = select.iter().map(|(_, l)| Label::maybe_name(l.clone())).collect_vec();
            let mut expressions: Vec<_> = select
                .into_iter()
                .map(|(e, _)| Self::build_expression(e, &scope))
                .collect::<Result<_>>()?;
            let parent_scope = scope;
            scope = parent_scope.project(&expressions, &labels)?;

            // Add hidden columns for HAVING and ORDER BY fields not in SELECT.
            // TODO: track hidden fields in Scope.
            let size = expressions.len();
            for expr in having.iter().chain(order.iter().map(|(e, _)| e)) {
                self.build_hidden(&mut scope, &parent_scope, &mut expressions, expr);
            }
            hidden += expressions.len() - size;

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
        //
        // TODO: add a separate plan node kind for this, and also use it for
        // RIGHT JOIN projections.
        if hidden > 0 {
            let columns = scope.len() - hidden;
            let labels = vec![Label::None; columns];
            let expressions = (0..columns).map(|i| Expression::Field(i, Label::None)).collect_vec();
            scope = scope.project(&expressions, &labels)?;
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
                    scope = scope.project(&expressions, &labels)?;
                    node = Node::Projection { source: Box::new(node), expressions, labels }
                }
                node
            }
        };

        parent_scope.merge(scope)?;
        Ok(node)
    }

    /// Builds an aggregate node, computing aggregate functions for a set of
    /// GROUP BY buckets.
    ///
    /// The aggregate functions have been collected from the SELECT, HAVING, and
    /// ORDER BY clauses (all of which can contain their own aggregate
    /// functions).
    ///
    /// The ast::Expression for each aggregate function and each GROUP BY
    /// expression (except trivial column names) is stored in the Scope along
    /// with the column index. Later nodes (i.e. SELECT, HAVING, and ORDER BY)
    /// can look up the column index of aggregate expressions via Scope.
    /// Similarly, they are allowed to reference GROUP BY expressions by
    /// specifying the exact same expression.
    ///
    /// TODO: consider avoiding the expr cloning by taking &Expression in
    /// various places.
    fn build_aggregate(
        &self,
        scope: &mut Scope,
        source: Node,
        group_by: Vec<ast::Expression>,
        aggregates: Vec<&ast::Expression>,
    ) -> Result<Node> {
        // Construct a child scope with the group_by and aggregate AST
        // expressions, such that downstream nodes can identify and reference
        // them. Discard redundant expressions.
        //
        // TODO: reverse the order of the emitted columns: group_by then
        // aggregates.
        let mut child_scope = scope.project(&[], &[])?; // project to keep tables
        let aggregates = aggregates
            .into_iter()
            .filter(|&expr| {
                if child_scope.lookup_aggregate(expr).is_some() {
                    return false;
                }
                child_scope.add_aggregate((expr).clone(), Label::None);
                true
            })
            .collect_vec();
        let group_by = group_by
            .into_iter()
            .filter(|expr| {
                if child_scope.lookup_aggregate(expr).is_some() {
                    return false; // already exists in child scope
                }
                let mut label = Label::None;
                // TODO: add a Scope method to pass through columns from a parent scope.
                if let ast::Expression::Field(table, column) = expr {
                    if let Ok(index) = scope.lookup_column(table.as_deref(), column.as_str()) {
                        label = scope.get_column_label(index).unwrap();
                    }
                }
                child_scope.add_aggregate(expr.clone(), label);
                true
            })
            .collect_vec();

        // Build the node from the remaining expressions.
        let aggregates = aggregates
            .into_iter()
            .map(|expr| Self::build_aggregate_function(scope, expr.clone()))
            .collect::<Result<_>>()?;
        let group_by = group_by
            .into_iter()
            .map(|expr| Self::build_expression(expr, scope))
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
            ["avg", "count", "max", "min", "sum"].contains(&name.as_str())
        } else {
            false
        }
    }

    /// Collects aggregate functions from SELECT, HAVING, and ORDER BY clauses.
    fn collect_aggregates<'c>(
        select: &'c [(ast::Expression, Option<String>)],
        having: &'c Option<ast::Expression>,
        order_by: &'c [(ast::Expression, ast::Order)],
    ) -> Vec<&'c ast::Expression> {
        let select = select.iter().map(|(expr, _)| expr);
        let having = having.iter();
        let order_by = order_by.iter().map(|(expr, _)| expr);

        let mut aggregates = Vec::new();
        for expr in select.chain(having).chain(order_by) {
            expr.collect(&|e| Self::is_aggregate_function(e), &mut aggregates)
        }
        aggregates
    }

    /// Adds hidden columns to a projection to pass through fields that are used
    /// by downstream nodes. Consider e.g.:
    ///
    /// SELECT id FROM table ORDER BY value
    ///
    /// The ORDER BY node is evaluated after the projection (it may need to
    /// order on projected fields like "foo + bar AS alias"), but "value" isn't
    /// projected and so isn't available to the ORDER BY node. We add a hidden
    /// column to the projection such that the effective projection is:
    ///
    /// SELECT id, value FROM table ORDER BY value
    fn build_hidden(
        &self,
        scope: &mut Scope,
        parent_scope: &Scope,
        projection: &mut Vec<Expression>,
        expr: &ast::Expression,
    ) {
        expr.walk(&mut |expr| {
            // If this is an aggregate function or GROUP BY expression that
            // isn't already available in the child scope, pass it through.
            if let Some(index) = parent_scope.lookup_aggregate(expr) {
                if scope.lookup_aggregate(expr).is_none() {
                    let label = parent_scope.get_column_label(index).unwrap();
                    scope.add_aggregate(expr.clone(), label);
                    projection.push(Expression::Field(index, Label::None));
                    return true;
                }
            }

            // Otherwise, only look for field references.
            let ast::Expression::Field(table, name) = expr else {
                return true;
            };
            // If the field already exists post-projection, do nothing.
            if scope.lookup_column(table.as_deref(), name).is_ok() {
                return true;
            }
            // If the field doesn't exist in the parent scope either, we simply
            // don't build a hidden column for it. The field evaluation will
            // error when building the downstream node (e.g. ORDER BY).
            let Ok(index) = parent_scope.lookup_column(table.as_deref(), name) else {
                return true;
            };
            // Add a hidden column to the projection. Use the given label for
            // the projection, but the qualified label for the scope.
            scope.add_column(parent_scope.get_column_label(index).unwrap());
            projection.push(Expression::Field(
                index,
                Label::maybe_qualified(table.clone(), name.clone()),
            ));
            true
        });
    }

    /// Builds an expression from an AST expression.
    pub fn build_expression(expr: ast::Expression, scope: &Scope) -> Result<Expression> {
        use Expression::*;

        // Look up aggregate functions or GROUP BY expressions. These were added
        // to the parent scope when building the Aggregate node, if any.
        if let Some(index) = scope.lookup_aggregate(&expr) {
            return Ok(Field(index, scope.get_column_label(index)?));
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

/// A scope maps column/table names to input column indexes for expressions,
/// and keeps track of output column names.
///
/// Expression evaluation generally happens in the context of an input row. This
/// row may come directly from a single table, or it may be the result of a long
/// chain of joins and projections. The scope keeps track of which columns are
/// currently visible and what names they have. During expression planning, the
/// scope is used to resolve column names to column indexes, which are placed in
/// the plan and used during execution.
///
/// It also keeps track of output columns for aggregate functions and GROUP BY
/// expressions in Aggregate nodes. See aggregates field.
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
    /// Index of aggregate expressions to column indexes. This is used to track
    /// output columns of Aggregate nodes, e.g. SUM(2 * a + b), and look them up
    /// from expressions in downstream SELECT, HAVING, and ORDER BY columns,
    /// e.g. SELECT SUM(2 * a + b) / COUNT(*) FROM table. When build_expression
    /// encounters an aggregate function, it's mapped onto an aggregate column
    /// index via this index.
    ///
    /// This is also used to map GROUP BY expressions to the corresponding
    /// Aggregate node output column when evaluating downstream node expressions
    /// in SELECT, HAVING, and ORDER BY. For trivial column references, e.g.
    /// GROUP BY a, b, the column can be accessed and looked up as normal via
    /// lookup_column() in downstream node expressions, but for more complex
    /// expressions like GROUP BY a * b / 2, the group column can be accessed
    /// using the same expression in other nodes, e.g.  GROUP BY a * b / 2 ORDER
    /// BY a * b / 2.
    aggregates: HashMap<ast::Expression, usize>,
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
            self.add_column(Label::Qualified(label.to_string(), column.name.clone()));
        }
        self.tables.insert(label.to_string());
        Ok(())
    }

    /// Adds a column and label to the scope. Returns the column index.
    fn add_column(&mut self, label: Label) -> usize {
        let index = self.len();
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

    /// Adds an aggregate expression to the scope, returning the column index.
    /// This is either an aggregate function or a GROUP BY expression (i.e. not
    /// just a simple column name). It is used to access the aggregate output or
    /// GROUP BY column in downstream nodes like SELECT, HAVING, and ORDER BY.
    ///
    /// If the expression already exists, the current index is returned.
    fn add_aggregate(&mut self, expr: ast::Expression, label: Label) -> usize {
        if let Some(index) = self.aggregates.get(&expr) {
            return *index;
        }
        let index = self.add_column(label);
        self.aggregates.insert(expr, index);
        index
    }

    /// Looks up an aggregate column index by aggregate function or GROUP BY
    /// expression, if any. Trivial GROUP BY column names are accessed via
    /// lookup_column() as normal.
    ///
    /// Unlike lookup_column(), this returns an option since the caller is
    /// expected to fall back to normal expressions building.
    fn lookup_aggregate(&self, expr: &ast::Expression) -> Option<usize> {
        self.aggregates.get(expr).copied()
    }

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
    fn project(&self, expressions: &[Expression], labels: &[Label]) -> Result<Self> {
        assert_eq!(expressions.len(), labels.len());

        // Copy the table index.
        let mut new = Scope::new();
        new.tables = self.tables.clone();

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
                new.add_column(label.clone()); // explicit label
            } else if let Some(f) = field_map.get(&i) {
                new.add_column(self.columns[*f].clone()); // field reference
            } else {
                new.add_column(Label::None); // unlabeled expression
            }
        }
        Ok(new)
    }
}
