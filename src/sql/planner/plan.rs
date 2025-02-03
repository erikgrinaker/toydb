use std::collections::HashMap;
use std::fmt::Display;

use itertools::Itertools as _;
use serde::{Deserialize, Serialize};

use super::optimizer::OPTIMIZERS;
use super::planner::Planner;
use crate::error::Result;
use crate::sql::engine::{Catalog, Transaction};
use crate::sql::execution::{self, ExecutionResult};
use crate::sql::parser::ast;
use crate::sql::types::{Expression, Label, Table, Value};

/// A statement execution plan. The root nodes can perform data modifications or
/// schema changes, in addition to SELECT queries. Beyond the root, the plan is
/// made up of a tree of inner plan nodes that stream and process rows via
/// iterators. Below is an example of an (unoptimized) plan for the given query:
///
/// SELECT title, released, genres.name AS genre
/// FROM movies INNER JOIN genres ON movies.genre_id = genres.id
/// WHERE released >= 2000 ORDER BY released
///
/// Order: movies.released desc
/// └─ Projection: movies.title, movies.released, genres.name as genre
///    └─ Filter: movies.released >= 2000
///       └─ NestedLoopJoin: inner on movies.genre_id = genres.id
///          ├─ Scan: movies
///          └─ Scan: genres
///
/// Rows flow from the tree leaves to the root. The Scan nodes read and emit
/// table rows from storage. They are passed to the NestedLoopJoin node which
/// joins the rows from the two tables, then the Filter node discards old
/// movies, the Projection node picks out the requested columns, and the Order
/// node sorts them before emitting the rows to the client.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Plan {
    /// A CREATE TABLE plan. Creates a new table with the given schema. Errors
    /// if the table already exists or the schema is invalid.
    CreateTable { schema: Table },
    /// A DROP TABLE plan. Drops the given table. Errors if the table does not
    /// exist, unless if_exists is true.
    DropTable { table: String, if_exists: bool },
    /// A DELETE plan. Deletes rows in table that match the rows from source.
    /// primary_key specifies the primary key column index in the source rows.
    Delete { table: String, primary_key: usize, source: Node },
    /// An INSERT plan. Inserts rows from source (typically a Values node) into
    /// table. If column_map is given, it maps table → source column indexes and
    /// must have one entry for every column in source. Table columns not
    /// present in source will get the column's default value if set, or error.
    Insert { table: Table, column_map: Option<HashMap<usize, usize>>, source: Node },
    /// An UPDATE plan. Updates rows in table that match the rows from source,
    /// where primary_key specifies the primary key column index in the source
    /// rows. The given column/expression pairs specify the row updates to make,
    /// evaluated using the existing source row, which must be a complete row
    /// from the update table.
    Update { table: Table, primary_key: usize, source: Node, expressions: Vec<(usize, Expression)> },
    /// A SELECT plan. Recursively executes the query plan tree and returns the
    /// resulting rows.
    Select(Node),
}

impl Plan {
    /// Builds a plan from an AST statement.
    pub fn build(statement: ast::Statement, catalog: &impl Catalog) -> Result<Self> {
        Planner::new(catalog).build(statement)
    }

    /// Executes the plan, consuming it.
    pub fn execute(self, txn: &(impl Transaction + Catalog)) -> Result<ExecutionResult> {
        execution::execute_plan(self, txn, txn)
    }

    /// Optimizes the plan, consuming it. See OPTIMIZERS for the list of
    /// optimizers.
    pub fn optimize(self) -> Result<Self> {
        let optimize = |node| OPTIMIZERS.iter().try_fold(node, |node, (_, opt)| opt(node));
        Ok(match self {
            Self::CreateTable { .. } | Self::DropTable { .. } => self,
            Self::Delete { table, primary_key, source } => {
                Self::Delete { table, primary_key, source: optimize(source)? }
            }
            Self::Insert { table, column_map, source } => {
                Self::Insert { table, column_map, source: optimize(source)? }
            }
            Self::Update { table, primary_key, source, expressions } => {
                Self::Update { table, primary_key, source: optimize(source)?, expressions }
            }
            Self::Select(root) => Self::Select(optimize(root)?),
        })
    }
}

/// A query plan node. Returns a row iterator, and can be nested.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    /// Computes the given aggregate values for the given group_by buckets
    /// across all rows in the source node. The group_by columns are emitted
    /// first, followed by the aggregate columns, in the given order.
    Aggregate { source: Box<Node>, group_by: Vec<Expression>, aggregates: Vec<Aggregate> },
    /// Filters source rows, by discarding rows for which the predicate
    /// evaluates to false.
    Filter { source: Box<Node>, predicate: Expression },
    /// Joins the left and right sources on the given columns by building an
    /// in-memory hashmap of the right source and looking up matches for each
    /// row in the left source. When outer is true (e.g. LEFT JOIN), a left row
    /// without a right match is emitted anyway, with NULLs for the right row.
    HashJoin {
        left: Box<Node>,
        left_column: usize,
        right: Box<Node>,
        right_column: usize,
        outer: bool,
    },
    /// Looks up the given values in a secondary index and emits matching rows.
    /// NULL and NaN values are considered equal, to allow IS NULL and IS NAN
    /// index lookups, as is -0.0 and 0.0.
    IndexLookup { table: Table, column: usize, values: Vec<Value>, alias: Option<String> },
    /// Looks up the given primary keys and emits their rows.
    KeyLookup { table: Table, keys: Vec<Value>, alias: Option<String> },
    /// Only emits the first limit rows from the source, discards the rest.
    Limit { source: Box<Node>, limit: usize },
    /// Joins the left and right sources on the given predicate by buffering the
    /// right source and iterating over it for every row in the left source.
    /// When outer is true (e.g. LEFT JOIN), a left row without a right match is
    /// emitted anyway, with NULLs for the right row.
    NestedLoopJoin { left: Box<Node>, right: Box<Node>, predicate: Option<Expression>, outer: bool },
    /// Nothing does not emit anything, and is used to short-circuit nodes that
    /// can't emit anything during optimization. It retains the column names of
    /// any replaced nodes for results headers and plan formatting.
    Nothing { columns: Vec<Label> },
    /// Discards the first offset rows from source, emits the rest.
    Offset { source: Box<Node>, offset: usize },
    /// Sorts the source rows by the given sort key. Buffers the entire row set
    /// in memory.
    Order { source: Box<Node>, key: Vec<(Expression, Direction)> },
    /// Projects the input rows by evaluating the given expressions. Aliases are
    /// only used when displaying the plan.
    Projection { source: Box<Node>, expressions: Vec<Expression>, aliases: Vec<Label> },
    /// Remaps source columns to the given target column index, or None to drop
    /// the column. Unspecified target columns yield Value::Null. The source →
    /// target mapping ensures a source column can only be mapped to a single
    /// target column, allowing the value to be moved rather than cloned.
    Remap { source: Box<Node>, targets: Vec<Option<usize>> },
    /// A full table scan, with an optional pushed-down filter. The schema is
    /// used during plan optimization. The alias is only used for formatting.
    Scan { table: Table, filter: Option<Expression>, alias: Option<String> },
    /// A constant set of values.
    Values { rows: Vec<Vec<Expression>> },
}

impl Node {
    /// Returns the number of columns emitted by the node.
    pub fn columns(&self) -> usize {
        match self {
            // Source nodes emit all table columns.
            Self::IndexLookup { table, .. }
            | Self::KeyLookup { table, .. }
            | Self::Scan { table, .. } => table.columns.len(),

            // Some nodes modify the column set.
            Self::Aggregate { aggregates, group_by, .. } => aggregates.len() + group_by.len(),
            Self::Projection { expressions, .. } => expressions.len(),
            Self::Remap { targets, .. } => {
                targets.iter().filter_map(|v| *v).map(|i| i + 1).max().unwrap_or(0)
            }

            // Join nodes emit the combined columns.
            Self::HashJoin { left, right, .. } | Self::NestedLoopJoin { left, right, .. } => {
                left.columns() + right.columns()
            }

            // Simple nodes just pass through the source columns.
            Self::Filter { source, .. }
            | Self::Limit { source, .. }
            | Self::Offset { source, .. }
            | Self::Order { source, .. } => source.columns(),

            // And some are trivial.
            Self::Nothing { columns } => columns.len(),
            Self::Values { rows } => rows.first().map(|row| row.len()).unwrap_or(0),
        }
    }

    /// Returns a label for a column, if any, by tracing the column through the
    /// plan tree. Only used for query result headers and plan display purposes,
    /// not to look up expression columns (see Scope).
    pub fn column_label(&self, index: usize) -> Label {
        match self {
            // Source nodes use the table/column name.
            Self::IndexLookup { table, alias, .. }
            | Self::KeyLookup { table, alias, .. }
            | Self::Scan { table, alias, .. } => Label::Qualified(
                alias.as_ref().unwrap_or(&table.name).clone(),
                table.columns[index].name.clone(),
            ),

            // Some nodes rearrange columns. Route them to the correct
            // upstream column where appropriate.
            Self::Aggregate { source, group_by, .. } => match group_by.get(index) {
                Some(Expression::Column(index)) => source.column_label(*index),
                Some(_) | None => Label::None,
            },
            Self::Projection { source, expressions, aliases } => match aliases.get(index) {
                Some(Label::None) | None => match expressions.get(index) {
                    // Unaliased column references route to the source.
                    Some(Expression::Column(index)) => source.column_label(*index),
                    // Unaliased expressions don't have a name.
                    Some(_) | None => Label::None,
                },
                // Aliased columns use the alias.
                Some(alias) => alias.clone(),
            },
            Self::Remap { source, targets } => targets
                .iter()
                .position(|t| t == &Some(index))
                .map(|i| source.column_label(i))
                .unwrap_or(Label::None),

            // Joins dispatch to the appropriate source.
            Self::HashJoin { left, right, .. } | Self::NestedLoopJoin { left, right, .. } => {
                if index < left.columns() {
                    left.column_label(index)
                } else {
                    right.column_label(index - left.columns())
                }
            }

            // Simple nodes just dispatch to the source.
            Self::Filter { source, .. }
            | Self::Limit { source, .. }
            | Self::Offset { source, .. }
            | Self::Order { source, .. } => source.column_label(index),

            // Nothing nodes contain the original columns of replaced nodes.
            Self::Nothing { columns } => columns.get(index).cloned().unwrap_or(Label::None),

            // And some don't have any names at all.
            Self::Values { .. } => Label::None,
        }
    }

    /// Recursively transforms query nodes depth-first by applying the given
    /// closures before and after descending.
    pub fn transform(
        mut self,
        before: &impl Fn(Self) -> Result<Self>,
        after: &impl Fn(Self) -> Result<Self>,
    ) -> Result<Self> {
        // Helper for transforming boxed nodes.
        let xform = |mut node: Box<Node>| -> Result<Box<Node>> {
            *node = node.transform(before, after)?;
            Ok(node)
        };

        self = before(self)?;
        self = match self {
            Self::Aggregate { source, group_by, aggregates } => {
                Self::Aggregate { source: xform(source)?, group_by, aggregates }
            }
            Self::Filter { source, predicate } => {
                Self::Filter { source: xform(source)?, predicate }
            }
            Self::HashJoin { left, left_column, right, right_column, outer } => Self::HashJoin {
                left: xform(left)?,
                left_column,
                right: xform(right)?,
                right_column,
                outer,
            },
            Self::Limit { source, limit } => Self::Limit { source: xform(source)?, limit },
            Self::NestedLoopJoin { left, right, predicate, outer } => {
                Self::NestedLoopJoin { left: xform(left)?, right: xform(right)?, predicate, outer }
            }
            Self::Offset { source, offset } => Self::Offset { source: xform(source)?, offset },
            Self::Order { source, key } => Self::Order { source: xform(source)?, key },
            Self::Projection { source, expressions, aliases } => {
                Self::Projection { source: xform(source)?, expressions, aliases }
            }
            Self::Remap { source, targets } => Self::Remap { source: xform(source)?, targets },

            Self::IndexLookup { .. }
            | Self::KeyLookup { .. }
            | Self::Nothing { .. }
            | Self::Scan { .. }
            | Self::Values { .. } => self,
        };
        self = after(self)?;
        Ok(self)
    }

    /// Recursively transforms all node expressions by calling the given
    /// closures on them before and after descending.
    pub fn transform_expressions(
        self,
        before: &impl Fn(Expression) -> Result<Expression>,
        after: &impl Fn(Expression) -> Result<Expression>,
    ) -> Result<Self> {
        Ok(match self {
            Self::Filter { source, mut predicate } => {
                predicate = predicate.transform(before, after)?;
                Self::Filter { source, predicate }
            }
            Self::NestedLoopJoin { left, right, predicate: Some(predicate), outer } => {
                let predicate = Some(predicate.transform(before, after)?);
                Self::NestedLoopJoin { left, right, predicate, outer }
            }
            Self::Order { source, mut key } => {
                key = key
                    .into_iter()
                    .map(|(expr, dir)| Ok((expr.transform(before, after)?, dir)))
                    .collect::<Result<_>>()?;
                Self::Order { source, key }
            }
            Self::Projection { source, mut expressions, aliases } => {
                expressions = expressions
                    .into_iter()
                    .map(|expr| expr.transform(before, after))
                    .try_collect()?;
                Self::Projection { source, expressions, aliases }
            }
            Self::Scan { table, alias, filter: Some(filter) } => {
                let filter = Some(filter.transform(before, after)?);
                Self::Scan { table, alias, filter }
            }
            Self::Values { mut rows } => {
                rows = rows
                    .into_iter()
                    .map(|row| row.into_iter().map(|expr| expr.transform(before, after)).collect())
                    .try_collect()?;
                Self::Values { rows }
            }

            Self::Aggregate { .. }
            | Self::HashJoin { .. }
            | Self::IndexLookup { .. }
            | Self::KeyLookup { .. }
            | Self::Limit { .. }
            | Self::NestedLoopJoin { predicate: None, .. }
            | Self::Nothing { .. }
            | Self::Offset { .. }
            | Self::Remap { .. }
            | Self::Scan { filter: None, .. } => self,
        })
    }
}

/// An aggregate function.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Aggregate {
    Average(Expression),
    Count(Expression),
    Max(Expression),
    Min(Expression),
    Sum(Expression),
}

impl Aggregate {
    fn format(&self, node: &Node) -> String {
        match self {
            Self::Average(expr) => format!("avg({})", expr.format(node)),
            Self::Count(expr) => format!("count({})", expr.format(node)),
            Self::Max(expr) => format!("max({})", expr.format(node)),
            Self::Min(expr) => format!("min({})", expr.format(node)),
            Self::Sum(expr) => format!("sum({})", expr.format(node)),
        }
    }
}

/// A sort order direction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ascending => f.write_str("asc"),
            Self::Descending => f.write_str("desc"),
        }
    }
}

impl From<ast::Direction> for Direction {
    fn from(dir: ast::Direction) -> Self {
        match dir {
            ast::Direction::Ascending => Self::Ascending,
            ast::Direction::Descending => Self::Descending,
        }
    }
}

/// Formats the plan as an EXPLAIN tree.
impl Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTable { schema } => write!(f, "CreateTable: {}", schema.name),
            Self::DropTable { table, .. } => write!(f, "DropTable: {table}"),
            Self::Delete { table, source, .. } => {
                write!(f, "Delete: {table}")?;
                source.format(f, "", false, true)
            }
            Self::Insert { table, source, .. } => {
                write!(f, "Insert: {}", table.name)?;
                source.format(f, "", false, true)
            }
            Self::Update { table, source, expressions, .. } => {
                let expressions = expressions
                    .iter()
                    .map(|(i, expr)| format!("{}={}", table.columns[*i].name, expr.format(source)))
                    .join(", ");
                write!(f, "Update: {} ({expressions})", table.name)?;
                source.format(f, "", false, true)
            }
            Self::Select(root) => root.format(f, "", true, true),
        }
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format(f, "", true, true)
    }
}

impl Node {
    /// Recursively formats the node. Prefix is used for tree branch lines. root
    /// is true if this is the root (first) node, and last_child is true if this
    /// is the last child node of the parent.
    pub fn format(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        prefix: &str,
        root: bool,
        last_child: bool,
    ) -> std::fmt::Result {
        // If this is not the root node, emit a newline after the previous node.
        // This avoids a spurious newline at the end of the plan.
        if !root {
            writeln!(f)?;
        }

        // Prefix the node with a tree branch line. Modify the prefix for any
        // child nodes we'll recurse into.
        let prefix = if !last_child {
            write!(f, "{prefix}├─ ")?;
            format!("{prefix}│  ")
        } else if !root {
            write!(f, "{prefix}└─ ")?;
            format!("{prefix}   ")
        } else {
            write!(f, "{prefix}")?;
            prefix.to_string()
        };

        // Format the node.
        match self {
            Self::Aggregate { source, aggregates, group_by } => {
                let aggregates = group_by
                    .iter()
                    .map(|group_by| group_by.format(source))
                    .chain(aggregates.iter().map(|agg| agg.format(source)))
                    .join(", ");
                write!(f, "Aggregate: {aggregates}")?;
                source.format(f, &prefix, false, true)?;
            }
            Self::Filter { source, predicate } => {
                write!(f, "Filter: {}", predicate.format(source))?;
                source.format(f, &prefix, false, true)?;
            }
            Self::HashJoin { left, left_column, right, right_column, outer } => {
                let kind = if *outer { "outer" } else { "inner" };
                let left_column = match left.column_label(*left_column) {
                    Label::None => format!("left #{left_column}"),
                    label => format!("{label}"),
                };
                let right_column = match right.column_label(*right_column) {
                    Label::None => format!("right #{right_column}"),
                    label => format!("{label}"),
                };
                write!(f, "HashJoin: {kind} on {left_column} = {right_column}")?;
                left.format(f, &prefix, false, false)?;
                right.format(f, &prefix, false, true)?;
            }
            Self::IndexLookup { table, column, alias, values } => {
                let column = &table.columns[*column].name;
                write!(f, "IndexLookup: {}.{column}", table.name)?;
                if let Some(alias) = alias {
                    write!(f, " as {alias}.{column}")?;
                }
                if !values.is_empty() && values.len() < 10 {
                    write!(f, " ({})", values.iter().join(", "))?;
                } else {
                    write!(f, " ({} values)", values.len())?;
                }
            }
            Self::KeyLookup { table, alias, keys } => {
                write!(f, "KeyLookup: {}", table.name)?;
                if let Some(alias) = alias {
                    write!(f, " as {alias}")?;
                }
                if !keys.is_empty() && keys.len() < 10 {
                    write!(f, " ({})", keys.iter().join(", "))?;
                } else {
                    write!(f, " ({} keys)", keys.len())?;
                }
            }
            Self::Limit { source, limit } => {
                write!(f, "Limit: {limit}")?;
                source.format(f, &prefix, false, true)?;
            }
            Self::NestedLoopJoin { left, right, predicate, outer, .. } => {
                let kind = if *outer { "outer" } else { "inner" };
                write!(f, "NestedLoopJoin: {kind}")?;
                if let Some(predicate) = predicate {
                    write!(f, " on {}", predicate.format(self))?;
                }
                left.format(f, &prefix, false, false)?;
                right.format(f, &prefix, false, true)?;
            }
            Self::Nothing { .. } => write!(f, "Nothing")?,
            Self::Offset { source, offset } => {
                write!(f, "Offset: {offset}")?;
                source.format(f, &prefix, false, true)?;
            }
            Self::Order { source, key: orders } => {
                let orders = orders
                    .iter()
                    .map(|(expr, dir)| format!("{} {dir}", expr.format(source)))
                    .join(", ");
                write!(f, "Order: {orders}")?;
                source.format(f, &prefix, false, true)?;
            }
            Self::Projection { source, expressions, aliases } => {
                let expressions = expressions
                    .iter()
                    .enumerate()
                    .map(|(i, expr)| match aliases.get(i) {
                        Some(Label::None) | None => expr.format(source),
                        Some(alias) => format!("{} as {alias}", expr.format(source)),
                    })
                    .join(", ");
                write!(f, "Projection: {expressions}")?;
                source.format(f, &prefix, false, true)?;
            }
            Self::Remap { source, targets } => {
                let remap = remap_sources(targets)
                    .into_iter()
                    .map(|from| match from {
                        Some(from) => match source.column_label(from) {
                            Label::None => format!("#{from}"),
                            label => label.to_string(),
                        },
                        None => "Null".to_string(),
                    })
                    .join(", ");
                write!(f, "Remap: {remap}")?;
                let dropped = targets
                    .iter()
                    .enumerate()
                    .filter_map(|(i, v)| {
                        v.is_none().then_some(match source.column_label(i) {
                            Label::None => format!("#{i}"),
                            label => format!("{label}"),
                        })
                    })
                    .join(", ");
                if !dropped.is_empty() {
                    write!(f, " (dropped: {dropped})")?;
                }
                source.format(f, &prefix, false, true)?;
            }
            Self::Scan { table, alias, filter } => {
                write!(f, "Scan: {}", table.name)?;
                if let Some(alias) = alias {
                    write!(f, " as {alias}")?;
                }
                if let Some(filter) = filter {
                    write!(f, " ({})", filter.format(self))?;
                }
            }
            Self::Values { rows, .. } => {
                write!(f, "Values: ")?;
                match rows.len() {
                    1 if rows[0].is_empty() => write!(f, "blank row")?,
                    1 => write!(f, "{}", rows[0].iter().map(|e| e.format(self)).join(", "))?,
                    n => write!(f, "{n} rows")?,
                }
            }
        };
        Ok(())
    }
}

/// Inverts a Remap targets vector to a vector of source indexes, with None
/// for columns that weren't targeted.
pub fn remap_sources(targets: &[Option<usize>]) -> Vec<Option<usize>> {
    let size = targets.iter().filter_map(|v| *v).map(|i| i + 1).max().unwrap_or(0);
    let mut sources = vec![None; size];
    for (from, to) in targets.iter().enumerate() {
        if let Some(to) = to {
            sources[*to] = Some(from);
        }
    }
    sources
}
