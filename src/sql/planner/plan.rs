use super::optimizer::OPTIMIZERS;
use super::planner::Planner;
use crate::error::Result;
use crate::sql::engine::{Catalog, Transaction};
use crate::sql::execution::{self, ExecutionResult};
use crate::sql::parser::ast;
use crate::sql::types::{Expression, Label, Table, Value};

use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A statement execution plan. Primarily made up of a tree of query plan Nodes,
/// which process and stream rows, but the root nodes can perform data
/// modifications or schema changes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Plan {
    /// A CREATE TABLE plan. Creates a new table with the given schema. Errors
    /// if the table already exists or the schema is invalid.
    CreateTable { schema: Table },
    /// A DROP TABLE plan. Drops the given table. Errors if the table does not
    /// exist, unless if_exists is true.
    DropTable { table: String, if_exists: bool },
    /// A DELETE plan. Deletes rows in table that match the rows from source.
    /// primary_key specifies the primary key column index in source rows.
    Delete { table: String, primary_key: usize, source: Node },
    /// An INSERT plan. Inserts rows from source (typically a Values node) into
    /// table. If column_map is given, it maps table → source column indexes and
    /// must have one entry for every column in source. Columns not emitted by
    /// source will get the column's default value if any, otherwise error.
    Insert { table: Table, column_map: Option<HashMap<usize, usize>>, source: Node },
    /// An UPDATE plan. Updates rows in table that match the rows from source,
    /// where primary_key specifies the source row column index of primary keys
    /// to update. The given column/expression pairs specify the row updates to
    /// be made, and will be evaluated using the old row entry from source. Rows
    /// in source must be complete, existing rows from the table to update.
    ///
    /// TODO: the expressions string is only used when displaying the plan, i.e.
    /// "column = expr". Consider getting rid of it, or combining it with other
    /// label/column handling elsewhere.
    Update {
        table: String,
        primary_key: usize,
        source: Node,
        expressions: Vec<(usize, String, Expression)>,
    },
    /// A SELECT plan. Recursively executes the query plan tree and returns the
    /// resulting rows. Also includes the output column labels.
    Select { root: Node, labels: Vec<Label> },
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

    /// Optimizes the plan, consuming it.
    pub fn optimize(self) -> Result<Self> {
        let optimize = |mut node| -> Result<Node> {
            for (_, optimizer) in OPTIMIZERS {
                node = optimizer(node)?;
            }
            Ok(node)
        };
        Ok(match self {
            Self::CreateTable { .. } | Self::DropTable { .. } | Self::Insert { .. } => self,
            Self::Delete { table, primary_key, source } => {
                Self::Delete { table, primary_key, source: optimize(source)? }
            }
            Self::Update { table, primary_key, source, expressions } => {
                Self::Update { table, primary_key, source: optimize(source)?, expressions }
            }
            Self::Select { root, labels } => Self::Select { root: optimize(root)?, labels },
        })
    }
}

/// A query plan node. These return row iterators and can be nested.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    /// Computes aggregate values for the given expressions and group_by buckets
    /// across all rows in the source node. The group_by columns are emitted first,
    /// followed by the aggregate columns, in the given order.
    Aggregate { source: Box<Node>, group_by: Vec<Expression>, aggregates: Vec<Aggregate> },
    /// Filters source rows, by only emitting rows for which the predicate
    /// evaluates to true.
    Filter { source: Box<Node>, predicate: Expression },
    /// Joins the left and right sources on the given fields by building an
    /// in-memory hashmap of the right source and looking up matches for each
    /// row in the left source. When outer is true (e.g. LEFT JOIN), a left row
    /// without a right match is emitted anyway, with NULLs for the right row.
    /// TODO: do we need the labels?
    HashJoin {
        left: Box<Node>,
        left_field: usize,
        left_label: Label,
        right: Box<Node>,
        right_field: usize,
        right_label: Label,
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
    /// Nothing does not emit anything.
    Nothing,
    /// Discards the first offset rows from source, emits the rest.
    Offset { source: Box<Node>, offset: usize },
    /// Sorts the source rows by the given expression/direction pairs. Buffers
    /// the entire row set in memory.
    Order { source: Box<Node>, orders: Vec<(Expression, Direction)> },
    /// Projects the input rows by evaluating the given expressions.
    /// The labels are only used when displaying the plan.
    Projection { source: Box<Node>, expressions: Vec<Expression>, labels: Vec<Label> },
    /// Remaps source columns to the given target column index, or None to drop
    /// the column. Unspecified target columns yield Value::Null.
    Remap { source: Box<Node>, targets: Vec<Option<usize>> },
    /// A full table scan, with an optional filter pushdown. The schema is used
    /// during plan optimization. The alias is only used for formatting.
    Scan { table: Table, filter: Option<Expression>, alias: Option<String> },
    /// A constant set of values.
    Values { rows: Vec<Vec<Expression>> },
}

impl Node {
    /// Recursively transforms query nodes depth-first by applying the given
    /// closures before and after descending.
    pub fn transform<B, A>(mut self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Self) -> Result<Self>,
        A: Fn(Self) -> Result<Self>,
    {
        // Helper for transforming boxed nodes.
        let transform = |mut node: Box<Node>| -> Result<Box<Node>> {
            *node = node.transform(before, after)?;
            Ok(node)
        };

        self = before(self)?;
        self = match self {
            Self::Aggregate { source, aggregates, group_by } => {
                Self::Aggregate { source: transform(source)?, aggregates, group_by }
            }
            Self::Filter { source, predicate } => {
                Self::Filter { source: transform(source)?, predicate }
            }
            Self::HashJoin {
                left,
                left_field,
                left_label,
                right,
                right_field,
                right_label,
                outer,
            } => Self::HashJoin {
                left: transform(left)?,
                left_field,
                left_label,
                right: transform(right)?,
                right_field,
                right_label,
                outer,
            },
            Self::Limit { source, limit } => Self::Limit { source: transform(source)?, limit },
            Self::NestedLoopJoin { left, right, predicate, outer } => Self::NestedLoopJoin {
                left: transform(left)?,
                right: transform(right)?,
                predicate,
                outer,
            },
            Self::Offset { source, offset } => Self::Offset { source: transform(source)?, offset },
            Self::Order { source, orders } => Self::Order { source: transform(source)?, orders },
            Self::Projection { source, labels, expressions } => {
                Self::Projection { source: transform(source)?, labels, expressions }
            }
            Self::Remap { source, targets } => Self::Remap { source: transform(source)?, targets },

            node @ (Self::IndexLookup { .. }
            | Self::KeyLookup { .. }
            | Self::Nothing
            | Self::Scan { .. }
            | Self::Values { .. }) => node,
        };
        self = after(self)?;
        Ok(self)
    }

    /// Recursively transforms all expressions in a node by calling the given
    /// closures on them before and after descending.
    pub fn transform_expressions<B, A>(self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Expression) -> Result<Expression>,
        A: Fn(Expression) -> Result<Expression>,
    {
        Ok(match self {
            Self::Filter { source, predicate } => {
                Self::Filter { source, predicate: predicate.transform(before, after)? }
            }
            Self::Order { source, orders } => Self::Order {
                source,
                orders: orders
                    .into_iter()
                    .map(|(e, o)| e.transform(before, after).map(|e| (e, o)))
                    .collect::<Result<_>>()?,
            },
            Self::NestedLoopJoin { left, right, predicate: Some(predicate), outer } => {
                Self::NestedLoopJoin {
                    left,
                    right,
                    predicate: Some(predicate.transform(before, after)?),
                    outer,
                }
            }
            Self::Projection { source, labels, expressions } => Self::Projection {
                source,
                labels,
                expressions: expressions
                    .into_iter()
                    .map(|e| e.transform(before, after))
                    .collect::<Result<_>>()?,
            },
            Self::Scan { table, alias, filter: Some(filter) } => {
                Self::Scan { table, alias, filter: Some(filter.transform(before, after)?) }
            }
            Self::Values { rows } => Self::Values {
                rows: rows
                    .into_iter()
                    .map(|row| row.into_iter().map(|e| e.transform(before, after)).collect())
                    .collect::<Result<_>>()?,
            },

            node @ (Self::Aggregate { .. }
            | Self::HashJoin { .. }
            | Self::IndexLookup { .. }
            | Self::KeyLookup { .. }
            | Self::Limit { .. }
            | Self::NestedLoopJoin { predicate: None, .. }
            | Self::Nothing
            | Self::Offset { .. }
            | Self::Scan { filter: None, .. }
            | Self::Remap { .. }) => node,
        })
    }

    /// Returns the size of the node, i.e. the column width.
    pub fn size(&self) -> usize {
        match &self {
            Node::Aggregate { aggregates, group_by, .. } => aggregates.len() + group_by.len(),
            Node::Filter { source, .. } => source.size(),
            Node::HashJoin { left, right, .. } => left.size() + right.size(),
            Node::IndexLookup { table, .. } => table.columns.len(),
            Node::KeyLookup { table, .. } => table.columns.len(),
            Node::Limit { source, .. } => source.size(),
            Node::NestedLoopJoin { left, right, .. } => left.size() + right.size(),
            Node::Nothing => 0,
            Node::Offset { source, .. } => source.size(),
            Node::Order { source, .. } => source.size(),
            Node::Projection { expressions, .. } => expressions.len(),
            Node::Remap { targets, .. } => {
                targets.iter().filter_map(|v| *v).map(|i| i + 1).max().unwrap_or(0)
            }
            Node::Scan { table, .. } => table.columns.len(),
            Node::Values { rows } => rows.first().map(|row| row.len()).unwrap_or(0),
        }
    }
}

/// Formats the plan as an EXPLAIN tree.
impl std::fmt::Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTable { schema } => write!(f, "CreateTable: {}", schema.name),
            Self::DropTable { table, .. } => write!(f, "DropTable: {table}"),
            Self::Delete { table, source, .. } => {
                write!(f, "Delete: {table}")?;
                source.format(f, String::new(), false, true)
            }
            Self::Insert { table, source, .. } => {
                write!(f, "Insert: {}", table.name)?;
                source.format(f, String::new(), false, true)
            }
            Self::Update { table, source, expressions, .. } => {
                let expressions = expressions.iter().map(|(_, l, e)| format!("{l}={e}")).join(",");
                write!(f, "Update: {table} ({expressions})")?;
                source.format(f, String::new(), false, true)
            }
            Self::Select { root, .. } => root.fmt(f),
        }
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format(f, String::new(), true, true)
    }
}

impl Node {
    /// Recursively formats the node. Prefix is used for tree branches. root is
    /// true if this is the root (first) node, and last is used if this is the
    /// last node in this branch.
    pub fn format(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        mut prefix: String,
        root: bool,
        last: bool,
    ) -> std::fmt::Result {
        // If this is not the root node, emit a newline after the previous node.
        // This avoids a spurious newline at the end of the plan.
        if !root {
            writeln!(f)?;
        }

        // Prefix the node with a tree branch line. Modify the prefix for any
        // child nodes we'll recurse into.
        write!(f, "{prefix}")?;
        if !last {
            write!(f, "├─ ")?;
            prefix += "│  "
        } else if !root {
            write!(f, "└─ ")?;
            prefix += "   ";
        }

        // Format the node.
        match self {
            Self::Aggregate { source, aggregates, group_by } => {
                let aggregates = group_by
                    .iter()
                    .map(|group| format!("group({group})"))
                    .chain(aggregates.iter().map(|agg| agg.to_string()))
                    .join(", ");
                write!(f, "Aggregate: {aggregates}")?;
                source.format(f, prefix, false, true)?;
            }
            Self::Filter { source, predicate } => {
                write!(f, "Filter: {predicate}")?;
                source.format(f, prefix, false, true)?;
            }
            Self::HashJoin {
                left,
                left_field,
                left_label,
                right,
                right_field,
                right_label,
                outer,
                ..
            } => {
                let kind = if *outer { "outer" } else { "inner" };
                let left_label = match left_label {
                    Label::None => format!("left #{left_field}"),
                    label => format!("{label}"),
                };
                let right_label = match right_label {
                    Label::None => format!("right #{right_field}"),
                    label => format!("{label}"),
                };
                write!(f, "HashJoin: {kind} on {left_label} = {right_label}")?;
                left.format(f, prefix.clone(), false, false)?;
                right.format(f, prefix, false, true)?;
            }
            Self::IndexLookup { table, column, alias, values } => {
                let column = &table.columns[*column].name;
                let table = &table.name;
                write!(f, "IndexLookup: {table}.{column}")?;
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
                source.format(f, prefix, false, true)?;
            }
            Self::NestedLoopJoin { left, right, predicate, outer, .. } => {
                write!(f, "NestedLoopJoin: {}", if *outer { "outer" } else { "inner" })?;
                if let Some(expr) = predicate {
                    write!(f, " on {expr}")?;
                }
                left.format(f, prefix.clone(), false, false)?;
                right.format(f, prefix, false, true)?;
            }
            Self::Nothing => {
                write!(f, "Nothing")?;
            }
            Self::Offset { source, offset } => {
                write!(f, "Offset: {offset}")?;
                source.format(f, prefix, false, true)?;
            }
            Self::Order { source, orders } => {
                let orders = orders.iter().map(|(expr, dir)| format!("{expr} {dir}")).join(", ");
                write!(f, "Order: {orders}")?;
                source.format(f, prefix, false, true)?;
            }
            Self::Projection { source, expressions, .. } => {
                write!(f, "Projection: {}", expressions.iter().join(", "))?;
                source.format(f, prefix, false, true)?;
            }
            Self::Remap { source, targets } => {
                let remap = remap_sources(targets)
                    .into_iter()
                    .map(|from| from.map(|from| format!("#{from}")).unwrap_or("Null".to_string()))
                    .join(", ");
                write!(f, "Remap: {remap}")?;
                let dropped = targets
                    .iter()
                    .enumerate()
                    .filter_map(|(i, v)| v.is_none().then_some(format!("#{i}")))
                    .join(", ");
                if !dropped.is_empty() {
                    write!(f, " (dropped: {dropped})")?;
                }
                source.format(f, prefix, false, true)?;
            }
            Self::Scan { table, alias, filter } => {
                write!(f, "Scan: {}", table.name)?;
                if let Some(alias) = alias {
                    write!(f, " as {alias}")?;
                }
                if let Some(expr) = filter {
                    write!(f, " ({expr})")?;
                }
            }
            Self::Values { rows, .. } => {
                write!(f, "Values: ")?;
                match rows.len() {
                    1 if rows[0].is_empty() => write!(f, "blank row")?,
                    1 => write!(f, "{}", rows[0].iter().map(|e| e.to_string()).join(", "))?,
                    n => write!(f, "{n} rows")?,
                }
            }
        };
        Ok(())
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

impl std::fmt::Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Average(expr) => write!(f, "avg({expr})"),
            Self::Count(expr) => write!(f, "count({expr})"),
            Self::Max(expr) => write!(f, "max({expr})"),
            Self::Min(expr) => write!(f, "min({expr})"),
            Self::Sum(expr) => write!(f, "sum({expr})"),
        }
    }
}

/// A sort order direction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ascending => write!(f, "asc"),
            Self::Descending => write!(f, "desc"),
        }
    }
}

impl From<ast::Order> for Direction {
    fn from(order: ast::Order) -> Self {
        match order {
            ast::Order::Ascending => Self::Ascending,
            ast::Order::Descending => Self::Descending,
        }
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
