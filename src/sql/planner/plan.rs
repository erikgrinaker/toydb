use super::optimizer::{self, Optimizer as _};
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
#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
            node = optimizer::ConstantFolder.optimize(node)?;
            node = optimizer::FilterPushdown.optimize(node)?;
            node = optimizer::IndexLookup.optimize(node)?;
            node = optimizer::NoopCleaner.optimize(node)?;
            node = optimizer::JoinType.optimize(node)?;
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
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    /// Aggregates the input rows by computing the given aggregates for the
    /// corresponding source columns. Additional columns in source for which
    /// there are no aggregates are used as group by buckets, the number of
    /// such group by columns is given in group_by.
    ///
    /// TODO: consider making aggregates a field/aggregate pair.
    Aggregation { source: Box<Node>, aggregates: Vec<Aggregate>, group_by: usize },
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
        right_size: usize,
        outer: bool,
    },
    /// Looks up the given values in a secondary index and emits matching rows.
    IndexLookup { table: String, column: String, values: Vec<Value>, alias: Option<String> },
    /// Looks up the given primary keys and emits their rows.
    KeyLookup { table: String, keys: Vec<Value>, alias: Option<String> },
    /// Only emits the first limit rows from the source, discards the rest.
    Limit { source: Box<Node>, limit: usize },
    /// Joins the left and right sources on the given predicate by buffering the
    /// right source and iterating over it for every row in the left source.
    /// When outer is true (e.g. LEFT JOIN), a left row without a right match is
    /// emitted anyway, with NULLs for the right row.
    /// TODO: do we need left_size?
    NestedLoopJoin {
        left: Box<Node>,
        left_size: usize,
        right: Box<Node>,
        right_size: usize,
        predicate: Option<Expression>,
        outer: bool,
    },
    /// Emits a single empty row. Used for SELECT queries with no FROM clause.
    /// TODO: replace with Values, but requires changes to aggregate planning.
    Nothing,
    /// Discards the first offset rows from source, emits the rest.
    Offset { source: Box<Node>, offset: usize },
    /// Sorts the source rows by the given expression/direction pairs. Buffers
    /// the entire row set in memory.
    Order { source: Box<Node>, orders: Vec<(Expression, Direction)> },
    /// Projects the input rows by evaluating the given expressions.
    /// The labels are only used when displaying the plan.
    Projection { source: Box<Node>, expressions: Vec<Expression>, labels: Vec<Label> },
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
            Self::Aggregation { source, aggregates, group_by } => {
                Self::Aggregation { source: transform(source)?, aggregates, group_by }
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
                right_size,
                outer,
            } => Self::HashJoin {
                left: transform(left)?,
                left_field,
                left_label,
                right: transform(right)?,
                right_field,
                right_label,
                right_size,
                outer,
            },
            Self::Limit { source, limit } => Self::Limit { source: transform(source)?, limit },
            Self::NestedLoopJoin { left, left_size, right, right_size, predicate, outer } => {
                Self::NestedLoopJoin {
                    left: transform(left)?,
                    left_size,
                    right: transform(right)?,
                    right_size,
                    predicate,
                    outer,
                }
            }
            Self::Offset { source, offset } => Self::Offset { source: transform(source)?, offset },
            Self::Order { source, orders } => Self::Order { source: transform(source)?, orders },
            Self::Projection { source, labels, expressions } => {
                Self::Projection { source: transform(source)?, labels, expressions }
            }

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
            Self::NestedLoopJoin {
                left,
                left_size,
                right,
                right_size,
                predicate: Some(predicate),
                outer,
            } => Self::NestedLoopJoin {
                left,
                left_size,
                right,
                right_size,
                predicate: Some(predicate.transform(before, after)?),
                outer,
            },
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

            node @ Self::Aggregation { .. }
            | node @ Self::HashJoin { .. }
            | node @ Self::IndexLookup { .. }
            | node @ Self::KeyLookup { .. }
            | node @ Self::Limit { .. }
            | node @ Self::NestedLoopJoin { predicate: None, .. }
            | node @ Self::Nothing
            | node @ Self::Offset { .. }
            | node @ Self::Scan { filter: None, .. } => node,
        })
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
            Self::Aggregation { source, aggregates, .. } => {
                write!(f, "Aggregation: {}", aggregates.iter().join(", "))?;
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
                write!(f, "IndexLookup: {table}")?;
                if let Some(alias) = alias {
                    write!(f, " as {}", alias)?;
                }
                write!(f, " column {}", column)?;
                if !values.is_empty() && values.len() < 10 {
                    write!(f, " ({})", values.iter().join(", "))?;
                } else {
                    write!(f, " ({} values)", values.len())?;
                }
            }
            Self::KeyLookup { table, alias, keys } => {
                write!(f, "KeyLookup: {table}")?;
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
            Self::Nothing {} => {
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
                write!(f, "Values: {} rows", rows.len())?;
            }
        };
        Ok(())
    }
}

/// An aggregation function.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Aggregate {
    Average,
    Count,
    Max,
    Min,
    Sum,
}

impl std::fmt::Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Average => write!(f, "avg"),
            Self::Count => write!(f, "count"),
            Self::Max => write!(f, "max"),
            Self::Min => write!(f, "min"),
            Self::Sum => write!(f, "sum"),
        }
    }
}

impl Aggregate {
    pub(super) fn from_name(name: &str) -> Option<Self> {
        match name {
            "avg" => Some(Aggregate::Average),
            "count" => Some(Aggregate::Count),
            "max" => Some(Aggregate::Max),
            "min" => Some(Aggregate::Min),
            "sum" => Some(Aggregate::Sum),
            _ => None,
        }
    }
}

/// A sort order direction.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
