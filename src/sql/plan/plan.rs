#![allow(clippy::module_inception)]

use super::optimizer::{self, Optimizer as _};
use super::planner::Planner;
use crate::error::Result;
use crate::sql::engine::{Catalog, Transaction};
use crate::sql::execution::{self, ExecutionResult};
use crate::sql::parser::ast;
use crate::sql::types::schema::Table;
use crate::sql::types::{Expression, Value};

use serde::{Deserialize, Serialize};

/// A statement execution plan. These are mostly made up of nested query plan
/// Nodes, which stream rows, but the root nodes can also perform data
/// modifications or schema changes.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Plan {
    /// A CREATE TABLE plan.
    CreateTable { schema: Table },
    /// A DROP TABLE plan.
    DropTable { table: String, if_exists: bool },
    /// A DELETE plan.
    Delete { table: String, source: Node },
    /// An INSERT plan.
    /// TODO: consider using a source which generates expression rows.
    Insert { table: String, columns: Vec<String>, expressions: Vec<Vec<Expression>> },
    /// An UPDATE plan.
    Update { table: String, source: Node, expressions: Vec<(usize, Option<String>, Expression)> },
    /// A SELECT plan.
    Select(Node),
}

impl Plan {
    /// Builds a plan from an AST statement.
    pub fn build<C: Catalog>(statement: ast::Statement, catalog: &mut C) -> Result<Self> {
        Planner::new(catalog).build(statement)
    }

    /// Executes the plan, consuming it.
    pub fn execute(self, txn: &mut impl Transaction) -> Result<ExecutionResult> {
        execution::execute_plan(self, txn)
    }

    /// Optimizes the plan, consuming it.
    pub fn optimize<C: Catalog>(self, catalog: &mut C) -> Result<Self> {
        let mut optimize = |mut node| -> Result<Node> {
            node = optimizer::ConstantFolder.optimize(node)?;
            node = optimizer::FilterPushdown.optimize(node)?;
            node = optimizer::IndexLookup::new(catalog).optimize(node)?;
            node = optimizer::NoopCleaner.optimize(node)?;
            node = optimizer::JoinType.optimize(node)?;
            Ok(node)
        };
        Ok(match self {
            Self::CreateTable { .. } | Self::DropTable { .. } | Self::Insert { .. } => self,
            Self::Delete { table, source } => Self::Delete { table, source: optimize(source)? },
            Self::Update { table, source, expressions } => {
                Self::Update { table, source: optimize(source)?, expressions }
            }
            Self::Select(root) => Self::Select(optimize(root)?),
        })
    }
}

// TODO: this needs testing and cleaning up.
impl std::fmt::Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTable { schema } => write!(f, "CreateTable: {}", schema.name),
            Self::DropTable { table, if_exists: _ } => write!(f, "DropTable: {table}"),
            Self::Delete { source, table } => {
                write!(
                    f,
                    "Delete: {table}\n{}",
                    source.format(String::new(), false, true).trim_end()
                )
            }
            Self::Insert { table, columns: _, expressions } => {
                write!(f, "Insert: {table} ({} rows)", expressions.len())
            }
            Self::Select(root) => root.fmt(f),
            Self::Update { source, table, expressions } => {
                write!(
                    f,
                    "Update: {table} ({})\n{}",
                    expressions
                        .iter()
                        .map(|(i, l, e)| format!(
                            "{}={}",
                            l.clone().unwrap_or_else(|| format!("#{}", i)),
                            e
                        ))
                        .collect::<Vec<_>>()
                        .join(","),
                    source.format(String::new(), false, true).trim_end()
                )
            }
        }
    }
}

/// A query plan node. These return row iterators and can be nested.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    Aggregation {
        source: Box<Node>,
        aggregates: Vec<Aggregate>,
    },
    Filter {
        source: Box<Node>,
        predicate: Expression,
    },
    HashJoin {
        left: Box<Node>,
        left_field: (usize, Option<(Option<String>, String)>),
        right: Box<Node>,
        right_field: (usize, Option<(Option<String>, String)>),
        outer: bool,
    },
    IndexLookup {
        table: String,
        alias: Option<String>,
        column: String,
        values: Vec<Value>,
    },
    KeyLookup {
        table: String,
        alias: Option<String>,
        keys: Vec<Value>,
    },
    Limit {
        source: Box<Node>,
        limit: u64,
    },
    NestedLoopJoin {
        left: Box<Node>,
        left_size: usize,
        right: Box<Node>,
        predicate: Option<Expression>,
        outer: bool,
    },
    Nothing,
    Offset {
        source: Box<Node>,
        offset: u64,
    },
    Order {
        source: Box<Node>,
        orders: Vec<(Expression, Direction)>,
    },
    Projection {
        source: Box<Node>,
        expressions: Vec<(Expression, Option<String>)>,
    },
    Scan {
        table: String,
        alias: Option<String>,
        filter: Option<Expression>,
    },
}

impl Node {
    /// Recursively transforms query nodes by applying the given closures before
    /// and after descending.
    pub fn transform<B, A>(mut self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Self) -> Result<Self>,
        A: Fn(Self) -> Result<Self>,
    {
        self = before(self)?;
        self = match self {
            node @ Self::IndexLookup { .. }
            | node @ Self::KeyLookup { .. }
            | node @ Self::Nothing
            | node @ Self::Scan { .. } => node,

            Self::Aggregation { source, aggregates } => {
                Self::Aggregation { source: source.transform(before, after)?.into(), aggregates }
            }
            Self::Filter { source, predicate } => {
                Self::Filter { source: source.transform(before, after)?.into(), predicate }
            }
            Self::HashJoin { left, left_field, right, right_field, outer } => Self::HashJoin {
                left: left.transform(before, after)?.into(),
                left_field,
                right: right.transform(before, after)?.into(),
                right_field,
                outer,
            },
            Self::Limit { source, limit } => {
                Self::Limit { source: source.transform(before, after)?.into(), limit }
            }
            Self::NestedLoopJoin { left, left_size, right, predicate, outer } => {
                Self::NestedLoopJoin {
                    left: left.transform(before, after)?.into(),
                    left_size,
                    right: right.transform(before, after)?.into(),
                    predicate,
                    outer,
                }
            }
            Self::Offset { source, offset } => {
                Self::Offset { source: source.transform(before, after)?.into(), offset }
            }
            Self::Order { source, orders } => {
                Self::Order { source: source.transform(before, after)?.into(), orders }
            }
            Self::Projection { source, expressions } => {
                Self::Projection { source: source.transform(before, after)?.into(), expressions }
            }
        };
        after(self)
    }

    /// Recursively transforms all expressions in a node by calling the given closures
    /// on them before and after descending.
    pub fn transform_expressions<B, A>(self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Expression) -> Result<Expression>,
        A: Fn(Expression) -> Result<Expression>,
    {
        Ok(match self {
            node @ Self::Aggregation { .. }
            | node @ Self::HashJoin { .. }
            | node @ Self::IndexLookup { .. }
            | node @ Self::KeyLookup { .. }
            | node @ Self::Limit { .. }
            | node @ Self::NestedLoopJoin { predicate: None, .. }
            | node @ Self::Nothing
            | node @ Self::Offset { .. }
            | node @ Self::Scan { filter: None, .. } => node,

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
            Self::NestedLoopJoin { left, left_size, right, predicate: Some(predicate), outer } => {
                Self::NestedLoopJoin {
                    left,
                    left_size,
                    right,
                    predicate: Some(predicate.transform(before, after)?),
                    outer,
                }
            }
            Self::Projection { source, expressions } => Self::Projection {
                source,
                expressions: expressions
                    .into_iter()
                    .map(|(e, l)| Ok((e.transform(before, after)?, l)))
                    .collect::<Result<_>>()?,
            },
            Self::Scan { table, alias, filter: Some(filter) } => {
                Self::Scan { table, alias, filter: Some(filter.transform(before, after)?) }
            }
        })
    }

    // Displays the node, where prefix gives the node prefix.
    pub fn format(&self, mut indent: String, root: bool, last: bool) -> String {
        // TODO: indent should be &str or int.
        let mut s = indent.clone();
        if !last {
            s += "├─ ";
            indent += "│  "
        } else if !root {
            s += "└─ ";
            indent += "   ";
        }
        match self {
            Self::Aggregation { source, aggregates } => {
                s += &format!(
                    "Aggregation: {}\n",
                    aggregates.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ")
                );
                s += &source.format(indent, false, true);
            }
            Self::Filter { source, predicate } => {
                s += &format!("Filter: {}\n", predicate);
                s += &source.format(indent, false, true);
            }
            Self::HashJoin { left, left_field, right, right_field, outer } => {
                s += &format!(
                    "HashJoin: {} on {} = {}\n",
                    if *outer { "outer" } else { "inner" },
                    match left_field {
                        (_, Some((Some(t), n))) => format!("{}.{}", t, n),
                        (_, Some((None, n))) => n.clone(),
                        (i, None) => format!("left #{}", i),
                    },
                    match right_field {
                        (_, Some((Some(t), n))) => format!("{}.{}", t, n),
                        (_, Some((None, n))) => n.clone(),
                        (i, None) => format!("right #{}", i),
                    },
                );
                s += &left.format(indent.clone(), false, false);
                s += &right.format(indent, false, true);
            }
            Self::IndexLookup { table, column, alias, values } => {
                s += &format!("IndexLookup: {}", table);
                if let Some(alias) = alias {
                    s += &format!(" as {}", alias);
                }
                s += &format!(" column {}", column);
                if !values.is_empty() && values.len() < 10 {
                    s += &format!(
                        " ({})",
                        values.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(", ")
                    );
                } else {
                    s += &format!(" ({} values)", values.len());
                }
                s += "\n";
            }
            Self::KeyLookup { table, alias, keys } => {
                s += &format!("KeyLookup: {}", table);
                if let Some(alias) = alias {
                    s += &format!(" as {}", alias);
                }
                if !keys.is_empty() && keys.len() < 10 {
                    s += &format!(
                        " ({})",
                        keys.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(", ")
                    );
                } else {
                    s += &format!(" ({} keys)", keys.len());
                }
                s += "\n";
            }
            Self::Limit { source, limit } => {
                s += &format!("Limit: {}\n", limit);
                s += &source.format(indent, false, true);
            }
            Self::NestedLoopJoin { left, left_size: _, right, predicate, outer } => {
                s += &format!("NestedLoopJoin: {}", if *outer { "outer" } else { "inner" });
                if let Some(expr) = predicate {
                    s += &format!(" on {}", expr);
                }
                s += "\n";
                s += &left.format(indent.clone(), false, false);
                s += &right.format(indent, false, true);
            }
            Self::Nothing {} => {
                s += "Nothing\n";
            }
            Self::Offset { source, offset } => {
                s += &format!("Offset: {}\n", offset);
                s += &source.format(indent, false, true);
            }
            Self::Order { source, orders } => {
                s += &format!(
                    "Order: {}\n",
                    orders
                        .iter()
                        .map(|(expr, dir)| format!("{} {}", expr, dir))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                s += &source.format(indent, false, true);
            }
            Self::Projection { source, expressions } => {
                s += &format!(
                    "Projection: {}\n",
                    expressions
                        .iter()
                        .map(|(expr, _)| expr.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                s += &source.format(indent, false, true);
            }
            Self::Scan { table, alias, filter } => {
                s += &format!("Scan: {}", table);
                if let Some(alias) = alias {
                    s += &format!(" as {}", alias);
                }
                if let Some(expr) = filter {
                    s += &format!(" ({})", expr);
                }
                s += "\n";
            }
        };
        if root {
            s = s.trim_end().to_string()
        }
        s
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format("".to_string(), true, true))
    }
}

/// An aggregate operation
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
            Self::Average => write!(f, "average"),
            Self::Count => write!(f, "count"),
            Self::Max => write!(f, "maximum"),
            Self::Min => write!(f, "minimum"),
            Self::Sum => write!(f, "sum"),
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
