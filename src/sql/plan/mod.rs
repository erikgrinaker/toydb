mod optimizer;
mod planner;
use optimizer::Optimizer as _;
use planner::Planner;

use super::engine::Transaction;
use super::execution::{Executor, ResultSet};
use super::parser::ast;
use super::schema::{Catalog, Table};
use super::types::{Expression, Value};
use crate::error::Result;

use serde_derive::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A query plan
#[derive(Debug)]
pub struct Plan(pub Node);

impl Plan {
    /// Builds a plan from an AST statement.
    pub fn build<C: Catalog>(statement: ast::Statement, catalog: &mut C) -> Result<Self> {
        Planner::new(catalog).build(statement)
    }

    /// Executes the plan, consuming it.
    pub fn execute<T: Transaction + 'static>(self, txn: &mut T) -> Result<ResultSet> {
        Executor::build(self.0).execute(txn)
    }

    /// Optimizes the plan, consuming it.
    pub fn optimize<C: Catalog>(self, catalog: &mut C) -> Result<Self> {
        let mut root = self.0;
        root = optimizer::ConstantFolder.optimize(root)?;
        root = optimizer::FilterPushdown.optimize(root)?;
        root = optimizer::IndexLookup::new(catalog).optimize(root)?;
        Ok(Plan(root))
    }
}

/// A plan node
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    Aggregation {
        source: Box<Node>,
        aggregates: Vec<Aggregate>,
    },
    CreateTable {
        schema: Table,
    },
    Delete {
        table: String,
        source: Box<Node>,
    },
    DropTable {
        table: String,
    },
    Filter {
        source: Box<Node>,
        // A filter node may not make sense, but it's useful during planning and optimization
        // when we're moving predicates around - in particular, replacing a filter node with
        // a source node during filter pushdown will cause Node.transform() to skip the source.
        predicate: Option<Expression>,
    },
    IndexLookup {
        table: String,
        alias: Option<String>,
        column: String,
        values: Vec<Value>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        expressions: Vec<Vec<Expression>>,
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
        outer: Box<Node>,
        outer_size: usize,
        inner: Box<Node>,
        predicate: Option<Expression>,
        pad: bool,
        flip: bool,
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
    // Uses BTreeMap for test stability
    Update {
        table: String,
        source: Box<Node>,
        expressions: BTreeMap<String, Expression>,
    },
}

impl Node {
    /// Recursively transforms nodes by applying functions before and after descending.
    pub fn transform<B, A>(mut self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Self) -> Result<Self>,
        A: Fn(Self) -> Result<Self>,
    {
        self = before(self)?;
        self = match self {
            n @ Self::CreateTable { .. }
            | n @ Self::DropTable { .. }
            | n @ Self::IndexLookup { .. }
            | n @ Self::Insert { .. }
            | n @ Self::KeyLookup { .. }
            | n @ Self::Nothing
            | n @ Self::Scan { .. } => n,

            Self::Aggregation { source, aggregates } => {
                Self::Aggregation { source: source.transform(before, after)?.into(), aggregates }
            }
            Self::Delete { table, source } => {
                Self::Delete { table, source: source.transform(before, after)?.into() }
            }
            Self::Filter { source, predicate } => {
                Self::Filter { source: source.transform(before, after)?.into(), predicate }
            }
            Self::Limit { source, limit } => {
                Self::Limit { source: source.transform(before, after)?.into(), limit }
            }
            Self::NestedLoopJoin { outer, outer_size, inner, predicate, pad, flip } => {
                Self::NestedLoopJoin {
                    outer: outer.transform(before, after)?.into(),
                    outer_size,
                    inner: inner.transform(before, after)?.into(),
                    predicate,
                    pad,
                    flip,
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
            Self::Update { table, source, expressions } => {
                Self::Update { table, source: source.transform(before, after)?.into(), expressions }
            }
        };
        after(self)
    }

    /// Transforms all expressions in a node by calling .transform() on them with the given closure.
    pub fn transform_expressions<B, A>(self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Expression) -> Result<Expression>,
        A: Fn(Expression) -> Result<Expression>,
    {
        Ok(match self {
            n @ Self::Aggregation { .. }
            | n @ Self::CreateTable { .. }
            | n @ Self::Delete { .. }
            | n @ Self::DropTable { .. }
            | n @ Self::Filter { predicate: None, .. }
            | n @ Self::IndexLookup { .. }
            | n @ Self::KeyLookup { .. }
            | n @ Self::Limit { .. }
            | n @ Self::NestedLoopJoin { .. }
            | n @ Self::Nothing
            | n @ Self::Offset { .. }
            | n @ Self::Scan { filter: None, .. } => n,

            Self::Filter { source, predicate: Some(predicate) } => {
                Self::Filter { source, predicate: Some(predicate.transform(before, after)?) }
            }
            Self::Insert { table, columns, expressions } => Self::Insert {
                table,
                columns,
                expressions: expressions
                    .into_iter()
                    .map(|exprs| exprs.into_iter().map(|e| e.transform(before, after)).collect())
                    .collect::<Result<_>>()?,
            },
            Self::Order { source, orders } => Self::Order {
                source,
                orders: orders
                    .into_iter()
                    .map(|(e, o)| e.transform(before, after).map(|e| (e, o)))
                    .collect::<Result<_>>()?,
            },
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
            Self::Update { table, source, expressions } => Self::Update {
                table,
                source,
                expressions: expressions
                    .into_iter()
                    .map(|(k, e)| e.transform(before, after).map(|e| (k, e)))
                    .collect::<Result<_>>()?,
            },
        })
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

pub type Aggregates = Vec<Aggregate>;

/// A sort order direction
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}
