mod aggregation;
mod join;
mod mutation;
mod query;
mod schema;
mod source;

use aggregation::Aggregation;
use join::{HashJoin, NestedLoopJoin};
use mutation::{Delete, Insert, Update};
use query::{Filter, Limit, Offset, Order, Projection};
use schema::{CreateTable, DropTable};
use source::{IndexLookup, KeyLookup, Nothing, Scan};

use super::engine::Transaction;
use super::plan::Node;
use super::types::{Columns, Row, Rows, Value};
use crate::error::{Error, Result};

use derivative::Derivative;
use serde_derive::{Deserialize, Serialize};

/// A plan executor
pub trait Executor<T: Transaction> {
    /// Executes the executor, consuming it and returning a result set
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

impl<T: Transaction + 'static> dyn Executor<T> {
    /// Builds an executor for a plan node, consuming it
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::Aggregation { source, aggregates } => {
                Aggregation::new(Self::build(*source), aggregates)
            }
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Delete { table, source } => Delete::new(table, Self::build(*source)),
            Node::DropTable { table, if_exists } => DropTable::new(table, if_exists),
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
            Node::HashJoin { left, left_field, right, right_field, outer } => HashJoin::new(
                Self::build(*left),
                left_field.0,
                Self::build(*right),
                right_field.0,
                outer,
            ),
            Node::IndexLookup { table, alias: _, column, values } => {
                IndexLookup::new(table, column, values)
            }
            Node::Insert { table, columns, expressions } => {
                Insert::new(table, columns, expressions)
            }
            Node::KeyLookup { table, alias: _, keys } => KeyLookup::new(table, keys),
            Node::Limit { source, limit } => Limit::new(Self::build(*source), limit),
            Node::NestedLoopJoin { left, left_size: _, right, predicate, outer } => {
                NestedLoopJoin::new(Self::build(*left), Self::build(*right), predicate, outer)
            }
            Node::Nothing => Nothing::new(),
            Node::Offset { source, offset } => Offset::new(Self::build(*source), offset),
            Node::Order { source, orders } => Order::new(Self::build(*source), orders),
            Node::Projection { source, expressions } => {
                Projection::new(Self::build(*source), expressions)
            }
            Node::Scan { table, filter, alias: _ } => Scan::new(table, filter),
            Node::Update { table, source, expressions } => Update::new(
                table,
                Self::build(*source),
                expressions.into_iter().map(|(i, _, e)| (i, e)).collect(),
            ),
        }
    }
}

/// An executor result set
#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Debug, PartialEq)]
pub enum ResultSet {
    // Transaction started
    Begin {
        version: u64,
        read_only: bool,
    },
    // Transaction committed
    Commit {
        version: u64,
    },
    // Transaction rolled back
    Rollback {
        version: u64,
    },
    // Rows created
    Create {
        count: u64,
    },
    // Rows deleted
    Delete {
        count: u64,
    },
    // Rows updated
    Update {
        count: u64,
    },
    // Table created
    CreateTable {
        name: String,
    },
    // Table dropped
    DropTable {
        name: String,
        existed: bool,
    },
    // Query result
    Query {
        columns: Columns,
        #[derivative(Debug = "ignore")]
        #[derivative(PartialEq = "ignore")]
        #[serde(skip, default = "ResultSet::empty_rows")]
        rows: Rows,
    },
    // Explain result
    Explain(Node),
}

impl ResultSet {
    /// Creates an empty row iterator, for use by serde(default).
    fn empty_rows() -> Rows {
        Box::new(std::iter::empty())
    }

    /// Converts the ResultSet into a row, or errors if not a query result with rows.
    pub fn into_row(self) -> Result<Row> {
        self.into_rows()?.next().transpose()?.ok_or_else(|| Error::Value("No rows returned".into()))
    }

    /// Converts the ResultSet into a row iterator, or errors if not a query
    /// result with rows.
    pub fn into_rows(self) -> Result<Rows> {
        if let ResultSet::Query { rows, .. } = self {
            Ok(rows)
        } else {
            Err(Error::Value(format!("Not a query result: {:?}", self)))
        }
    }

    /// Converts the ResultSet into a value, if possible.
    pub fn into_value(self) -> Result<Value> {
        self.into_row()?.into_iter().next().ok_or_else(|| Error::Value("No value returned".into()))
    }
}
