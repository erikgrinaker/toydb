mod aggregation;
mod create_table;
mod delete;
mod drop_table;
mod filter;
mod index_lookup;
mod insert;
mod key_lookup;
mod limit;
mod nested_loop_join;
mod nothing;
mod offset;
mod order;
mod projection;
mod scan;
mod update;

use aggregation::Aggregation;
use create_table::CreateTable;
use delete::Delete;
use drop_table::DropTable;
use filter::Filter;
use index_lookup::IndexLookup;
use insert::Insert;
use key_lookup::KeyLookup;
use limit::Limit;
use nested_loop_join::NestedLoopJoin;
use nothing::Nothing;
use offset::Offset;
use order::Order;
use projection::Projection;
use scan::Scan;
use update::Update;

use super::engine::{Mode, Transaction};
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
            Node::DropTable { table } => DropTable::new(table),
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
            Node::IndexLookup { table, alias, column, keys } => {
                IndexLookup::new(table, alias, column, keys)
            }
            Node::Insert { table, columns, expressions } => {
                Insert::new(table, columns, expressions)
            }
            Node::KeyLookup { table, alias, keys } => KeyLookup::new(table, alias, keys),
            Node::Limit { source, limit } => Limit::new(Self::build(*source), limit),
            Node::NestedLoopJoin { outer, inner, predicate, pad, flip } => {
                NestedLoopJoin::new(Self::build(*outer), Self::build(*inner), predicate, pad, flip)
            }
            Node::Nothing => Nothing::new(),
            Node::Offset { source, offset } => Offset::new(Self::build(*source), offset),
            Node::Order { source, orders } => Order::new(Self::build(*source), orders),
            Node::Projection { source, labels, expressions } => {
                Projection::new(Self::build(*source), labels, expressions)
            }
            Node::Scan { table, label, filter } => Scan::new(table, label, filter),
            Node::Update { table, source, expressions } => {
                Update::new(table, Self::build(*source), expressions)
            }
        }
    }
}

/// An executor result set
#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Debug, PartialEq)]
pub enum ResultSet {
    // Transaction started
    Begin {
        id: u64,
        mode: Mode,
    },
    // Transaction committed
    Commit {
        id: u64,
    },
    // Transaction rolled back
    Rollback {
        id: u64,
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
    /// Creates an empty row iteratur, for use by serde(default).
    fn empty_rows() -> Rows {
        Box::new(std::iter::empty())
    }

    /// Converts the ResultSet into a row, or errors if not a query result with rows.
    pub fn into_row(self) -> Result<Row> {
        if let ResultSet::Query { mut rows, .. } = self {
            rows.next().transpose()?.ok_or_else(|| Error::Value("No rows returned".into()))
        } else {
            Err(Error::Value(format!("Not a query result: {:?}", self)))
        }
    }

    /// Converts the ResultSet into a value, if possible.
    pub fn into_value(self) -> Result<Value> {
        self.into_row()?.into_iter().next().ok_or_else(|| Error::Value("No value returned".into()))
    }
}
