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
use super::plan::{Node, Plan};
use super::types::{Columns, Row, Rows, Value};
use crate::errdata;
use crate::error::Result;

use derivative::Derivative;
use serde::{Deserialize, Serialize};

/// A plan execution result.
pub enum ExecutionResult {
    CreateTable { name: String },
    DropTable { name: String, existed: bool },
    Delete { count: u64 },
    Insert { count: u64 },
    Update { count: u64 },
    Select { iter: QueryIterator },
}

/// A query result iterator, containins the columns and row iterator.
pub struct QueryIterator {
    // TODO: use a different type here.
    pub columns: Columns,
    // TODO: remove Send, it's only needed for the ResultSet conversion.
    pub rows: Box<dyn Iterator<Item = Result<Row>> + Send>,
}

impl Iterator for QueryIterator {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next()
    }
}

/// Executes a plan, returning an execution result.
pub fn execute_plan(plan: Plan, txn: &mut impl Transaction) -> Result<ExecutionResult> {
    Ok(match plan {
        Plan::Select(node) => ExecutionResult::Select { iter: execute(node, txn)? },

        Plan::CreateTable { schema } => {
            let name = schema.name.clone();
            CreateTable::new(schema).execute(txn)?;
            ExecutionResult::CreateTable { name }
        }

        Plan::DropTable { table, if_exists } => {
            let name = table.clone();
            let existed = DropTable::new(table, if_exists).execute(txn)?;
            ExecutionResult::DropTable { name, existed }
        }

        Plan::Delete { table, source } => {
            let source = execute(source, txn)?;
            let count = Delete::new(table, source).execute(txn)?;
            ExecutionResult::Delete { count }
        }

        Plan::Insert { table, columns, expressions } => {
            let count = Insert::new(table, columns, expressions).execute(txn)?;
            ExecutionResult::Insert { count }
        }

        Plan::Update { table, source, expressions } => {
            let source = execute(source, txn)?;
            let expressions = expressions.into_iter().map(|(i, _, expr)| (i, expr)).collect();
            let count = Update::new(table, source, expressions).execute(txn)?;
            ExecutionResult::Update { count }
        }
    })
}

/// Recursively executes a query plan node, returning a row iterator.
///
/// TODO: flatten the executor structs into functions where appropriate. Same
/// goes for all other execute functions.
///
/// TODO: since iterators are lazy, make this infallible and move all catalog
/// lookups to planning.
pub fn execute(node: Node, txn: &mut impl Transaction) -> Result<QueryIterator> {
    match node {
        Node::Aggregation { source, aggregates } => {
            let source = execute(*source, txn)?;
            Aggregation::new(source, aggregates).execute()
        }

        Node::Filter { source, predicate } => {
            let source = execute(*source, txn)?;
            Ok(Filter::new(source, predicate).execute())
        }

        Node::HashJoin { left, left_field, right, right_field, outer } => {
            let left = execute(*left, txn)?;
            let right = execute(*right, txn)?;
            HashJoin::new(left, left_field.0, right, right_field.0, outer).execute()
        }

        Node::IndexLookup { table, alias: _, column, values } => {
            IndexLookup::new(table, column, values).execute(txn)
        }

        Node::KeyLookup { table, alias: _, keys } => KeyLookup::new(table, keys).execute(txn),

        Node::Limit { source, limit } => {
            let source = execute(*source, txn)?;
            Ok(Limit::new(source, limit).execute())
        }

        Node::NestedLoopJoin { left, left_size: _, right, predicate, outer } => {
            let left = execute(*left, txn)?;
            let right = execute(*right, txn)?;
            NestedLoopJoin::new(left, right, predicate, outer).execute()
        }

        Node::Nothing => Ok(Nothing.execute()),

        Node::Offset { source, offset } => {
            let source = execute(*source, txn)?;
            Ok(Offset::new(source, offset).execute())
        }

        Node::Order { source, orders } => {
            let source = execute(*source, txn)?;
            Order::new(source, orders).execute()
        }

        Node::Projection { source, expressions } => {
            let source = execute(*source, txn)?;
            Ok(Projection::new(source, expressions).execute())
        }

        Node::Scan { table, alias: _, filter } => Scan::new(table, filter).execute(txn),
    }
}

/// An executor result set
///
/// TODO: rename to StatementResult and move to Session.
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
    Explain(Plan),
}

impl ResultSet {
    /// Creates an empty row iterator, for use by serde(default).
    fn empty_rows() -> Rows {
        Box::new(std::iter::empty())
    }

    /// Converts the ResultSet into a row, or errors if not a query result with rows.
    pub fn into_row(self) -> Result<Row> {
        self.into_rows()?.next().transpose()?.ok_or(errdata!("no rows returned"))
    }

    /// Converts the ResultSet into a row iterator, or errors if not a query
    /// result with rows.
    pub fn into_rows(self) -> Result<Rows> {
        if let ResultSet::Query { rows, .. } = self {
            Ok(rows)
        } else {
            errdata!("not a query result: {self:?}")
        }
    }

    /// Converts the ResultSet into a value, if possible.
    /// TODO: use TryFrom for this, also to primitive types via Value as TryFrom.
    pub fn into_value(self) -> Result<Value> {
        self.into_row()?.into_iter().next().ok_or(errdata!("no value returned"))
    }
}

// TODO: remove or revisit this.
impl From<ExecutionResult> for ResultSet {
    fn from(result: ExecutionResult) -> Self {
        match result {
            ExecutionResult::CreateTable { name } => ResultSet::CreateTable { name },
            ExecutionResult::DropTable { name, existed } => ResultSet::DropTable { name, existed },
            ExecutionResult::Delete { count } => ResultSet::Delete { count },
            ExecutionResult::Insert { count } => ResultSet::Create { count },
            ExecutionResult::Select { iter } => {
                ResultSet::Query { columns: iter.columns, rows: iter.rows }
            }
            ExecutionResult::Update { count } => ResultSet::Update { count },
        }
    }
}

impl From<QueryIterator> for ResultSet {
    fn from(iter: QueryIterator) -> Self {
        Self::Query { columns: iter.columns, rows: iter.rows }
    }
}

impl From<QueryIterator> for Result<ResultSet> {
    fn from(value: QueryIterator) -> Self {
        Ok(value.into())
    }
}
