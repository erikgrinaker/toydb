use super::aggregation::Aggregation;
use super::join::{HashJoin, NestedLoopJoin};
use super::mutation::{Delete, Insert, Update};
use super::query::{Filter, Limit, Offset, Order, Projection};
use super::schema::{CreateTable, DropTable};
use super::source::{IndexLookup, KeyLookup, Nothing, Scan};
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::plan::{Node, Plan};
use crate::sql::types::{Columns, Row};

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
