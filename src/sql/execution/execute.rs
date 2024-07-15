use super::aggregate;
use super::join;
use super::source;
use super::transform;
use super::write;
use crate::error::Result;
use crate::sql::engine::{Catalog, Transaction};
use crate::sql::planner::{Node, Plan};
use crate::sql::types::{Label, Rows};

/// Executes a plan, returning an execution result.
///
/// Takes the transaction and catalog separately, even though Transaction must
/// implement Catalog, to ensure the catalog is primarily used during planning.
pub fn execute_plan(
    plan: Plan,
    txn: &impl Transaction,
    catalog: &impl Catalog,
) -> Result<ExecutionResult> {
    Ok(match plan {
        Plan::CreateTable { schema } => {
            let name = schema.name.clone();
            catalog.create_table(schema)?;
            ExecutionResult::CreateTable { name }
        }

        Plan::DropTable { table, if_exists } => {
            let existed = catalog.drop_table(&table, if_exists)?;
            ExecutionResult::DropTable { name: table, existed }
        }

        Plan::Delete { table, primary_key, source } => {
            let source = execute(source, txn)?;
            let count = write::delete(txn, table, primary_key, source)?;
            ExecutionResult::Delete { count }
        }

        Plan::Insert { table, column_map, source } => {
            let source = execute(source, txn)?;
            let count = write::insert(txn, table, column_map, source)?;
            ExecutionResult::Insert { count }
        }

        Plan::Select { root, labels } => {
            let iter = execute(root, txn)?;
            ExecutionResult::Select { rows: iter, labels }
        }

        Plan::Update { table, primary_key, source, expressions } => {
            let source = execute(source, txn)?;
            let expressions = expressions.into_iter().map(|(i, _, expr)| (i, expr)).collect();
            let count = write::update(txn, table, primary_key, source, expressions)?;
            ExecutionResult::Update { count }
        }
    })
}

/// Recursively executes a query plan node, returning a row iterator.
///
/// TODO: since iterators are lazy, make this infallible if possible.
pub fn execute(node: Node, txn: &impl Transaction) -> Result<Rows> {
    match node {
        Node::Aggregate { source, group_by, aggregates } => {
            let source = execute(*source, txn)?;
            aggregate::aggregate(source, group_by, aggregates)
        }

        Node::Filter { source, predicate } => {
            let source = execute(*source, txn)?;
            Ok(transform::filter(source, predicate))
        }

        Node::HashJoin {
            left,
            left_field,
            left_label: _,
            right,
            right_field,
            right_label: _,
            outer,
        } => {
            let right_size = right.size();
            let left = execute(*left, txn)?;
            let right = execute(*right, txn)?;
            join::hash(left, left_field, right, right_field, right_size, outer)
        }

        Node::IndexLookup { table, column, values, alias: _ } => {
            let column = table.columns.into_iter().nth(column).expect("invalid column").name;
            let table = table.name;
            source::lookup_index(txn, table, column, values)
        }

        Node::KeyLookup { table, keys, alias: _ } => source::lookup_key(txn, table.name, keys),

        Node::Limit { source, limit } => {
            let source = execute(*source, txn)?;
            Ok(transform::limit(source, limit))
        }

        Node::NestedLoopJoin { left, right, predicate, outer } => {
            let right_size = right.size();
            let left = execute(*left, txn)?;
            let right = execute(*right, txn)?;
            join::nested_loop(left, right, right_size, predicate, outer)
        }

        Node::Nothing => Ok(source::nothing()),

        Node::Offset { source, offset } => {
            let source = execute(*source, txn)?;
            Ok(transform::offset(source, offset))
        }

        Node::Order { source, orders } => {
            let source = execute(*source, txn)?;
            transform::order(source, orders)
        }

        Node::Projection { source, expressions, labels: _ } => {
            let source = execute(*source, txn)?;
            Ok(transform::project(source, expressions))
        }

        Node::Remap { source, targets } => {
            let source = execute(*source, txn)?;
            Ok(transform::remap(source, targets))
        }

        Node::Scan { table, filter, alias: _ } => source::scan(txn, table, filter),

        Node::Values { rows } => Ok(source::values(rows)),
    }
}

/// A plan execution result.
pub enum ExecutionResult {
    CreateTable { name: String },
    DropTable { name: String, existed: bool },
    Delete { count: u64 },
    Insert { count: u64 },
    Update { count: u64 },
    Select { rows: Rows, labels: Vec<Label> },
}
