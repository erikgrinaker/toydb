use super::{aggregate, join, source, transform, write};
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

        Plan::Select(root) => {
            let columns = (0..root.columns()).map(|i| root.column_label(i)).collect();
            let rows = execute(root, txn)?;
            ExecutionResult::Select { rows, columns }
        }

        Plan::Update { table, primary_key, source, expressions } => {
            let source = execute(source, txn)?;
            let count = write::update(txn, table.name, primary_key, source, expressions)?;
            ExecutionResult::Update { count }
        }
    })
}

/// Recursively executes a query plan node, returning a row iterator.
///
/// Rows stream through the plan node tree from the branches to the root. Nodes
/// recursively pull input rows upwards from their child node(s), process them,
/// and hand the resulting rows off to their parent node.
///
/// Below is an example of an (unoptimized) query plan:
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
pub fn execute(node: Node, txn: &impl Transaction) -> Result<Rows> {
    Ok(match node {
        Node::Aggregate { source, group_by, aggregates } => {
            let source = execute(*source, txn)?;
            aggregate::aggregate(source, group_by, aggregates)?
        }

        Node::Filter { source, predicate } => {
            let source = execute(*source, txn)?;
            transform::filter(source, predicate)
        }

        Node::HashJoin { left, left_column, right, right_column, outer } => {
            let right_size = right.columns();
            let left = execute(*left, txn)?;
            let right = execute(*right, txn)?;
            join::hash(left, left_column, right, right_column, right_size, outer)?
        }

        Node::IndexLookup { table, column, values, alias: _ } => {
            let column = table.columns.into_iter().nth(column).expect("invalid column").name;
            let table = table.name;
            source::lookup_index(txn, table, column, values)?
        }

        Node::KeyLookup { table, keys, alias: _ } => source::lookup_key(txn, table.name, keys)?,

        Node::Limit { source, limit } => {
            let source = execute(*source, txn)?;
            transform::limit(source, limit)
        }

        Node::NestedLoopJoin { left, right, predicate, outer } => {
            let right_size = right.columns();
            let left = execute(*left, txn)?;
            let right = execute(*right, txn)?;
            join::nested_loop(left, right, right_size, predicate, outer)?
        }

        Node::Nothing { .. } => source::nothing(),

        Node::Offset { source, offset } => {
            let source = execute(*source, txn)?;
            transform::offset(source, offset)
        }

        Node::Order { source, key: orders } => {
            let source = execute(*source, txn)?;
            transform::order(source, orders)?
        }

        Node::Projection { source, expressions, aliases: _ } => {
            let source = execute(*source, txn)?;
            transform::project(source, expressions)
        }

        Node::Remap { source, targets } => {
            let source = execute(*source, txn)?;
            transform::remap(source, targets)
        }

        Node::Scan { table, filter, alias: _ } => source::scan(txn, table, filter)?,

        Node::Values { rows } => source::values(rows),
    })
}

/// A plan execution result.
pub enum ExecutionResult {
    CreateTable { name: String },
    DropTable { name: String, existed: bool },
    Delete { count: u64 },
    Insert { count: u64 },
    Update { count: u64 },
    Select { rows: Rows, columns: Vec<Label> },
}
