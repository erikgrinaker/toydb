use super::aggregate;
use super::join;
use super::source;
use super::transform;
use super::write;
use crate::error::Result;
use crate::sql::engine::{Catalog, Transaction};
use crate::sql::planner::{Node, Plan};
use crate::sql::types::{Label, Row, Rows};

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

        Plan::Select(node) => ExecutionResult::Select { iter: execute(node, txn)? },

        Plan::Update { table, primary_key, source, expressions } => {
            let source = execute(source, txn)?;
            let expressions = expressions.into_iter().map(|(i, _, expr)| (i, expr)).collect();
            let count = write::update(txn, table, primary_key, source, expressions)?;
            ExecutionResult::Update { count }
        }
    })
}

/// Recursively executes a query plan node, returning a query iterator.
///
/// TODO: since iterators are lazy, make this infallible if possible.
pub fn execute(node: Node, txn: &impl Transaction) -> Result<QueryIterator> {
    match node {
        Node::Aggregation { source, aggregates } => {
            let source = execute(*source, txn)?;
            aggregate::aggregate(source, aggregates)
        }

        Node::Filter { source, predicate } => {
            let source = execute(*source, txn)?;
            Ok(transform::filter(source, predicate))
        }

        Node::HashJoin { left, left_field, right, right_field, outer } => {
            let left = execute(*left, txn)?;
            let right = execute(*right, txn)?;
            join::hash(left, left_field.0, right, right_field.0, outer)
        }

        Node::IndexLookup { table, alias: _, column, values } => {
            source::lookup_index(txn, table, column, values)
        }

        Node::KeyLookup { table, alias: _, keys } => source::lookup_key(txn, table, keys),

        Node::Limit { source, limit } => {
            let source = execute(*source, txn)?;
            Ok(transform::limit(source, limit))
        }

        Node::NestedLoopJoin { left, left_size: _, right, predicate, outer } => {
            let left = execute(*left, txn)?;
            let right = execute(*right, txn)?;
            join::nested_loop(left, right, predicate, outer)
        }

        Node::Nothing => Ok(source::nothing()),

        Node::Offset { source, offset } => {
            let source = execute(*source, txn)?;
            Ok(transform::offset(source, offset))
        }

        Node::Order { source, orders } => {
            let source = execute(*source, txn)?;
            Ok(transform::order(source, orders))
        }

        Node::Projection { source, labels, expressions } => {
            let source = execute(*source, txn)?;
            Ok(transform::project(source, labels, expressions))
        }

        Node::Scan { table, alias: _, filter } => source::scan(txn, table, filter),

        Node::Values { labels, rows } => Ok(source::values(labels, rows)),
    }
}

/// A plan execution result.
pub enum ExecutionResult {
    CreateTable { name: String },
    DropTable { name: String, existed: bool },
    Delete { count: u64 },
    Insert { count: u64 },
    Update { count: u64 },
    Select { iter: QueryIterator },
}

/// A query result iterator, containing the columns and row iterator.
///
/// TODO: if we resolve labels during planning, we can replace this with a
/// simple Rows iterator. We can probably also make that generic instead of a
/// trait object, since we know the type when calling each executor.
pub struct QueryIterator {
    /// Column names.
    pub columns: Vec<Option<Label>>,
    /// Row iterator.
    pub rows: Rows,
}

impl Iterator for QueryIterator {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next()
    }
}

impl QueryIterator {
    /// Replaces the columns with the result of the closure.
    pub fn map_columns<F>(mut self, f: F) -> Self
    where
        F: FnOnce(Vec<Option<Label>>) -> Vec<Option<Label>>,
    {
        self.columns = f(self.columns);
        self
    }

    /// Replaces the rows iterator with the result of the closure.
    pub fn map_rows<F, I>(mut self, f: F) -> Self
    where
        I: Iterator<Item = Result<Row>> + 'static,
        F: FnOnce(Rows) -> I,
    {
        self.rows = Box::new(f(self.rows));
        self
    }

    /// Like map_rows, but if the closure errors the row iterator will yield a
    /// single error item.
    pub fn try_map_rows<F, I>(mut self, f: F) -> Self
    where
        I: Iterator<Item = Result<Row>> + 'static,
        F: FnOnce(Rows) -> Result<I>,
    {
        self.rows = match f(self.rows) {
            Ok(rows) => Box::new(rows),
            Err(e) => Box::new(std::iter::once(Err(e))),
        };
        self
    }
}
