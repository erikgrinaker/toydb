use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use itertools::{Itertools as _, izip};

use super::aggregator::Aggregator;
use super::join::{HashJoiner, NestedLoopJoiner};
use crate::errinput;
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::planner::{Direction, Node, Plan};
use crate::sql::types::{Expression, Label, Row, Rows, Table, Value};

/// Executes statement plans.
///
/// The plan root specifies the action to take (e.g. SELECT, INSERT, UPDATE,
/// etc). It has a nested tree of child nodes that process rows.
///
/// Nodes are executed recursively, and return row iterators. Parent nodes
/// recursively pull input rows from their child nodes, process them, and pass
/// them on to their parent node.
///
/// Below is an example of an (unoptimized) query plan:
///
/// SELECT title, released, genres.name AS genre
/// FROM movies INNER JOIN genres ON movies.genre_id = genres.id
/// WHERE released >= 2000
/// ORDER BY released
///
/// Select
/// └─ Order: movies.released desc
///    └─ Projection: movies.title, movies.released, genres.name as genre
///       └─ Filter: movies.released >= 2000
///          └─ NestedLoopJoin: inner on movies.genre_id = genres.id
///             ├─ Scan: movies
///             └─ Scan: genres
///
/// Rows flow from the tree leaves to the root:
///
/// 1. Scan nodes read rows from movies and genres.
/// 2. NestedLoopJoin joins the rows from movies and genres.
/// 3. Filter discards rows with release dates older than 2000.
/// 4. Projection picks out the requested column values from the rows.
/// 5. Order sorts the rows by release date.
/// 6. Select returns the final rows to the client.
pub struct Executor<'a, T: Transaction> {
    /// The transaction used to execute the plan.
    txn: &'a T,
}

impl<'a, T: Transaction> Executor<'a, T> {
    /// Creates a new executor.
    pub fn new(txn: &'a T) -> Self {
        Self { txn }
    }

    /// Executes a plan, returning an execution result.
    pub fn execute(&mut self, plan: Plan) -> Result<ExecutionResult> {
        Ok(match plan {
            // CREATE TABLE
            Plan::CreateTable { schema } => {
                let name = schema.name.clone();
                self.txn.create_table(schema)?;
                ExecutionResult::CreateTable { name }
            }

            // DROP TABLE
            Plan::DropTable { name, if_exists } => {
                let existed = self.txn.drop_table(&name, if_exists)?;
                ExecutionResult::DropTable { name, existed }
            }

            // DELETE
            Plan::Delete { table, primary_key, source } => {
                let source = self.execute_node(source)?;
                let count = self.delete(&table, primary_key, source)?;
                ExecutionResult::Delete { count }
            }

            // INSERT
            Plan::Insert { table, column_map, source } => {
                let source = self.execute_node(source)?;
                let count = self.insert(table, column_map, source)?;
                ExecutionResult::Insert { count }
            }

            // SELECT
            Plan::Select(root) => {
                let columns = (0..root.columns()).map(|i| root.column_label(i)).collect();
                let rows = self.execute_node(root)?;
                ExecutionResult::Select { columns, rows }
            }

            // UPDATE
            Plan::Update { table, primary_key, source, expressions } => {
                let source = self.execute_node(source)?;
                let count = self.update(&table.name, primary_key, source, expressions)?;
                ExecutionResult::Update { count }
            }
        })
    }

    /// Recursively executes a query plan node, returning a row iterator.
    fn execute_node(&mut self, node: Node) -> Result<Rows> {
        Ok(match node {
            // GROUP BY and aggregate functions.
            Node::Aggregate { source, group_by, aggregates } => {
                let source = self.execute_node(*source)?;
                let mut aggregator = Aggregator::new(group_by, aggregates);
                aggregator.add_rows(source)?;
                aggregator.into_rows()
            }

            // WHERE and similar filtering.
            Node::Filter { source, predicate } => {
                let source = self.execute_node(*source)?;
                Box::new(source.filter_map(move |result| {
                    result
                        .and_then(|row| match predicate.evaluate(Some(&row))? {
                            Value::Boolean(true) => Ok(Some(row)),
                            Value::Boolean(false) | Value::Null => Ok(None),
                            value => errinput!("filter returned {value}, expected boolean",),
                        })
                        .transpose()
                }))
            }

            // JOIN using a hash join.
            Node::HashJoin { left, left_column, right, right_column, outer } => {
                let right_columns = right.columns();
                let left = self.execute_node(*left)?;
                let right = self.execute_node(*right)?;
                Box::new(HashJoiner::new(
                    left,
                    left_column,
                    right,
                    right_column,
                    right_columns,
                    outer,
                )?)
            }

            // Looks up primary keys by secondary index values.
            Node::IndexLookup { table, column, values, alias: _ } => {
                let column = table.columns.into_iter().nth(column).expect("invalid column").name;
                let ids =
                    self.txn.lookup_index(&table.name, &column, &values)?.into_iter().collect_vec();
                Box::new(self.txn.get(&table.name, &ids)?.into_iter().map(Ok))
            }

            // Looks up rows by primary key.
            Node::KeyLookup { table, keys, alias: _ } => {
                Box::new(self.txn.get(&table.name, &keys)?.into_iter().map(Ok))
            }

            // LIMIT
            Node::Limit { source, limit } => Box::new(self.execute_node(*source)?.take(limit)),

            // JOIN using a nested loop join.
            Node::NestedLoopJoin { left, right, predicate, outer } => {
                let right_columns = right.columns();
                let left = self.execute_node(*left)?;
                let right = self.execute_node(*right)?;
                Box::new(NestedLoopJoiner::new(left, right, right_columns, predicate, outer))
            }

            // An empty row iterator.
            Node::Nothing { .. } => Box::new(std::iter::empty()),

            // OFFSET
            Node::Offset { source, offset } => Box::new(self.execute_node(*source)?.skip(offset)),

            // ORDER BY
            Node::Order { source, key } => {
                let source = self.execute_node(*source)?;
                Box::new(Self::order(source, key)?)
            }

            // Projects columns from the source, and evaluates expressions.
            Node::Projection { source, expressions, aliases: _ } => {
                let source = self.execute_node(*source)?;
                Box::new(source.map(move |result| {
                    let row = result?;
                    expressions.iter().map(|expr| expr.evaluate(Some(&row))).collect()
                }))
            }

            // Remaps source column indexes to new target column indexes.
            Node::Remap { source, targets } => {
                let source = self.execute_node(*source)?;
                let size = targets.iter().copied().flatten().map(|i| i + 1).max().unwrap_or(0);
                Box::new(source.map_ok(move |row| {
                    let mut remapped = vec![Value::Null; size];
                    for (target, value) in targets.iter().copied().zip_eq(row) {
                        if let Some(target) = target {
                            remapped[target] = value;
                        }
                    }
                    remapped
                }))
            }

            // Scans a table, optionally filtering rows.
            Node::Scan { table, filter, alias: _ } => Box::new(self.txn.scan(&table.name, filter)?),

            // Emits constant values.
            Node::Values { rows } => Box::new(
                rows.into_iter()
                    .map(|row| row.into_iter().map(|expr| expr.evaluate(None)).collect()),
            ),
        })
    }

    /// DELETE: deletes rows, taking primary keys from the source at the given
    /// primary_key column index. Returns the number of rows deleted.
    fn delete(&self, table: &str, primary_key: usize, source: Rows) -> Result<u64> {
        let ids: Vec<Value> = source
            .map_ok(|row| row.into_iter().nth(primary_key).expect("short row"))
            .try_collect()?;
        let count = ids.len() as u64;
        self.txn.delete(table, &ids)?;
        Ok(count)
    }

    /// INSERT: inserts rows into a table from the given source.
    ///
    /// If given, column_map contains the mapping of table → source columns for
    /// all columns in source. Otherwise, every column in source corresponds to
    /// those in table, but a tail of source columns may be missing.
    fn insert(
        &self,
        table: Table,
        column_map: Option<HashMap<usize, usize>>,
        mut source: Rows,
    ) -> Result<u64> {
        let mut rows = Vec::new();
        while let Some(values) = source.next().transpose()? {
            // Fast path: the row is already complete, with no column mapping.
            if values.len() == table.columns.len() && column_map.is_none() {
                rows.push(values);
                continue;
            }
            if values.len() > table.columns.len() {
                return errinput!("too many values for table {}", table.name);
            }
            if let Some(column_map) = &column_map {
                if column_map.len() != values.len() {
                    return errinput!("column and value counts do not match");
                }
            }

            // Map source columns to table columns, and fill in default values.
            let mut row = Vec::with_capacity(table.columns.len());
            for (i, column) in table.columns.iter().enumerate() {
                if column_map.is_none() && i < values.len() {
                    // Pass through the source column to the table column.
                    row.push(values[i].clone())
                } else if let Some(vi) = column_map.as_ref().and_then(|c| c.get(&i)).copied() {
                    // Map the source column to the table column.
                    row.push(values[vi].clone())
                } else if let Some(default) = &column.default {
                    // Column not given in source, use the default.
                    row.push(default.clone())
                } else {
                    return errinput!("no value given for column {} with no default", column.name);
                }
            }
            rows.push(row);
        }
        let count = rows.len() as u64;
        self.txn.insert(&table.name, rows)?;
        Ok(count)
    }

    /// UPDATE: updates rows passed in from the source. Returns the number of
    /// rows updated.
    fn update(
        &self,
        table: &str,
        primary_key: usize,
        mut source: Rows,
        expressions: Vec<(usize, Expression)>,
    ) -> Result<u64> {
        let mut updates = BTreeMap::new();
        while let Some(row) = source.next().transpose()? {
            let mut update = row.clone();
            for (column, expr) in &expressions {
                update[*column] = expr.evaluate(Some(&row))?;
            }
            let id = row.into_iter().nth(primary_key).expect("short row");
            updates.insert(id, update);
        }
        let count = updates.len() as u64;
        self.txn.update(table, updates)?;
        Ok(count)
    }

    /// Sorts the input rows.
    fn order(source: Rows, order: Vec<(Expression, Direction)>) -> Result<Rows> {
        // We can't use sorted_by_cached_key(), since expression evaluation is
        // fallible, and since we may have to vary the sort direction of each
        // expression. Collect the rows and pre-computed sort keys into a vec.
        let mut rows: Vec<(Row, Vec<Value>)> = source
            .map(|result| {
                result.and_then(|row| {
                    let sort_keys =
                        order.iter().map(|(expr, _)| expr.evaluate(Some(&row))).try_collect()?;
                    Ok((row, sort_keys))
                })
            })
            .try_collect()?;

        rows.sort_by(|(_, a_keys), (_, b_keys)| {
            let dirs = order.iter().map(|(_, dir)| dir).copied();
            for (a_key, b_key, dir) in izip!(a_keys, b_keys, dirs) {
                let mut ordering = a_key.cmp(b_key);
                if dir == Direction::Descending {
                    ordering = ordering.reverse();
                }
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        });

        Ok(Box::new(rows.into_iter().map(|(row, _)| Ok(row))))
    }
}

/// A plan execution result.
pub enum ExecutionResult {
    CreateTable { name: String },
    DropTable { name: String, existed: bool },
    Delete { count: u64 },
    Insert { count: u64 },
    Update { count: u64 },
    Select { columns: Vec<Label>, rows: Rows },
}
