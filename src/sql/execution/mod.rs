mod aggregation;
mod create_table;
mod delete;
mod drop_table;
mod filter;
mod insert;
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
use insert::Insert;
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
use super::types::Environment;
use super::types::{Columns, Relation, Row, Value};
use crate::Error;

/// A plan executor
pub trait Executor<T: Transaction> {
    /// Executes the executor, consuming it and returning a result set
    fn execute(self: Box<Self>, ctx: &mut Context<T>) -> Result<ResultSet, Error>;
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
            Node::DropTable { name } => DropTable::new(name),
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
            Node::Insert { table, columns, expressions } => {
                Insert::new(table, columns, expressions)
            }
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
            Node::Scan { table, alias } => Scan::new(table, alias),
            Node::Update { table, source, expressions } => {
                Update::new(table, Self::build(*source), expressions)
            }
        }
    }
}

/// An execution context
pub struct Context<'a, T: Transaction> {
    /// The transaction to execute in
    pub txn: &'a mut T,
}

/// An executor result set
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ResultSet {
    // Transaction started
    Begin { id: u64, mode: Mode },
    // Transaction committed
    Commit { id: u64 },
    // Transaction rolled back
    Rollback { id: u64 },
    // Rows created
    Create { count: u64 },
    // Rows deleted
    Delete { count: u64 },
    // Rows updated
    Update { count: u64 },
    // Table created
    CreateTable { name: String },
    // Table dropped
    DropTable { name: String },
    // Query result
    Query { relation: Relation },
}

/// Column metadata for a result.
/// FIXME This is outdated and should be removed.
#[derive(Clone, Serialize, Deserialize)]
pub struct ResultColumns {
    columns: Vec<(Option<String>, Option<String>)>,
}

impl ResultColumns {
    pub fn new(columns: Vec<(Option<String>, Option<String>)>) -> Self {
        Self { columns }
    }

    pub fn from(columns: Vec<Option<String>>) -> Self {
        Self { columns: columns.into_iter().map(|c| (None, c)).collect() }
    }

    pub fn from_new_columns(columns: Columns) -> Self {
        Self { columns: columns.into_iter().map(|c| (c.relation, c.name)).collect() }
    }

    fn as_env<'b>(&'b self, row: &'b [Value]) -> ResultEnv<'b> {
        ResultEnv { columns: &self, row }
    }

    fn format(&self, relation: Option<&str>, field: &str) -> String {
        let mut s = super::parser::format_ident(field);
        if let Some(relation) = relation {
            s = format!("{}.{}", super::parser::format_ident(relation), s)
        }
        s
    }

    fn get(&self, relation: Option<&str>, field: &str) -> Result<(Option<String>, String), Error> {
        let matches: Vec<_> = self
            .columns
            .iter()
            .filter_map(|(r, c)| {
                if c.as_deref() == Some(field) {
                    if relation.is_none() || r.as_deref() == relation {
                        Some((r.clone(), c.clone().unwrap()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        match matches.len() {
            0 => Err(Error::Value(format!("Unknown field {}", self.format(relation, field)))),
            1 => Ok(matches.into_iter().next().unwrap()),
            _ => Err(Error::Value(format!("Field reference {} is ambiguous", field))),
        }
    }

    pub fn index(&self, relation: Option<&str>, field: &str) -> Result<usize, Error> {
        let matches: Vec<_> = self
            .columns
            .iter()
            .enumerate()
            .filter_map(|(i, (r, c))| {
                if c.as_deref() == Some(field) {
                    if relation.is_none() || r.as_deref() == relation {
                        Some(i)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        match matches.len() {
            0 => Err(Error::Value(format!("Unknown field {}", self.format(relation, field)))),
            1 => Ok(matches.into_iter().next().unwrap()),
            _ => Err(Error::Value(format!("Field reference {} is ambiguous", field))),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.len() == 0
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn merge(self, other: Self) -> Self {
        let mut columns = self.columns;
        columns.extend(other.columns);
        Self::new(columns)
    }

    pub fn names(&self) -> Vec<Option<String>> {
        self.columns.iter().map(|(_, c)| c.clone()).collect()
    }
}

// Environment for a result row
// FIXME This should be removed
struct ResultEnv<'a> {
    columns: &'a ResultColumns,
    row: &'a [Value],
}

impl<'a> Environment for ResultEnv<'a> {
    fn lookup(&self, relation: Option<&str>, field: &str) -> Result<Value, Error> {
        self.lookup_index(self.columns.index(relation, field)?)
    }

    fn lookup_index(&self, index: usize) -> Result<Value, Error> {
        self.row.get(index).cloned().ok_or_else(|| Error::Value("index out of bounds".into()))
    }
}
