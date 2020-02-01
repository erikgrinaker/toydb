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

use super::engine::Transaction;
use super::planner::Node;
use super::types::Row;
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
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Delete { table, source } => Delete::new(table, Self::build(*source)),
            Node::DropTable { name } => DropTable::new(name),
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
            Node::Insert { table, columns, expressions } => {
                Insert::new(table, columns, expressions)
            }
            Node::Limit { source, limit } => Limit::new(Self::build(*source), limit),
            Node::NestedLoopJoin { outer, inner } => {
                NestedLoopJoin::new(Self::build(*outer), Self::build(*inner))
            }
            Node::Nothing => Nothing::new(),
            Node::Offset { source, offset } => Offset::new(Self::build(*source), offset),
            Node::Order { source, orders } => Order::new(Self::build(*source), orders),
            Node::Projection { source, labels, expressions } => {
                Projection::new(Self::build(*source), labels, expressions)
            }
            Node::Scan { table } => Scan::new(table),
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

/// An executor result
pub struct ResultSet {
    /// The executor effect (i.e. mutation), if any
    effect: Option<Effect>,
    /// The column names of the result
    columns: Vec<String>,
    /// The result rows
    rows: Option<ResultRows>,
}

impl ResultSet {
    /// Creates a new result set
    pub fn new(effect: Option<Effect>, columns: Vec<String>, rows: Option<ResultRows>) -> Self {
        Self { effect, columns, rows }
    }

    /// Creates a new result set for an effect
    pub fn from_effect(effect: Effect) -> Self {
        Self { effect: Some(effect), columns: Vec::new(), rows: None }
    }

    /// Creates a new result set for a row iterator
    pub fn from_rows(columns: Vec<String>, rows: ResultRows) -> Self {
        Self { effect: None, columns, rows: Some(rows) }
    }

    /// Returns the result columns
    pub fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    /// Returns the query effect, if any
    pub fn effect(&self) -> Option<Effect> {
        self.effect.clone()
    }
}

impl Iterator for ResultSet {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // Make sure iteration is aborted on the first error, otherwise callers
        // will keep calling next for as long as it keeps returning errors
        if let Some(ref mut iter) = self.rows {
            let result = iter.next();
            if let Some(Err(_)) = result {
                self.rows = None
            }
            result
        } else {
            None
        }
    }
}

type ResultRows = Box<dyn Iterator<Item = Result<Row, Error>> + Send>;

#[derive(Clone, Debug, Serialize, Deserialize)]
/// An executor effect
pub enum Effect {
    // Transaction started
    Begin { id: u64, mode: super::engine::Mode },
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
}
