mod create_table;
mod delete;
mod drop_table;
mod filter;
mod insert;
mod limit;
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
use nothing::Nothing;
use offset::Offset;
use order::Order;
use projection::Projection;
use scan::Scan;
use update::Update;

use super::engine::Transaction;
use super::planner::Node;
use super::types::Row;
use crate::client;
use crate::Error;

/// A plan executor
pub trait Executor: 'static + Sync + Send {
    //fn affected(&self) -> Option<u64>;
    fn columns(&self) -> Vec<String>;
    fn fetch(&mut self) -> Result<Option<Row>, Error>;
}

impl dyn Executor {
    /// Executes a plan node, consuming it
    pub fn execute<T: Transaction>(ctx: &mut Context<T>, node: Node) -> Result<Box<Self>, Error> {
        Ok(match node {
            Node::CreateTable { schema } => CreateTable::execute(ctx, schema)?,
            Node::Delete { table, source } => {
                let source = Self::execute(ctx, *source)?;
                Delete::execute(ctx, source, table)?
            }
            Node::DropTable { name } => DropTable::execute(ctx, name)?,
            Node::Filter { source, predicate } => {
                let source = Self::execute(ctx, *source)?;
                Filter::execute(ctx, source, predicate)?
            }
            Node::Insert { table, columns, expressions } => {
                Insert::execute(ctx, &table, columns, expressions)?
            }
            Node::Limit { source, limit } => {
                let source = Self::execute(ctx, *source)?;
                Limit::execute(ctx, source, limit)?
            }
            Node::Nothing => Nothing::execute(ctx)?,
            Node::Offset { source, offset } => {
                let source = Self::execute(ctx, *source)?;
                Offset::execute(ctx, source, offset)?
            }
            Node::Order { source, orders } => {
                let source = Self::execute(ctx, *source)?;
                Order::execute(ctx, source, orders)?
            }
            Node::Projection { source, labels, expressions } => {
                let source = Self::execute(ctx, *source)?;
                Projection::execute(ctx, source, labels, expressions)?
            }
            Node::Scan { table } => Scan::execute(ctx, table)?,
            Node::Update { table, source, expressions } => {
                let source = Self::execute(ctx, *source)?;
                Update::execute(ctx, table, source, expressions)?
            }
        })
    }
}

impl Iterator for dyn Executor {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fetch().transpose()
    }
}

/// A plan execution context
pub struct Context<'a, T: Transaction> {
    /// The underlying storage engine
    pub txn: &'a mut T,
}

/// An execution result set
pub struct ResultSet {
    // FIXME Shouldn't be public, and shouldn't use client package
    pub effect: Option<client::Effect>,
    columns: Vec<String>,
    executor: Option<Box<dyn Executor>>,
}

impl ResultSet {
    /// Creates an empty result set
    pub fn empty() -> Self {
        Self { effect: None, columns: Vec::new(), executor: None }
    }

    /// Creates a result set from an executor
    pub fn from_executor(executor: Box<dyn Executor>) -> Self {
        Self { effect: None, columns: executor.columns(), executor: Some(executor) }
    }

    /// Fetches the columns of the result set
    pub fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }
}

impl Iterator for ResultSet {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // Make sure iteration is aborted on the first error, otherwise callers
        // may keep calling next for as long as it keeps returning errors
        if let Some(ref mut iter) = self.executor {
            match iter.next() {
                Some(Err(err)) => {
                    self.executor = None;
                    Some(Err(err))
                }
                r => r,
            }
        } else {
            None
        }
    }
}
