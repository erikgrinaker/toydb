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

use super::planner::Node;
use super::types::Row;
use super::Storage;
use crate::Error;

/// A plan executor
pub trait Executor: Sync + Send + 'static {
    //fn affected(&self) -> Option<u64>;
    fn columns(&self) -> Vec<String>;
    fn fetch(&mut self) -> Result<Option<Row>, Error>;
}

impl dyn Executor {
    /// Executes a plan node, consuming it
    pub fn execute(ctx: &mut Context, node: Node) -> Result<Box<Self>, Error> {
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
            Node::Order{source, orders} => {
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
pub struct Context {
    /// The underlying storage
    pub storage: Box<Storage>,
}
