use super::super::engine::Transaction;
use super::super::plan::Node;
use super::{Context, Executor, ResultSet};
use crate::Error;

/// An executor that produces a query plan
pub struct Explain(Node);

impl Explain {
    pub fn new(plan: Node) -> Box<Self> {
        Box::new(Self(plan))
    }
}

impl<T: Transaction> Executor<T> for Explain {
    fn execute(self: Box<Self>, _ctx: &mut Context<T>) -> Result<ResultSet, Error> {
        Ok(ResultSet::Explain(self.0))
    }
}
