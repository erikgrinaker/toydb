mod expression;
mod node;
mod planner;

pub use node::Node;

use super::parser::ast;
use super::types::Row;
use crate::Error;
use planner::Planner;

/// A plan
#[derive(Debug)]
pub struct Plan {
    /// The plan column names
    pub columns: Vec<String>,

    /// The plan root
    pub root: Node,
}

impl Iterator for Plan {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Result<Row, Error>> {
        self.root.next()
    }
}

impl Plan {
    /// Builds a plan from a statement AST
    pub fn build(statement: ast::Statement) -> Result<Self, Error> {
        Planner::new().build(statement)
    }
}
