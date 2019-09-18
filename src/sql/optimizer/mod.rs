mod constant_folder;

pub use constant_folder::ConstantFolder;

use super::planner::Node;
use crate::Error;

/// A plan optimizer
pub trait Optimizer {
    fn optimize(&mut self, node: Node) -> Result<Node, Error>;
}
