use super::super::types::{Environment, Expression};
use super::Node;
use crate::Error;

use std::collections::HashMap;

/// A plan optimizer
pub trait Optimizer {
    fn optimize(&mut self, node: Node) -> Result<Node, Error>;
}

/// A constant folding optimizer, which replaces constant expressions
/// with their evaluated value, to prevent it from being re-evaluated
/// over and over again during plan execution.
pub struct ConstantFolder;

impl Optimizer for ConstantFolder {
    fn optimize(&mut self, node: Node) -> Result<Node, Error> {
        let env = Environment::new();
        node.transform(&|n| Ok(n), &|n| {
            n.transform_expressions(&|e| Ok(e), &|e| {
                Ok(if e.is_constant() { Expression::Constant(e.evaluate(&env)?) } else { e })
            })
        })
    }
}

/// A filter pushdown optimizer, which moves filter predicates into or
/// closer to the source node.
pub struct FilterPushdown;

impl Optimizer for FilterPushdown {
    fn optimize(&mut self, node: Node) -> Result<Node, Error> {
        node.transform(
            &|n| match n {
                Node::Filter { source, predicate } => Self::pushdown(predicate, *source),
                n => Ok(n),
            },
            &|n| Ok(n),
        )
    }
}

impl FilterPushdown {
    /// Attempts to push a filter down into a target node, or returns a regular filter node.
    fn pushdown(filter: Expression, target: Node) -> Result<Node, Error> {
        Ok(match target {
            // Filter nodes immediately before a scan node without a filter can be trivially pushed
            // down, as long as we remove any qualified fields (e.g. movies table aliased as m).
            Node::Scan { table, alias, filter: None } => {
                let mut map = HashMap::new();
                map.insert(Some(table.clone()), None);
                if let Some(alias) = &alias {
                    map.insert(Some(alias.clone()), None);
                }
                Node::Scan {
                    table,
                    alias,
                    filter: Some(filter.transform(&|e| Self::requalify(e, &map), &|e| Ok(e))?),
                }
            }
            // FIXME We need to handle joins.
            n => Node::Filter { predicate: filter, source: Box::new(n) },
        })
    }

    /// Rewrites field relations, e.g. for mapping "movies.id" or "m.id" to "id". If a field
    /// cannot be rewritten, the original field is returned (to propagate errors).
    fn requalify(
        expr: Expression,
        map: &HashMap<Option<String>, Option<String>>,
    ) -> Result<Expression, Error> {
        Ok(match expr {
            Expression::Field(relation, name) => match map.get(&relation) {
                Some(mapped) => Expression::Field(mapped.clone(), name),
                None => Expression::Field(relation, name),
            },
            expr => expr,
        })
    }
}
