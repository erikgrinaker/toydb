use super::super::schema::Catalog;
use super::super::types::{Environment, Expression, Value};
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
    /// Attempts to push a predicate down into a target node, or returns a regular filter node.
    fn pushdown(mut predicate: Expression, target: Node) -> Result<Node, Error> {
        Ok(match target {
            // Filter nodes immediately before a scan node can be trivially pushed down, as long as
            // we remove any field qualifyers (e.g. movies table aliased as m).
            Node::Scan { table, alias, filter } => {
                let mut map = HashMap::new();
                map.insert(Some(table.clone()), None);
                if let Some(alias) = &alias {
                    map.insert(Some(alias.clone()), None);
                }
                predicate = predicate.transform(&|e| Self::requalify(e, &map), &|e| Ok(e))?;
                if let Some(filter) = filter {
                    predicate = Expression::And(Box::new(predicate), Box::new(filter))
                }
                Node::Scan { table, alias, filter: Some(predicate) }
            }
            // Filter nodes immediately before a nested loop join can be trivially pushed down.
            Node::NestedLoopJoin { outer, inner, predicate: join_predicate, pad, flip } => {
                if let Some(join_predicate) = join_predicate {
                    predicate = Expression::And(Box::new(predicate), Box::new(join_predicate));
                }
                Node::NestedLoopJoin { outer, inner, predicate: Some(predicate), pad, flip }
            }
            n => Node::Filter { predicate, source: Box::new(n) },
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

/// An index lookup optimizer, which converts table scans to index lookups or scans.
pub struct IndexLookup<'a, C: Catalog> {
    catalog: &'a mut C,
}

impl<'a, C: Catalog> IndexLookup<'a, C> {
    pub fn new(catalog: &'a mut C) -> Self {
        Self { catalog }
    }

    pub fn is_lookup(column: &str, expr: &Expression) -> Option<Value> {
        if let Expression::Equal(lhs, rhs) = expr {
            match (*lhs.clone(), *rhs.clone()) {
                (Expression::Field(None, name), Expression::Constant(v))
                | (Expression::Constant(v), Expression::Field(None, name))
                    if name == column =>
                {
                    // FIXME Handle IS NULL instead
                    return match v {
                        Value::Null => None,
                        v => Some(v),
                    };
                }
                _ => {}
            }
        }
        None
    }
}

impl<'a, C: Catalog> Optimizer for IndexLookup<'a, C> {
    fn optimize(&mut self, node: Node) -> Result<Node, Error> {
        node.transform(
            &|n| match n {
                // FIXME This needs to be smarter - at least handle ORs. Could be prettier too.
                Node::Scan { table, alias, filter: Some(expr @ Expression::Equal(_, _)) } => {
                    if let Some(table) = self.catalog.read_table(&table)? {
                        let pk = table.get_primary_key()?.name.clone();
                        if let Some(value) = Self::is_lookup(&pk, &expr) {
                            return Ok(Node::KeyLookup {
                                table: table.name,
                                alias,
                                keys: vec![value],
                            });
                        }
                        for column in table.columns.into_iter().filter(|c| c.index) {
                            if let Some(value) = Self::is_lookup(&column.name, &expr) {
                                return Ok(Node::IndexLookup {
                                    table: table.name,
                                    alias,
                                    column: column.name,
                                    keys: vec![value],
                                });
                            }
                        }
                    }
                    Ok(Node::Scan { table, alias, filter: Some(expr) })
                }
                n => Ok(n),
            },
            &|n| Ok(n),
        )
    }
}
