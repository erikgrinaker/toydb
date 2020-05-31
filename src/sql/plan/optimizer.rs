use super::super::schema::Catalog;
use super::super::types::{Expression, Value};
use super::Node;
use crate::error::Result;

/// A plan optimizer
pub trait Optimizer {
    fn optimize(&mut self, node: Node) -> Result<Node>;
}

/// A constant folding optimizer, which replaces constant expressions with their evaluated value, to
/// prevent it from being re-evaluated over and over again during plan execution.
pub struct ConstantFolder;

impl Optimizer for ConstantFolder {
    fn optimize(&mut self, node: Node) -> Result<Node> {
        node.transform(
            &|n| {
                n.transform_expressions(
                    &|e| {
                        if !e.contains(&|expr| match expr {
                            Expression::Field(_, _) => true,
                            _ => false,
                        }) {
                            Ok(Expression::Constant(e.evaluate(None)?))
                        } else {
                            Ok(e)
                        }
                    },
                    &|e| Ok(e),
                )
            },
            &|n| Ok(n),
        )
    }
}

/// A filter pushdown optimizer, which moves filter predicates into or
/// closer to the source node.
pub struct FilterPushdown;

impl Optimizer for FilterPushdown {
    fn optimize(&mut self, node: Node) -> Result<Node> {
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
    fn pushdown(mut predicate: Expression, target: Node) -> Result<Node> {
        Ok(match target {
            // Filter nodes immediately before a scan node can be trivially pushed down, as long as
            // we remove any field qualifyers (e.g. movies table aliased as m).
            Node::Scan { table, alias, filter } => {
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
}

/// An index lookup optimizer, which converts table scans to index lookups or scans.
pub struct IndexLookup<'a, C: Catalog> {
    catalog: &'a mut C,
}

impl<'a, C: Catalog> IndexLookup<'a, C> {
    pub fn new(catalog: &'a mut C) -> Self {
        Self { catalog }
    }

    pub fn is_lookup(field: usize, expr: &Expression) -> Option<Value> {
        if let Expression::Equal(lhs, rhs) = expr {
            match (*lhs.clone(), *rhs.clone()) {
                (Expression::Field(i, _), Expression::Constant(v))
                | (Expression::Constant(v), Expression::Field(i, _))
                    if field == i =>
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
    fn optimize(&mut self, node: Node) -> Result<Node> {
        node.transform(
            &|n| match n {
                // FIXME This needs to be smarter - at least handle ORs. Could be prettier too.
                Node::Scan { table, alias, filter: Some(expr @ Expression::Equal(_, _)) } => {
                    if let Some(table) = self.catalog.read_table(&table)? {
                        let pk = table.get_column_index(&table.get_primary_key()?.name)?;
                        if let Some(value) = Self::is_lookup(pk, &expr) {
                            return Ok(Node::KeyLookup {
                                table: table.name,
                                alias,
                                keys: vec![value],
                            });
                        }
                        for (i, column) in
                            table.columns.into_iter().enumerate().filter(|(_, c)| c.index)
                        {
                            if let Some(value) = Self::is_lookup(i, &expr) {
                                return Ok(Node::IndexLookup {
                                    table: table.name,
                                    alias,
                                    column: column.name,
                                    values: vec![value],
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
