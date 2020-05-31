use super::super::schema::Catalog;
use super::super::types::{Expression, Value};
use super::Node;
use crate::error::Result;

/// A plan optimizer
pub trait Optimizer {
    fn optimize(&self, node: Node) -> Result<Node>;
}

/// A constant folding optimizer, which replaces constant expressions with their evaluated value, to
/// prevent it from being re-evaluated over and over again during plan execution.
pub struct ConstantFolder;

impl Optimizer for ConstantFolder {
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(&|n| Ok(n), &|n| {
            n.transform_expressions(
                &|e| {
                    if !e.contains(&|expr| matches!(expr, Expression::Field(_, _))) {
                        Ok(Expression::Constant(e.evaluate(None)?))
                    } else {
                        Ok(e)
                    }
                },
                &|e| Ok(e),
            )
        })
    }
}

/// A filter pushdown optimizer, which moves filter predicates into or closer to the source node.
pub struct FilterPushdown;

impl Optimizer for FilterPushdown {
    fn optimize(&self, node: Node) -> Result<Node> {
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
            // Filter nodes immediately before a scan node can be trivially pushed down.
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
            // Pushdown failure just returns the filter node.
            n => Node::Filter { predicate, source: Box::new(n) },
        })
    }
}

/// An index lookup optimizer, which converts table scans to index lookups.
pub struct IndexLookup<'a, C: Catalog> {
    catalog: &'a mut C,
}

impl<'a, C: Catalog> IndexLookup<'a, C> {
    pub fn new(catalog: &'a mut C) -> Self {
        Self { catalog }
    }

    // Attempts to convert the given expression into a list of field values. Expressions must be a
    // combination of =, IS NULL, OR to be converted.
    fn as_lookups(&self, field: usize, expr: &Expression) -> Option<Vec<Value>> {
        use Expression::*;
        // FIXME This should use a single match level, but since the child expressions are boxed
        // that would require box patterns, which are unstable.
        match &*expr {
            Equal(lhs, rhs) => match (&**lhs, &**rhs) {
                (Field(i, _), Constant(v)) if i == &field => Some(vec![v.clone()]),
                (Constant(v), Field(i, _)) if i == &field => Some(vec![v.clone()]),
                (_, _) => None,
            },
            IsNull(e) => match &**e {
                Field(i, _) if i == &field => Some(vec![Value::Null]),
                _ => None,
            },
            Or(lhs, rhs) => match (self.as_lookups(field, lhs), self.as_lookups(field, rhs)) {
                (Some(mut lvalues), Some(mut rvalues)) => {
                    lvalues.append(&mut rvalues);
                    Some(lvalues)
                }
                _ => None,
            },
            _ => None,
        }
    }
}

impl<'a, C: Catalog> Optimizer for IndexLookup<'a, C> {
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(&|n| Ok(n), &|n| match n {
            Node::Scan { table, alias, filter: Some(filter) } => {
                let columns = self.catalog.must_read_table(&table)?.columns;
                let pk = columns.iter().position(|c| c.primary_key).unwrap();
                if let Some(keys) = self.as_lookups(pk, &filter) {
                    return Ok(Node::KeyLookup { table, alias, keys });
                }
                for (i, column) in columns.into_iter().enumerate().filter(|(_, c)| c.index) {
                    if let Some(values) = self.as_lookups(i, &filter) {
                        return Ok(Node::IndexLookup { table, alias, column: column.name, values });
                    }
                }
                Ok(Node::Scan { table, alias, filter: Some(filter) })
            }
            n => Ok(n),
        })
    }
}
