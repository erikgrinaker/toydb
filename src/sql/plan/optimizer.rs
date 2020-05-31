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
/// FIXME Should optimize true and false filters as well.
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
                Node::Filter { mut source, predicate } => {
                    match self.pushdown(predicate, &mut *source) {
                        Some(remainder) => Ok(Node::Filter { source, predicate: remainder }),
                        None => Ok(*source),
                    }
                }
                Node::NestedLoopJoin {
                    mut outer,
                    outer_size,
                    mut inner,
                    predicate: Some(predicate),
                    pad,
                    flip,
                } => {
                    let predicate =
                        self.pushdown_join(predicate, &mut outer, &mut inner, outer_size);
                    Ok(Node::NestedLoopJoin { outer, outer_size, inner, predicate, pad, flip })
                }
                n => Ok(n),
            },
            &|n| Ok(n),
        )
    }
}

impl FilterPushdown {
    /// Attempts to push a predicate down into a target node, returns any remaining expression.
    fn pushdown(&self, mut predicate: Expression, target: &mut Node) -> Option<Expression> {
        match target {
            // Filter nodes immediately before a scan node can be trivially pushed down.
            Node::Scan { ref mut filter, .. } => {
                if let Some(filter) = filter.take() {
                    predicate = Expression::And(Box::new(predicate), Box::new(filter))
                }
                filter.replace(predicate)
            }
            // Filter nodes immediately before a nested loop join can be trivially pushed down.
            Node::NestedLoopJoin { predicate: ref mut join_predicate, .. } => {
                if let Some(join_predicate) = join_predicate.take() {
                    predicate = Expression::And(Box::new(predicate), Box::new(join_predicate));
                }
                join_predicate.replace(predicate)
            }
            _ => Some(predicate),
        }
    }

    /// Attempts to partition a join predicate and push parts of it down into either source,
    /// returning any remaining expression.
    fn pushdown_join(
        &self,
        predicate: Expression,
        left: &mut Node,
        right: &mut Node,
        boundary: usize,
    ) -> Option<Expression> {
        let cnf = predicate.into_cnf_vec();
        let (push_left, cnf): (Vec<Expression>, Vec<Expression>) = cnf.into_iter().partition(|e| {
            // Partition only if no expressions reference the right-hand source.
            !e.contains(&|e| match e {
                Expression::Field(i, _) if i >= &boundary => true,
                _ => false,
            })
        });
        let (push_right, mut cnf): (Vec<Expression>, Vec<Expression>) =
            // Partition only if no expressions reference the left-hand source.
            cnf.into_iter().partition(|e| {
                !e.contains(&|e| match e {
                    Expression::Field(i, _) if i < &boundary => true,
                    _ => false,
                })
            });
        if let Some(push_left) = Expression::from_cnf_vec(push_left) {
            if let Some(remainder) = self.pushdown(push_left, left) {
                cnf.push(remainder)
            }
        }
        if let Some(mut push_right) = Expression::from_cnf_vec(push_right) {
            // All field references to the right must be shifted left.
            push_right = push_right
                .transform(
                    &|e| match e {
                        Expression::Field(i, label) => Ok(Expression::Field(i - boundary, label)),
                        e => Ok(e),
                    },
                    &|e| Ok(e),
                )
                .unwrap();
            if let Some(remainder) = self.pushdown(push_right, right) {
                cnf.push(remainder)
            }
        }
        Expression::from_cnf_vec(cnf)
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

    // Wraps a node in a filter for the given CNF vector, if any, otherwise returns the bare node.
    fn wrap_cnf(&self, node: Node, cnf: Vec<Expression>) -> Node {
        if let Some(predicate) = Expression::from_cnf_vec(cnf) {
            Node::Filter { source: Box::new(node), predicate }
        } else {
            node
        }
    }
}

impl<'a, C: Catalog> Optimizer for IndexLookup<'a, C> {
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(&|n| Ok(n), &|n| match n {
            Node::Scan { table, alias, filter: Some(filter) } => {
                let columns = self.catalog.must_read_table(&table)?.columns;
                let pk = columns.iter().position(|c| c.primary_key).unwrap();

                // Convert the filter into conjunctive normal form, and try to convert each
                // sub-expression into a lookup. If a lookup is found, return a lookup node and then
                // apply the remaining conjunctions as a filter node, if any.
                let mut cnf = filter.clone().into_cnf_vec();
                for i in 0..cnf.len() {
                    if let Some(keys) = self.as_lookups(pk, &cnf[i]) {
                        cnf.remove(i);
                        return Ok(self.wrap_cnf(Node::KeyLookup { table, alias, keys }, cnf));
                    }
                    for (ci, column) in columns.iter().enumerate().filter(|(_, c)| c.index) {
                        if let Some(values) = self.as_lookups(ci, &cnf[i]) {
                            cnf.remove(i);
                            return Ok(self.wrap_cnf(
                                Node::IndexLookup {
                                    table,
                                    alias,
                                    column: column.name.clone(),
                                    values,
                                },
                                cnf,
                            ));
                        }
                    }
                }
                Ok(Node::Scan { table, alias, filter: Some(filter) })
            }
            n => Ok(n),
        })
    }
}
