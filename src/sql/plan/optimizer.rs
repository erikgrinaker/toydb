use super::super::schema::Catalog;
use super::super::types::{Expression, Value};
use super::Node;
use crate::error::Result;

use std::mem::replace;

/// A plan optimizer
pub trait Optimizer {
    fn optimize(&self, node: Node) -> Result<Node>;
}

/// A constant folding optimizer, which replaces constant expressions with their evaluated value, to
/// prevent it from being re-evaluated over and over again during plan execution.
pub struct ConstantFolder;

impl Optimizer for ConstantFolder {
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(&Ok, &|n| {
            n.transform_expressions(
                &|e| {
                    if !e.contains(&|expr| matches!(expr, Expression::Field(_, _))) {
                        Ok(Expression::Constant(e.evaluate(None)?))
                    } else {
                        Ok(e)
                    }
                },
                &Ok,
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
                    // We don't replace the filter node here, since doing so would cause transform()
                    // to skip the source as it won't reapply the transform to the "same" node.
                    // We leave a noop filter node instead, which will be cleaned up by NoopCleaner.
                    if let Some(remainder) = self.pushdown(predicate, &mut source) {
                        Ok(Node::Filter { source, predicate: remainder })
                    } else {
                        Ok(Node::Filter {
                            source,
                            predicate: Expression::Constant(Value::Boolean(true)),
                        })
                    }
                }
                Node::NestedLoopJoin {
                    mut left,
                    left_size,
                    mut right,
                    predicate: Some(predicate),
                    outer,
                } => {
                    let predicate = self.pushdown_join(predicate, &mut left, &mut right, left_size);
                    Ok(Node::NestedLoopJoin { left, left_size, right, predicate, outer })
                }
                n => Ok(n),
            },
            &Ok,
        )
    }
}

impl FilterPushdown {
    /// Attempts to push an expression down into a target node, returns any remaining expression.
    fn pushdown(&self, mut expression: Expression, target: &mut Node) -> Option<Expression> {
        match target {
            Node::Scan { ref mut filter, .. } => {
                if let Some(filter) = filter.take() {
                    expression = Expression::And(Box::new(expression), Box::new(filter))
                }
                filter.replace(expression)
            }
            Node::NestedLoopJoin { ref mut predicate, .. } => {
                if let Some(predicate) = predicate.take() {
                    expression = Expression::And(Box::new(expression), Box::new(predicate));
                }
                predicate.replace(expression)
            }
            Node::Filter { ref mut predicate, .. } => {
                let p = replace(predicate, Expression::Constant(Value::Null));
                *predicate = Expression::And(Box::new(p), Box::new(expression));
                None
            }
            _ => Some(expression),
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
        // Convert the predicate into conjunctive normal form, and partition into expressions
        // only referencing the left or right sources, leaving cross-source expressions.
        let cnf = predicate.into_cnf_vec();
        let (mut push_left, cnf): (Vec<Expression>, Vec<Expression>) =
            cnf.into_iter().partition(|e| {
                // Partition only if no expressions reference the right-hand source.
                !e.contains(&|e| matches!(e, Expression::Field(i, _) if i >= &boundary))
            });
        let (mut push_right, mut cnf): (Vec<Expression>, Vec<Expression>) =
            cnf.into_iter().partition(|e| {
                // Partition only if no expressions reference the left-hand source.
                !e.contains(&|e| matches!(e, Expression::Field(i, _) if i < &boundary))
            });

        // Look for equijoins that have constant lookups on either side, and transfer the constants
        // to the other side of the join as well. This allows index lookup optimization in both
        // sides. We already know that the remaining cnf expressions span both sources.
        for e in &cnf {
            if let Expression::Equal(ref lhs, ref rhs) = e {
                if let (Expression::Field(l, ln), Expression::Field(r, rn)) = (&**lhs, &**rhs) {
                    let (l, ln, r, rn) = if l > r { (r, rn, l, ln) } else { (l, ln, r, rn) };
                    if let Some(lvals) = push_left.iter().find_map(|e| e.as_lookup(*l)) {
                        push_right.push(Expression::from_lookup(*r, rn.clone(), lvals));
                    } else if let Some(rvals) = push_right.iter().find_map(|e| e.as_lookup(*r)) {
                        push_left.push(Expression::from_lookup(*l, ln.clone(), rvals));
                    }
                }
            }
        }

        // Push predicates down into the sources.
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
                    &Ok,
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
        node.transform(&Ok, &|n| match n {
            Node::Scan { table, alias, filter: Some(filter) } => {
                let columns = self.catalog.must_read_table(&table)?.columns;
                let pk = columns.iter().position(|c| c.primary_key).unwrap();

                // Convert the filter into conjunctive normal form, and try to convert each
                // sub-expression into a lookup. If a lookup is found, return a lookup node and then
                // apply the remaining conjunctions as a filter node, if any.
                let mut cnf = filter.clone().into_cnf_vec();
                for i in 0..cnf.len() {
                    if let Some(keys) = cnf[i].as_lookup(pk) {
                        cnf.remove(i);
                        return Ok(self.wrap_cnf(Node::KeyLookup { table, alias, keys }, cnf));
                    }
                    for (ci, column) in columns.iter().enumerate().filter(|(_, c)| c.index) {
                        if let Some(values) = cnf[i].as_lookup(ci) {
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

/// Cleans up noops, e.g. filters with constant true/false predicates.
/// FIXME This should perhaps replace nodes that can never return anything with a Nothing node,
/// but that requires propagating the column names.
pub struct NoopCleaner;

impl Optimizer for NoopCleaner {
    fn optimize(&self, node: Node) -> Result<Node> {
        use Expression::*;
        node.transform(
            // While descending the node tree, clean up boolean expressions.
            &|n| {
                n.transform_expressions(&Ok, &|e| match &e {
                    And(lhs, rhs) => match (&**lhs, &**rhs) {
                        (Constant(Value::Boolean(false)), _)
                        | (Constant(Value::Null), _)
                        | (_, Constant(Value::Boolean(false)))
                        | (_, Constant(Value::Null)) => Ok(Constant(Value::Boolean(false))),
                        (Constant(Value::Boolean(true)), e)
                        | (e, Constant(Value::Boolean(true))) => Ok(e.clone()),
                        _ => Ok(e),
                    },
                    Or(lhs, rhs) => match (&**lhs, &**rhs) {
                        (Constant(Value::Boolean(false)), e)
                        | (Constant(Value::Null), e)
                        | (e, Constant(Value::Boolean(false)))
                        | (e, Constant(Value::Null)) => Ok(e.clone()),
                        (Constant(Value::Boolean(true)), _)
                        | (_, Constant(Value::Boolean(true))) => Ok(Constant(Value::Boolean(true))),
                        _ => Ok(e),
                    },
                    // No need to handle Not, constant folder should have evaluated it already.
                    _ => Ok(e),
                })
            },
            // While ascending the node tree, remove any unnecessary filters or nodes.
            // FIXME This should replace scan and join predicates with None as well.
            &|n| match n {
                Node::Filter { source, predicate } => match predicate {
                    Expression::Constant(Value::Boolean(true)) => Ok(*source),
                    predicate => Ok(Node::Filter { source, predicate }),
                },
                n => Ok(n),
            },
        )
    }
}

// Optimizes join types, currently by swapping nested-loop joins with hash joins where appropriate.
pub struct JoinType;

impl Optimizer for JoinType {
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(
            &|n| match n {
                // Replace nested-loop equijoins with hash joins.
                Node::NestedLoopJoin {
                    left,
                    left_size,
                    right,
                    predicate: Some(Expression::Equal(a, b)),
                    outer,
                } => match (*a, *b) {
                    (Expression::Field(a, a_label), Expression::Field(b, b_label)) => {
                        let (left_field, right_field) = if a < left_size {
                            ((a, a_label), (b - left_size, b_label))
                        } else {
                            ((b, b_label), (a - left_size, a_label))
                        };
                        Ok(Node::HashJoin { left, left_field, right, right_field, outer })
                    }
                    (a, b) => Ok(Node::NestedLoopJoin {
                        left,
                        left_size,
                        right,
                        predicate: Some(Expression::Equal(a.into(), b.into())),
                        outer,
                    }),
                },
                n => Ok(n),
            },
            &Ok,
        )
    }
}
