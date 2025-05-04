use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::LazyLock;

use super::Node;
use crate::error::Result;
use crate::sql::types::{Expression, Label, Value};

/// The set of optimizers, and the order in which they are applied.
pub static OPTIMIZERS: LazyLock<Vec<Box<dyn Optimizer>>> = LazyLock::new(|| {
    vec![
        Box::new(ConstantFolding),
        Box::new(FilterPushdown),
        Box::new(IndexLookup),
        Box::new(HashJoin),
        Box::new(ShortCircuit),
    ]
});

/// A node optimizer, which recursively transforms a plan node to make plan
/// execution more efficient where possible.
pub trait Optimizer: Debug + Send + Sync {
    /// Optimizes a node, returning the optimized node.
    fn optimize(&self, node: Node) -> Result<Node>;
}

/// Folds constant expressions by pre-evaluating them once now, instead of
/// re-evaluating them for every row during execution.
#[derive(Debug)]
pub struct ConstantFolding;

impl Optimizer for ConstantFolding {
    fn optimize(&self, node: Node) -> Result<Node> {
        // Recursively transform expressions in the node tree. Post-order to
        // partially fold child expressions as far as possible, and avoid
        // quadratic costs.
        node.transform(&|node| node.transform_expressions(&Ok, &Self::fold), &Ok)
    }
}

impl ConstantFolding {
    /// Folds constant expressions in a node.
    pub fn fold(mut expr: Expression) -> Result<Expression> {
        use Expression::*;
        use Value::*;

        // If the expression is constant, evaluate it.
        //
        // This is a very simple approach, which doesn't handle more complex
        // cases such as 1 + a - 2 (which would require rearranging the
        // expression as 1 - 2 + a to evaluate the 1 - 2 branch).
        //
        // TODO: consider doing something better.
        if !expr.contains(&|expr| matches!(expr, Column(_))) {
            return expr.evaluate(None).map(Constant);
        }

        // If the expression is a logical operator, and one of the sides is
        // constant, we may be able to evaluate it even if it has a column
        // reference. For example, a AND FALSE is always FALSE, regardless of
        // what a is.
        expr = match expr {
            And(lhs, rhs) => match (*lhs, *rhs) {
                // If either side of an AND is false, the AND is false.
                (Constant(Boolean(false)), _) | (_, Constant(Boolean(false))) => {
                    Constant(Boolean(false))
                }
                // If either side of an AND is true, the AND is redundant.
                (Constant(Boolean(true)), expr) | (expr, Constant(Boolean(true))) => expr,
                (lhs, rhs) => And(lhs.into(), rhs.into()),
            },

            Or(lhs, rhs) => match (*lhs, *rhs) {
                // If either side of an OR is true, the OR is true.
                (Constant(Boolean(true)), _) | (_, Constant(Boolean(true))) => {
                    Constant(Boolean(true))
                }
                // If either side of an OR is false, the OR is redundant.
                (Constant(Boolean(false)), expr) | (expr, Constant(Boolean(false))) => expr,
                (lhs, rhs) => Or(lhs.into(), rhs.into()),
            },

            expr => expr,
        };

        Ok(expr)
    }
}

/// Pushes filter predicates down into child nodes where possible. In
/// particular, this can perform filtering during storage scans (below Raft),
/// instead of reading and transmitting all rows across the network before
/// filtering, by pushing a predicate from a Filter node down into a Scan node.
#[derive(Debug)]
pub struct FilterPushdown;

impl Optimizer for FilterPushdown {
    fn optimize(&self, node: Node) -> Result<Node> {
        // Push down before descending, so we can keep recursively pushing down.
        node.transform(&|node| Ok(Self::push_filters(node)), &Ok)
    }
}

impl FilterPushdown {
    /// Pushes filter predicates down into child nodes where possible.
    fn push_filters(mut node: Node) -> Node {
        node = Self::maybe_push_filter(node);
        node = Self::maybe_push_join(node);
        node
    }

    /// Pushes an expression into a node if possible. Otherwise, returns the the
    /// unpushed expression.
    fn push_into(expr: Expression, target: &mut Node) -> Option<Expression> {
        match target {
            Node::Filter { predicate, .. } => {
                // Temporarily replace the predicate to take ownership.
                let rhs = std::mem::replace(predicate, Expression::Constant(Value::Null));
                *predicate = Expression::And(expr.into(), rhs.into());
            }
            Node::NestedLoopJoin { predicate, .. } => {
                *predicate = match predicate.take() {
                    Some(predicate) => Some(Expression::And(expr.into(), predicate.into())),
                    None => Some(expr),
                };
            }
            Node::Scan { filter, .. } => {
                *filter = match filter.take() {
                    Some(filter) => Some(Expression::And(expr.into(), filter.into())),
                    None => Some(expr),
                };
            }
            // Unable to push down, just return the original expression.
            _ => return Some(expr),
        }
        None
    }

    /// Pushes a filter node predicate down into its source, if possible.
    fn maybe_push_filter(node: Node) -> Node {
        let Node::Filter { mut source, predicate } = node else {
            return node;
        };
        // Attempt to push the filter into the source, or return the original.
        if let Some(predicate) = Self::push_into(predicate, &mut source) {
            return Node::Filter { source, predicate };
        }
        // Push succeded, return the source that was pushed into. When we
        // replace this filter node with the source node, Node.transform() will
        // skip the source node since it now takes the place of the original
        // filter node. Transform the source manually.
        Self::push_filters(*source)
    }

    // Pushes down parts of a join predicate into the left or right sources
    // where possible.
    fn maybe_push_join(node: Node) -> Node {
        let Node::NestedLoopJoin { mut left, mut right, predicate: Some(predicate), outer } = node
        else {
            return node;
        };
        // Convert the predicate into conjunctive normal form (an AND vector).
        let cnf = predicate.into_cnf_vec();

        // Push down expressions that don't reference both sources. Constant
        // expressions can be pushed down into both.
        let (mut push_left, mut push_right, mut predicate) = (Vec::new(), Vec::new(), Vec::new());
        for expr in cnf {
            let (mut ref_left, mut ref_right) = (false, false);
            expr.walk(&mut |expr| {
                if let Expression::Column(index) = expr {
                    ref_left = ref_left || *index < left.columns();
                    ref_right = ref_right || *index >= left.columns();
                }
                !(ref_left && ref_right) // exit once both are referenced
            });
            match (ref_left, ref_right) {
                (true, true) => predicate.push(expr),
                (true, false) => push_left.push(expr),
                (false, true) => push_right.push(expr),
                (false, false) => {
                    push_left.push(expr.clone());
                    push_right.push(expr);
                }
            }
        }

        // In the remaining cross-source expressions, look for equijoins where
        // one side also has constant value lookups. In this case we can copy
        // the constant lookups to the other side, to allow index lookups. This
        // commonly happens when joining a foreign key (which is indexed) on a
        // primary key, and we want to make use of the foreign key index, e.g.:
        //
        // SELECT m.name, g.name FROM movies m JOIN genres g ON m.genre_id = g.id AND g.id = 7;
        let left_lookups: HashMap<usize, usize> = push_left // column → push_left index
            .iter()
            .enumerate()
            .filter_map(|(i, expr)| expr.is_column_lookup().map(|column| (column, i)))
            .collect();
        let right_lookups: HashMap<usize, usize> = push_right // column → push_right index
            .iter()
            .enumerate()
            .filter_map(|(i, expr)| expr.is_column_lookup().map(|column| (column, i)))
            .collect();

        for expr in &predicate {
            // Find equijoins.
            let Expression::Equal(lhs, rhs) = expr else { continue };
            let Expression::Column(mut l) = **lhs else { continue };
            let Expression::Column(mut r) = **rhs else { continue };

            // The lhs may be a reference to the right source; swap them.
            if l > r {
                (l, r) = (r, l)
            }

            // Check if either side is a column lookup, and copy it over.
            if let Some(expr) = left_lookups.get(&l).map(|i| push_left[*i].clone()) {
                push_right.push(expr.replace_column(l, r));
            }
            if let Some(expr) = right_lookups.get(&r).map(|i| push_right[*i].clone()) {
                push_left.push(expr.replace_column(r, l));
            }
        }

        // Push predicates down into the sources if possible.
        if let Some(expr) = Expression::and_vec(push_left) {
            if let Some(expr) = Self::push_into(expr, &mut left) {
                // Pushdown failed, put it back into the join predicate.
                predicate.push(expr)
            }
        }

        if let Some(mut expr) = Expression::and_vec(push_right) {
            // Right columns have indexes in the joined row; shift them left.
            expr = expr.shift_column(-(left.columns() as isize));
            if let Some(mut expr) = Self::push_into(expr, &mut right) {
                // Pushdown failed, undo the column index shift.
                expr = expr.shift_column(left.columns() as isize);
                predicate.push(expr)
            }
        }

        // Leave any remaining predicates in the join node.
        let predicate = Expression::and_vec(predicate);
        Node::NestedLoopJoin { left, right, predicate, outer }
    }
}

/// Uses a primary key or secondary index lookup where possible.
#[derive(Debug)]
pub struct IndexLookup;

impl Optimizer for IndexLookup {
    fn optimize(&self, node: Node) -> Result<Node> {
        // Recursively transform expressions in the node tree. Post-order to
        // partially fold child expressions as far as possible, and avoid
        // quadratic costs.
        node.transform(&|node| Ok(Self::index_lookup(node)), &Ok)
    }
}

impl IndexLookup {
    /// Rewrites a filtered scan node into a key or index lookup if possible.
    fn index_lookup(mut node: Node) -> Node {
        // Only handle scan filters. Assume FilterPushdown has pushed filters
        // into scan nodes first.
        let Node::Scan { table, alias, filter: Some(filter) } = node else {
            return node;
        };

        // Convert the filter into conjunctive normal form (a list of ANDs).
        let mut cnf = filter.clone().into_cnf_vec();

        // Find the first expression that's either a primary key or secondary
        // index lookup. We could be more clever here, but this is fine.
        let Some((i, column)) = cnf.iter().enumerate().find_map(|(i, expr)| {
            expr.is_column_lookup()
                .filter(|&c| c == table.primary_key || table.columns[c].index)
                .map(|column| (i, column))
        }) else {
            // No index lookups found, return the original node.
            return Node::Scan { table, alias, filter: Some(filter) };
        };

        // Extract the lookup values and expression from the cnf vector.
        let values = cnf.remove(i).into_column_values(column);

        // Build the primary key or secondary index lookup node.
        if column == table.primary_key {
            node = Node::KeyLookup { table, keys: values, alias };
        } else {
            node = Node::IndexLookup { table, column, values, alias };
        }

        // If there's any remaining CNF expressions, add a filter node for them.
        if let Some(predicate) = Expression::and_vec(cnf) {
            node = Node::Filter { source: Box::new(node), predicate };
        }

        node
    }
}

/// Uses a hash join instead of a nested loop join for single-column equijoins.
#[derive(Debug)]
pub struct HashJoin;

impl Optimizer for HashJoin {
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(&|node| Ok(Self::hash_join(node)), &Ok)
    }
}

impl HashJoin {
    /// Rewrites a nested loop join into a hash join if possible.
    pub fn hash_join(node: Node) -> Node {
        let Node::NestedLoopJoin {
            left,
            right,
            predicate: Some(Expression::Equal(lhs, rhs)),
            outer,
        } = node
        else {
            return node;
        };

        match (*lhs, *rhs) {
            // If this is a single-column equijoin, use a hash join.
            (Expression::Column(mut left_column), Expression::Column(mut right_column)) => {
                // The LHS column may be a column in the right table; swap them.
                if right_column < left_column {
                    (left_column, right_column) = (right_column, left_column);
                }
                // The NestedLoopJoin predicate uses column indexes in the
                // joined row, while the HashJoin uses column indexes in each
                // individual table. Adjust the RHS column reference.
                right_column -= left.columns();
                Node::HashJoin { left, left_column, right, right_column, outer }
            }
            // Otherwise, retain the nested loop join.
            (lhs, rhs) => {
                let predicate = Some(Expression::Equal(lhs.into(), rhs.into()));
                Node::NestedLoopJoin { left, right, predicate, outer }
            }
        }
    }
}

/// Short-circuits useless nodes and expressions (for example a Filter node that
/// always evaluates to false), by removing them and/or replacing them with
/// Nothing nodes that yield no rows.
#[derive(Debug)]
pub struct ShortCircuit;

impl Optimizer for ShortCircuit {
    fn optimize(&self, node: Node) -> Result<Node> {
        // Post-order transform, to pull Nothing nodes upwards in the tree.
        node.transform(&Ok, &|node| Ok(Self::short_circuit(node)))
    }
}

impl ShortCircuit {
    /// Short-circuits useless nodes. Assumes the node has already been
    /// optimized by ConstantFolding.
    fn short_circuit(mut node: Node) -> Node {
        use Expression::*;
        use Value::*;

        node = match node {
            // Filter nodes that always yield true are unnecessary: remove them.
            Node::Filter { source, predicate: Constant(Boolean(true)) } => *source,

            // Predicates that always yield true are unnecessary: remove them.
            Node::Scan { table, filter: Some(Constant(Boolean(true))), alias } => {
                Node::Scan { table, filter: None, alias }
            }
            Node::NestedLoopJoin {
                left,
                right,
                predicate: Some(Constant(Boolean(true))),
                outer,
            } => Node::NestedLoopJoin { left, right, predicate: None, outer },

            // Remove noop projections that simply pass through the source columns.
            Node::Projection { source, expressions, aliases }
                if source.columns() == expressions.len()
                    && aliases.iter().all(|alias| *alias == Label::None)
                    && expressions
                        .iter()
                        .enumerate()
                        .all(|(i, expr)| *expr == Expression::Column(i)) =>
            {
                *source
            }

            node => node,
        };

        // Short-circuit nodes that don't produce anything by replacing them
        // with a Nothing node.
        let is_empty = match &node {
            Node::Filter { predicate: Constant(Boolean(false) | Null), .. } => true,
            Node::IndexLookup { values, .. } if values.is_empty() => true,
            Node::KeyLookup { keys, .. } if keys.is_empty() => true,
            Node::Limit { limit: 0, .. } => true,
            Node::NestedLoopJoin { predicate: Some(Constant(Boolean(false) | Null)), .. } => true,
            Node::Scan { filter: Some(Constant(Boolean(false) | Null)), .. } => true,
            Node::Values { rows } if rows.is_empty() => true,

            // Nodes that pull from a Nothing node can't produce anything.
            //
            // NB: does not short-circuit aggregation, since an aggregation over 0
            // rows should produce a result.
            Node::Filter { source, .. }
            | Node::HashJoin { left: source, .. }
            | Node::HashJoin { right: source, .. }
            | Node::NestedLoopJoin { left: source, .. }
            | Node::NestedLoopJoin { right: source, .. }
            | Node::Offset { source, .. }
            | Node::Order { source, .. }
            | Node::Projection { source, .. }
                if matches!(**source, Node::Nothing { .. }) =>
            {
                true
            }

            _ => false,
        };

        if is_empty {
            let columns = (0..node.columns()).map(|i| node.column_label(i)).collect();
            return Node::Nothing { columns };
        }

        node
    }
}
