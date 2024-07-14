use super::Node;
use crate::error::Result;
use crate::sql::types::{Expression, Value};

use std::collections::HashMap;

/// A plan optimizer, which takes a root node and recursively transforms it.
pub type Optimizer = fn(Node) -> Result<Node>;

/// The set of optimizers, and the order in which they are applied.
pub static OPTIMIZERS: &[(&str, Optimizer)] = &[
    ("Constant folding", fold_constants),
    ("Filter pushdown", push_filters),
    ("Index lookup", index_lookup),
    ("Join type", join_type),
    ("Short circuit", short_circuit),
];

/// Folds constant (sub)expressions by pre-evaluating them, instead of
/// re-evaluating then for every row during execution.
pub(super) fn fold_constants(node: Node) -> Result<Node> {
    use Expression::*;
    use Value::*;

    // Transforms expressions.
    let transform = |expr: Expression| {
        // If the expression is constant, evaulate it.
        if !expr.contains(&|e| matches!(e, Expression::Field(_, _))) {
            return expr.evaluate(None).map(Expression::Constant);
        }
        // If the expression is a logical operator, and one of the sides is
        // known, we may be able to short-circuit it.
        Ok(match expr {
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
        })
    };

    // Transform expressions after descending, both to perform the logical
    // short-circuiting on child expressions that have already been folded, and
    // to reduce the quadratic cost when an expression contains a field.
    node.transform(&|n| n.transform_expressions(&Ok, &transform), &Ok)
}

/// Pushes filter predicates down into child nodes where possible. In
/// particular, this can allow filtering during storage scans (below Raft).
pub(super) fn push_filters(node: Node) -> Result<Node> {
    /// Pushes an expression into a node if possible. Otherwise, returns the the
    /// unpushed expression.
    fn push_into(expr: Expression, target: &mut Node) -> Option<Expression> {
        match target {
            Node::Filter { predicate, .. } => {
                // Temporarily swap the predicate to take ownership.
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
            // We don't handle HashJoin here, since we assume the join_type()
            // optimizer runs after this.
            Node::HashJoin { .. } => panic!("filter pushdown must run before join optimizer"),
            // Unable to push down, just return the original expression.
            _ => return Some(expr),
        }
        None
    }

    /// Pushes down a filter node if possible.
    fn push_filter(node: Node) -> Node {
        let Node::Filter { mut source, predicate } = node else { return node };
        // Attempt to push the filter into the source.
        if let Some(predicate) = push_into(predicate, &mut source) {
            // Push failed, return the original filter node.
            return Node::Filter { source, predicate };
        }
        // Push succeded, return the source that was pushed into. When we
        // replace this filter node with the source node, Node.transform() will
        // skip the source node since it now takes the place of the original
        // filter node. Transform the source manually.
        transform(*source)
    }

    // Pushes down parts of a join predicate into the left or right sources
    // where possible.
    fn push_join(node: Node) -> Node {
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
            expr.walk(&mut |e| {
                if let Expression::Field(index, _) = e {
                    ref_left = ref_left || *index < left.size();
                    ref_right = ref_right || *index >= left.size();
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
        // SELECT m.name, g.name FROM movies m JOIN genres g ON m.genre_id = g.id AND g.id = 7;
        let left_lookups: HashMap<usize, usize> = push_left // field → push_left index
            .iter()
            .enumerate()
            .filter_map(|(i, expr)| expr.is_field_lookup().map(|field| (field, i)))
            .collect();
        let right_lookups: HashMap<usize, usize> = push_right // field → push_right index
            .iter()
            .enumerate()
            .filter_map(|(i, expr)| expr.is_field_lookup().map(|field| (field, i)))
            .collect();

        for expr in &predicate {
            // Find equijoins.
            let Expression::Equal(lhs, rhs) = expr else { continue };
            let Expression::Field(l, lname) = lhs.as_ref() else { continue };
            let Expression::Field(r, rname) = rhs.as_ref() else { continue };

            // The lhs may be a reference to the right source; swap them.
            let (l, lname, r, rname) =
                if l > r { (r, rname, l, lname) } else { (l, lname, r, rname) };

            // Check if either side is a field lookup, and copy it over.
            if let Some(expr) = left_lookups.get(l).map(|i| push_left[*i].clone()) {
                push_right.push(expr.replace_field(*l, *r, rname));
            }
            if let Some(expr) = right_lookups.get(r).map(|i| push_right[*i].clone()) {
                push_left.push(expr.replace_field(*r, *l, lname));
            }
        }

        // Push predicates down into the sources if possible.
        if let Some(expr) = Expression::and_vec(push_left) {
            if let Some(expr) = push_into(expr, &mut left) {
                // Pushdown failed, put it back into the join predicate.
                predicate.push(expr)
            }
        }

        if let Some(mut expr) = Expression::and_vec(push_right) {
            // Right fields have indexes in the joined row; shift them left.
            expr = expr.shift_field(-(left.size() as isize));
            if let Some(mut expr) = push_into(expr, &mut right) {
                // Pushdown failed, undo the field index shift.
                expr = expr.shift_field(left.size() as isize);
                predicate.push(expr)
            }
        }

        // Leave any remaining predicates in the join node.
        let predicate = Expression::and_vec(predicate);
        Node::NestedLoopJoin { left, right, predicate, outer }
    }

    /// Applies pushdown transformations to a node.
    fn transform(mut node: Node) -> Node {
        node = push_filter(node);
        node = push_join(node);
        node
    }

    // Push down before descending, so we can keep recursively pushing down.
    node.transform(&|n| Ok(transform(n)), &Ok)
}

/// Uses an index or primary key lookup for a filter when possible.
pub(super) fn index_lookup(node: Node) -> Result<Node> {
    let transform = |mut node| {
        // Only handle scan filters. filter_pushdown() must have pushed filters
        // into scan nodes first.
        let Node::Scan { table, alias, filter: Some(filter) } = node else { return node };

        // Convert the filter into conjunctive normal form (a list of ANDs).
        let mut cnf = filter.clone().into_cnf_vec();

        // Find the first expression that's either a primary key or secondary
        // index lookup. We could be more clever here, but this is fine.
        let Some(i) = cnf.iter().enumerate().find_map(|(i, e)| {
            e.is_field_lookup()
                .filter(|f| *f == table.primary_key || table.columns[*f].index)
                .and(Some(i))
        }) else {
            return Node::Scan { table, alias, filter: Some(filter) };
        };

        // Extract the lookup values and expression from the cnf vector.
        let (column, values) = cnf.remove(i).into_field_values().expect("field lookup failed");

        // Build the primary key or secondary index lookup node.
        if column == table.primary_key {
            node = Node::KeyLookup { table, keys: values, alias };
        } else {
            node = Node::IndexLookup { table, column, values, alias };
        }

        // If there's any remaining CNF expressions add a filter node for them.
        if let Some(predicate) = Expression::and_vec(cnf) {
            node = Node::Filter { source: Box::new(node), predicate };
        }

        node
    };
    node.transform(&Ok, &|n| Ok(transform(n)))
}

/// Uses a hash join instead of a nested loop join for single-field equijoins.
pub(super) fn join_type(node: Node) -> Result<Node> {
    let transform = |node| match node {
        // We could use a single match if we had deref patterns, but alas.
        Node::NestedLoopJoin {
            left,
            right,
            predicate: Some(Expression::Equal(lhs, rhs)),
            outer,
        } => match (*lhs, *rhs) {
            (
                Expression::Field(mut left_field, mut left_label),
                Expression::Field(mut right_field, mut right_label),
            ) => {
                // The LHS field may be a field in the right table; swap them.
                if right_field < left_field {
                    (left_field, right_field) = (right_field, left_field);
                    (left_label, right_label) = (right_label, left_label);
                }
                // The NestedLoopJoin predicate uses field indexes in the joined
                // row, while the HashJoin uses field indexes for each table
                // individually. Adjust the RHS field reference.
                right_field -= left.size();
                Node::HashJoin {
                    left,
                    left_field,
                    left_label,
                    right,
                    right_field,
                    right_label,
                    outer,
                }
            }
            (lhs, rhs) => {
                let predicate = Some(Expression::Equal(lhs.into(), rhs.into()));
                Node::NestedLoopJoin { left, right, predicate, outer }
            }
        },
        node => node,
    };
    node.transform(&|n| Ok(transform(n)), &Ok)
}

/// Short-circuits useless nodes and expressions, by removing them and/or
/// replacing them with Nothing nodes that yield no rows.
pub(super) fn short_circuit(node: Node) -> Result<Node> {
    use Expression::Constant;
    use Value::{Boolean, Null};

    let transform = |node| match node {
        // Filter nodes that always yield true are unnecessary: remove them.
        Node::Filter { source, predicate: Constant(Boolean(true)) } => *source,

        // Predicates that always yield true are unnecessary: remove them.
        Node::Scan { table, filter: Some(Constant(Boolean(true))), alias } => {
            Node::Scan { table, filter: None, alias }
        }
        Node::NestedLoopJoin { left, right, predicate: Some(Constant(Boolean(true))), outer } => {
            Node::NestedLoopJoin { left, right, predicate: None, outer }
        }

        // Short-circuit nodes that can't produce anything by replacing them
        // with a Nothing node.
        Node::Filter { predicate: Constant(Boolean(false) | Null), .. } => Node::Nothing,
        Node::IndexLookup { values, .. } if values.is_empty() => Node::Nothing,
        Node::KeyLookup { keys, .. } if keys.is_empty() => Node::Nothing,
        Node::Limit { limit: 0, .. } => Node::Nothing,
        Node::NestedLoopJoin { predicate: Some(Constant(Boolean(false) | Null)), .. } => {
            Node::Nothing
        }
        Node::Scan { filter: Some(Constant(Boolean(false) | Null)), .. } => Node::Nothing,
        Node::Values { rows, .. } if rows.is_empty() => Node::Nothing,

        // Short-circuit nodes that pull from a Nothing node.
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
            if *source == Node::Nothing =>
        {
            Node::Nothing
        }

        // Remove noop projections that simply pass through the source columns.
        Node::Projection { source, expressions, .. }
            if source.size() == expressions.len()
                && expressions
                    .iter()
                    .enumerate()
                    .all(|(i, e)| matches!(e, Expression::Field(f, _) if i == *f)) =>
        {
            *source
        }

        node => node,
    };

    // Transform after descending, to pull Nothing nodes upwards.
    node.transform(&Ok, &|n| Ok(transform(n)))
}
