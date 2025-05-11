# SQL Optimization

[Query optimization](https://en.wikipedia.org/wiki/Query_optimization) attempts to improve query
performance and efficiency by altering the execution plan. This is a deep and complex field, and
we can only scratch the surface here.

toyDB's query optimizer is very basic -- it only has a handful of rudimentary heuristic
optimizations to illustrate how the process works. Real-world optimizers use much more sophisticated
methods, including statistical analysis, cost estimation, adaptive execution, etc.

The optimizers are located in the [`sql::planner::optimizer`](https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs) module.
An optimizer `sql::planner::Optimizer` just takes in a plan node `sql::planner::Node` (the root node
in the plan), and returns an optimized node:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L20-L25

Optimizations are always implemented as recursive node transformations. To help with this, `Node`
has the helper methods `Node::transform` and `Node::transform_expressions` which recurse into a node
or expression tree and call a given transformation closure on each node, as either
[pre-order](https://en.wikipedia.org/wiki/Tree_traversal#Pre-order,_NLR) or
[post-order](https://en.wikipedia.org/wiki/Tree_traversal#Post-order,_LRN) transforms:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/plan.rs#L269-L371

A technique that's often useful during optimization is to convert expressions into
[conjunctive normal form](https://en.wikipedia.org/wiki/Conjunctive_normal_form), i.e. "an AND of
ORs". For example, the two following expressions are equivalent, but the latter is in conjunctive
normal form (it's a chain of ANDs):

```
(a AND b) OR (c AND d)  →  (a OR c) AND (a OR d) AND (b OR c) AND (b OR d)
```

This is useful because we can often move each AND operand independently around in the plan tree
and still get the same result -- we'll see this in action later. Expressions are converted into
conjunctive normal form via `Expression::into_cnf`, which is implemented using
[De Morgan's laws](https://en.wikipedia.org/wiki/De_Morgan%27s_laws):

https://github.com/erikgrinaker/toydb/blob/9419bcf6aededf0e20b4e7485e2a5fa3e975d79f/src/sql/types/expression.rs#L289-L351

We'll have a brief look at all of toyDB's optimizers, which are listed here in the order they're
applied:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L9-L18

Test scripts for the optimizers are in [`src/sql/testscripts/optimizers`](https://github.com/erikgrinaker/toydb/tree/9419bcf6aededf0e20b4e7485e2a5fa3e975d79f/src/sql/testscripts/optimizers),
and show how query plans evolve as each optimizer is applied.

## Constant Folding

The `ConstantFolding` optimizer performs [constant folding](https://en.wikipedia.org/wiki/Constant_folding).
This pre-evaluates constant expressions in the plan during planning, instead of evaluating them
for every row during execution.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L27-L30

For example, consider the query `SELECT 1 + 2 * 3 - foo FROM bar`. There is no point in
re-evaluating `1 + 2 * 3` for every row in `bar`, because the result is always the same, so we can
just evaluate this once during planning, transforming the expression into `7 - foo`.

Concretely, this plan:

```
Select
└─ Projection: 1 + 2 * 3 - bar.foo
   └─ Scan: bar
```

Should be transformed into this plan:

```
Select
└─ Projection: 7 - bar.foo
   └─ Scan: bar
```

To do this, `ConstantFolding` simply checks whether an `Expression` tree contains an
`Expression::Column` node -- if it doesn't, then it much be a constant expression (since that's the
only dynamic value in an expression), and we can evaluate it with a `None` input row and replace the
original expression node with an `Expression::Constant` node.

This is done recursively for each plan node, and recursively for each expression node (so it does
this both for `SELECT`, `WHERE`, `ORDER BY`, and all other parts of the query). Notably, it does a
post-order expression transform, so it starts at the expression leaf nodes and attempts to transform
each expression node as it moves back up the tree -- this allows it to iteratively evaluate constant
parts as far as possible for each branch.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L32-L56

Additionally, `ConstantFolding` also short-circuits logical expressions. For example, the expression
`foo AND FALSE` will always be `FALSE`, regardless of what `foo` is, so we can replace it with
`FALSE`:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L58-L84

As the code comment mentions though, this doesn't fold as far as possible. It doesn't attempt to
rearrange expressions, which would require knowledge of precedence rules. For example,
`(1 + foo) - 2` could be folded into `foo - 1` by first rearranging it as `foo + (1 - 2)`, but we
don't do this currently.

## Filter Pushdown

The `FilterPushdown` optimizer attempts to push filter predicates as far down into the plan as
possible, to reduce the amount of work we do.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L90-L95

Recall the `movies` query plan from the planning section:

```
Select
└─ Order: movies.released desc
   └─ Projection: movies.title, movies.released, genres.name as genre
      └─ Filter: movies.released >= 2000
         └─ NestedLoopJoin: inner on movies.genre_id = genres.id
            ├─ Scan: movies
            └─ Scan: genres
```

Even though we're filtering on `release >= 2000`, the `Scan` node still has to read all of them
from disk and send them via Raft, and the `NestedLoopJoin` node still has to join all of them.
It would be nice if we could push this filtering into into the `NestedLoopJoin` and `Scan` nodes
and avoid this work, which is exactly what `FilterPushdown` does.

The only plan nodes that have predicates that can be pushed down are `Filter` nodes and
`NestedLoopJoin` nodes, so we recurse through the plan tree and look for these nodes, attempting
to push down.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L97-L110

When it encounters the `Filter` node, it will extract the predicate and attempt to push it down
into its `source` node:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L139-L153

If the source node is a `Filter`, `NestedLoopJoin`, or `Scan` node, then we can push the predicate
down into it by `AND`ing it with the existing predicate (if any).

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L112-L137

In our case, we were able to push the `Filter` into the `NestedLoopJoin`, and our plan now looks
like this:

```
Select
└─ Order: movies.released desc
   └─ Projection: movies.title, movies.released, genres.name as genre
      └─ NestedLoopJoin: inner on movies.genre_id = genres.id AND movies.released >= 2000
         ├─ Scan: movies
         └─ Scan: genres
```

But we're still not done, as we'd like to push `movies.released >= 2000` down into the `Scan` node.
Pushdown for join nodes is a little more tricky, because we can only push down parts of the
expression that reference one of the source nodes.

We first have to convert the expression into conjunctive normal form, i.e. and AND of ORs, as we've
discussed previously. This allows us to examine and push down each AND part in isolation, because it
has the same effect regardless of whether it is evaluated in the `NestedLoopJoin` node or one of
the source nodes. Our expression is already in conjunctive normal form, though.

We then look at each AND part, and check which side of the join they have column references for.
If they only reference one of the sides, then the expression can be pushed down into it. We also
make some effort here to move primary/foreign key constants across to both sides, but we'll gloss
over that.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L155-L247

This allows us to push down the `movies.released >= 2000` predicate into the corresponding `Scan`
node, significantly reducing the amount of data transferred across Raft:

```
Select
└─ Order: movies.released desc
   └─ Projection: movies.title, movies.released, genres.name as genre
      └─ NestedLoopJoin: inner on movies.genre_id = genres.id
         ├─ Scan: movies (released >= 2000)
         └─ Scan: genres
```

## Index Lookups

The `IndexLookup` optimizer uses primary key or secondary index lookups instead of full table
scans where possible.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L250-L252

The optimizer itself is fairly straightforward. It assumes that `FilterPushdown` has already pushed
predicates down into `Scan` nodes, so it only needs to examine these. It converts the predicate into
conjunctive normal form, and looks for any parts that are direct column lookups -- i.e.
`column = value` (possibly a long OR chain of these).

If it finds any, and the column is either a primary key or secondary index column, then we convert
the `Scan` node into either a `KeyLookup` or `IndexLookup` node respectively. If there are any
further AND predicates remaining, we add a parent `Filter` node to keep these predicates.

For example, the following plan:

```
Select
└─ Scan: movies ((id = 1 OR id = 7 OR id = 3) AND released >= 2000)
```

Will be transformed into one that does individual key lookups rather than a full table scan:

```
Select
└─ Filter: movies.released >= 2000
   └─ KeyLookup: movies (1, 3, 7)
```

The code is as outlined above:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L254-L303

Helped by `Expression::is_column_lookup` and `Expression::into_column_values`:

https://github.com/erikgrinaker/toydb/blob/9419bcf6aededf0e20b4e7485e2a5fa3e975d79f/src/sql/types/expression.rs#L363-L421

## Hash Join

The `HashJoin` optimizer will replace a `NestedLoopJoin` with a `HashJoin` where possible.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L305-L307

A [nested loop join](https://en.wikipedia.org/wiki/Nested_loop_join) is a very inefficient O(n²)
algorithm, which iterates over all rows in the right source for each row in the left source to see
if they match. However, it is completely general, and can join on arbitraily complex predicates.

In the common case where the join predicate is an equality check (i.e. an
[equijoin](https://en.wikipedia.org/wiki/Relational_algebra#θ-join_and_equijoin)), such as
`movies.genre_id = genres.id`, then we can instead use a
[hash join](https://en.wikipedia.org/wiki/Hash_join). This scans the right table once, builds an
in-memory hash table from it, and for each left row it looks up any right rows in the hash table.
This is a much more efficient O(n) algorithm.

In our previous movie example, we are in fact doing an equijoin, and so our `NestedLoopJoin`:

```
Select
└─ Order: movies.released desc
   └─ Projection: movies.title, movies.released, genres.name as genre
      └─ NestedLoopJoin: inner on movies.genre_id = genres.id
         ├─ Scan: movies (released >= 2000)
         └─ Scan: genres
```

Will be replaced by a `HashJoin`:

```
Select
└─ Order: movies.released desc
   └─ Projection: movies.title, movies.released, genres.name as genre
      └─ HashJoin: inner on movies.genre_id = genres.id
         ├─ Scan: movies (released >= 2000)
         └─ Scan: genres
```

The `HashJoin` optimizer is extremely simple: if the join predicate is an equijoin, use a hash join.
This isn't always a good idea (the right source can be huge and we can run out of memory for the
hash table), but we keep it simple.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L309-L348

Of course there are many other join algorithms out there, and one of the harder problems in SQL
optimization is how to efficiently perform deep multijoins. We don't attempt to tackle these
problems here -- the `HashJoin` optimizer is just a very simple example of such join optimization.

## Short Circuiting

The `ShortCircuit` optimizer tries to find nodes that can't possibly do any useful work, and either
removes them from the plan, or replaces them with trivial nodes that don't do anything. It is kind
of similar to the `ConstantFolding` optimizer in spirit, but works at the plan node level rather
than the expression node level.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L350-L354

For example, `Filter` nodes with a `TRUE` predicate won't actually filter anything:

```
Select
└─ Filter: true
   └─ Scan: movies
```

So we can just remove them:

```
Select
└─ Scan: movies
```

Similarly, `Filter` nodes with a `FALSE` predicate will never emit anything:

```
Select
└─ Filter: false
   └─ Scan: movies
```

There's no point doing a scan in this case, so we can just replace it with a `Nothing` node that
does no work and doesn't emit anything:

```
Select
└─ Nothing
```

The optimizer tries to find a bunch of such patterns. This can also tidy up query plans a fair bit
by removing unnecessary cruft.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/optimizer.rs#L356-L438

---

<p align="center">
← <a href="sql-planner.md">SQL Planning</a> &nbsp; | &nbsp; <a href="sql-execution.md">SQL Execution</a> →
</p>