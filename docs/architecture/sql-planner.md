# SQL Planning

The SQL planner in [`sql/planner`](https://github.com/erikgrinaker/toydb/tree/c64012e29c5712d6fe028d3d5375a98b8faea266/src/sql/planner)
takes a SQL statement AST from the parser and generates an execution plan for it. We won't actually
execute it just yet though, only figure out how to execute it.

## Execution Plan

A plan is represented by the `sql::planner::Plan` enum. The variant specifies the operation to
execute (e.g. `SELECT`, `INSERT`, `UPDATE`, `DELETE`):

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/plan.rs#L15-L73

Below the root, the plan is typically made of up of a tree of nested `sql::planner::Node`. Each node
emits a stream of SQL rows as output, and may take streams of input rows from child nodes.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/sql/planner/plan.rs#L106-L175

Here is an example (taken from the `Plan` code comment above):

```sql
SELECT title, released, genres.name AS genre
FROM movies INNER JOIN genres ON movies.genre_id = genres.id
WHERE released >= 2000
ORDER BY released
```

Which results in this query plan:

```
Select
└─ Order: movies.released desc
   └─ Projection: movies.title, movies.released, genres.name as genre
      └─ Filter: movies.released >= 2000
         └─ NestedLoopJoin: inner on movies.genre_id = genres.id
            ├─ Scan: movies
            └─ Scan: genres
```

Rows flow from the tree leaves to the root:

1. `Scan` nodes read rows from the tables `movies` and `genres`.
2. `NestedLoopJoin` joins the rows from `movies` and `genres`.
3. `Filter` discards rows with release dates older than 2000.
4. `Projection` picks out the requested column values from the rows.
5. `Order` sorts the rows by release date.
6. `Select` returns the final rows to the client.

## Scope and Name Resolution

One of the main jobs of the planner is to resolve column names to column indexes in the input rows
of each node.

In the query example above, the `WHERE released >= 2000` filter may refer to a column `released`
from either the joined `movies` table or the `genres` tables. The planner needs to figure out which
table has a `released` column, and also figure out which column number in the `NestedLoopJoin`
output rows corresponds to the `released` column (for example column number 2).

This job is further complicated by the fact that many nodes can alias, reorder, or drop columns,
and some nodes may also refer to columns that shouldn't be part of the result at all (for example,
it's possible to `ORDER BY` a column that won't be output by a `SELECT` projection at all, but
the `Order` node still needs access to the column data to sort by it).

The planner uses a `sql::planner::Scope` to keep track of which column names are currently visible,
and which column indexes they refer to. For each node the planner builds, starting from the leaves,
it creates a new `Scope` that tracks how columns are modified and rearranged by the node.

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L577-L610

When an expression refers to a column name, the planner can use `Scope::lookup_column` to find out
which column number the expression should take its input value from.

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L660-L686

## Planner

The planner itself is `sql:planner::Planner`. It uses a `sql::engine::Catalog` to look up
information about tables and columns from storage.

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L12-L20

To build an execution plan, the planner first looks at the `ast::Statement` kind to determine
what kind of plan to build:

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L28-L47

Let's build this `SELECT` plan from above:

```sql
SELECT title, released, genres.name AS genre
FROM movies INNER JOIN genres ON movies.genre_id = genres.id
WHERE released >= 2000
ORDER BY released
```

Which should result in this plan:

```
Select
└─ Order: movies.released desc
   └─ Projection: movies.title, movies.released, genres.name as genre
      └─ Filter: movies.released >= 2000
         └─ NestedLoopJoin: inner on movies.genre_id = genres.id
            ├─ Scan: movies
            └─ Scan: genres
```

The planner is given the following (simplified) AST from the parser as input:

```rust
// A SELECT statement.
Statement::Select {
    // SELECT title, released, genres.name AS genre
    select: [
        (Column("title"), None),
        (Column("released"), None),
        (Column("genres.name"), "genre"),
    ]

    // FROM movies INNER JOIN genres ON movies.genre_id = genres.id
    from: [
        Join {
            left: Table("movies"),
            right: Table("genres"),
            type: Inner,
            predicate: Some(
                Equal(
                    Column("movies.genre_id"),
                    Column("genres.id"),
                )
            )
        }
    ]

    // WHERE released >= 2000
    where: Some(
        GreaterThanOrEqual(
            Column("released"),
            Integer(2000),
        )
    )

    // ORDER BY released
    order: [
        (Column("released"), Ascending),
    ]
}
```

The first thing `Planner::build_select` does is to create an empty scope (which will track column
names and indexes) and build the `FROM` clause which will generate the initial input rows:

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L170-L179

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L283-L289

`Planner::build_from` first encounters the `ast::From::Join` item, which joins `movies` and
`genres`. This will build a `Node::NestedLoopJoin` plan node for the join, which is the simplest and
most straightforward join algorithm -- it simply iterates over all rows in the `genres` table for
every row in the `movies` table and emits the joined rows (we'll see how to optimize it with a
better join algorithm later).

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L319-L344

It first recurses into `Planner::build_from` to build each of the `ast::From::Table` nodes for each
table.  This will look up the table schemas in the catalog, add them to the current scope, and build
a `Node::Scan` node which will emit all rows from each table. The `Node::Scan` nodes are placed into
the `Node::NestedLoopJoin` above.

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L312-L317

While building the `Node::NestedLoopJoin`, it also needs to convert the join expression
`movies.genre_id = genres.id` into a proper `sql::types::Expression`. This is done by
`Planner::build_expression`:

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L493-L568

Expression building is mostly a direct translation from an `ast::Expression` variant to a
corresponding `sql::types::Expression` variant (for example from
`ast::Expression::Operator(ast::Operator::Equal)` to `sql::types::Expression::Equal`). However, as
mentioned earlier, `ast::Expression` contains column references by name, while
`sql::types::Expression` contains column references as row indexes. This name resolution is done
here, by looking up the column names in the scope:

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L521-L523

The expression we're building is the join predicate of `Node::NestedLoopJoin`, so it operates on
joined rows containing all columns of `movies` then all columns of `genres`. It also operates on all
combinations of joined rows (the [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product)),
and the purpose of the join predicate is to determine which joined rows to actually keep. For
example, the full set of joined rows that are evaluated might be:

| movies.id | movies.title | movies.released | movies.genre_id | genres.id | genres.name |
|-----------|--------------|-----------------|-----------------|-----------|-------------|
| 1         | Sicario      | 2015            | 2               | 1         | Drama       |
| 2         | Sicario      | 2015            | 2               | 2         | Action      |
| 3         | 21 Grams     | 2003            | 1               | 1         | Drama       |
| 4         | 21 Grams     | 2003            | 1               | 2         | Action      |
| 5         | Heat         | 1995            | 2               | 1         | Drama       |
| 6         | Heat         | 1995            | 2               | 2         | Action      |

The join predicate should pick out the rows where `movies.genre_id = genres.id`. The scope will
reflect the column layout in the example above, and can resolve the column names to zero-based row
indexes as `#3 = #4`, which will be the final built `Expression`.

Now that we've built the `FROM` clause into a `Node::NestedLoopJoin` of two `Node::Scan` nodes, we
move on to the `WHERE` clause. This simply builds the `WHERE` expression `released >= 2000`, like
we've already seen with the join predicate, and creates a `Node::Filter` node which takes its input
rows from the `Node::NestedLoopJoin` and filters them by the given expression. Again, the scope
keeps track of which input columns we're getting from the join node and resolves the `released`
column reference in the expression.

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L202-L206

We then build the `SELECT` clause, which emits the `title, released, genres.name AS genre` columns.
This is just a list of expressions that are built in the current scope and placed into a
`Node::Projection` (the expressions could be arbitrarily complex). However, we also have to make
sure to update the scope with the final three columns that are output to subsequent nodes, taking
into account the `genre` alias for the original `genres.name` column (we won't dwell on the "hidden
columns" mentioned there -- they're not relevant for our query).

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L214-L234

Finally, we build the `ORDER BY` clause. Again, this just builds a trivial expression for `released`
and places it into an `Node::Order` node which takes input rows from the `Node::Projection` and
sorts them by the order expression.

https://github.com/erikgrinaker/toydb/blob/6f6cec4db10bc015a37ee47ff6c7dae383147dd5/src/sql/planner/planner.rs#L245-L252

And that's it. The `Node::Order` is placed into the root `Plan::Select`, and we have our final plan.
We'll see how to execute it soon, but first we should optimize it to see if we can make it run
faster -- in particular, to see if we can avoid reading all movies from storage, and if we can do
better than the very slow nested loop join.

```
Select
└─ Order: movies.released desc
   └─ Projection: movies.title, movies.released, genres.name as genre
      └─ Filter: movies.released >= 2000
         └─ NestedLoopJoin: inner on movies.genre_id = genres.id
            ├─ Scan: movies
            └─ Scan: genres
```

---

<p align="center">
← <a href="sql-parser.md">SQL Parsing</a> &nbsp; | &nbsp; <a href="sql-optimizer.md">SQL Optimization</a> →
</p>