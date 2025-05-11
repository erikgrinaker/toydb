# SQL Data Model

The SQL data model is toyDB's representation of user data. It is made up of data types and schemas.

## Data Types

toyDB supports four basic scalar data types as `sql::types::DataType`: booleans, floats, integers,
and strings.

https://github.com/erikgrinaker/toydb/blob/b2fe7b76ee634ca6ad31616becabfddb1c03d34b/src/sql/types/value.rs#L15-L27

Concrete values are represented as `sql::types::Value`, using corresponding Rust types. toyDB also
supports SQL `NULL` values, i.e. unknown values, following the rules of
[three-valued logic](https://en.wikipedia.org/wiki/Three-valued_logic).

https://github.com/erikgrinaker/toydb/blob/b2fe7b76ee634ca6ad31616becabfddb1c03d34b/src/sql/types/value.rs#L40-L64

The `Value` type provides basic formatting, conversion, and mathematical operations. It also
specifies comparison and ordering semantics, but these are subtly different from the SQL semantics.
For example, in Rust code `Value::Null == Value::Null` yields `true`, while in SQL `NULL = NULL`
yields `NULL`.  This mismatch is necessary for the Rust code to properly detect and process `Null`
values, and the desired SQL semantics are implemented higher up in the SQL execution engine (we'll
get back to this later).

https://github.com/erikgrinaker/toydb/blob/b2fe7b76ee634ca6ad31616becabfddb1c03d34b/src/sql/types/value.rs#L91-L162

During execution, a row of values will be represented as `sql::types::Row`, with multiple rows
emitted as `sql::types::Rows` row iterators:

https://github.com/erikgrinaker/toydb/blob/b2fe7b76ee634ca6ad31616becabfddb1c03d34b/src/sql/types/value.rs#L378-L388

## Schemas

toyDB schemas support a single object: a table. There's only a single, unnamed database, and no
named indexes, constraints, or other schema objects.

Tables are represented by `sql::types::Table`:

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/types/schema.rs#L12-L25

A table is made up of a set of columns, represented by `sql::types::Column`. These support the data
types described above, along with unique constraints, foreign keys, and secondary indexes.

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/types/schema.rs#L29-L53

The table name serves as a unique identifier, and can't be changed later. In fact, tables schemas
are entirely static: they can only be created or dropped (there are no schema changes).

Table schemas are stored in the catalog, represented by the `sql::engine::Catalog` trait. We'll
revisit the implementation of this trait in the storage section below.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/sql/engine/engine.rs#L60-L79

Table schemas are validated (e.g. during creation) via the `Table::validate()` method, which
enforces invariants and internal consistency. It uses the catalog to look up information about other
tables, e.g. that foreign key references point to a valid target column.

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/types/schema.rs#L98-L170

It also has a `Table::validate_row()` method which is used to validate that a given
`sql::types::Row` conforms to the schema (e.g. that the value data types match the column data
types). It uses a `sql::engine::Transaction` to look up other rows in the database, e.g. to check
for primary key conflicts (we'll get back to this below).

https://github.com/erikgrinaker/toydb/blob/c2b0f7f1d6cbf6e2cdc09fc0aec7b050e840ec21/src/sql/types/schema.rs#L172-L236

## Expressions

During SQL execution, we also have to model _expressions_, such as `1 + 2 * 3`. These are
represented as values and operations on them. They can be nested arbitrarily as a tree to represent
compound operations.

https://github.com/erikgrinaker/toydb/blob/9419bcf6aededf0e20b4e7485e2a5fa3e975d79f/src/sql/types/expression.rs#L11-L64


For example:

```rust
// 1 + 2 * 3 is represented as:
//
//             +
//            / \
//           1   *
//              /  \
///            2    3
Expression::Add(
    Expression::Constant(Value::Integer(1)),
    Expression::Multiply(
        Expression::Constant(Value::Integer(2)),
        Expression::Constant(Value::Integer(3)),
    ),
)
```

An `Expression` can contain two kinds of values: constant values as
`Expression::Constant(sql::types::Value)`, and dynamic values as `Expression::Column(usize)` column
references. The latter will fetch a `sql::types::Value` from a `sql::types::Row` at the specified
index during evaluation.

We'll see later how the SQL parser and planner transforms text expressions like `1 + 2 * 3` into
this `Expression` form, and how it resolves column names to row indexes -- e.g. `price * 0.25` to
`row[3] * 0.25`.

Expressions are evaluated recursively via `Expression::evalute()`, given a `sql::types::Row` with
input values for column references, and return a final `sql::types::Value` result:

https://github.com/erikgrinaker/toydb/blob/9419bcf6aededf0e20b4e7485e2a5fa3e975d79f/src/sql/types/expression.rs#L73-L208

Many of the comparison operations like `==` are implemented explicitly here instead of using
`sql::types::Value` comparisons. This is where we implement the SQL semantics of special values like
`NULL`, such that `NULL = NULL` yields `NULL` instead of `TRUE`.

For mathematical operations however, we generally dispatch to these methods on `sql::types::Value`:

https://github.com/erikgrinaker/toydb/blob/b2fe7b76ee634ca6ad31616becabfddb1c03d34b/src/sql/types/value.rs#L185-L295

Expression parsing and evaluation is tested via test scripts in
[`sql/testscripts/expression`](https://github.com/erikgrinaker/toydb/tree/9419bcf6aededf0e20b4e7485e2a5fa3e975d79f/src/sql/testscripts/expressions).

---

<p align="center">
← <a href="sql.md">SQL Engine</a> &nbsp; | &nbsp; <a href="sql-storage.md">SQL Storage</a> →
</p>