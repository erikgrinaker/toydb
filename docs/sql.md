# SQL Reference

## Data Types

The following data types are supported:

* `BOOLEAN` (`BOOL`): logical truth values, i.e. true and false.
* `FLOAT` (`DOUBLE`): 64-bit signed floating point numbers, using [IEEE 754 `binary64`](https://en.wikipedia.org/wiki/binary64) encoding. Supports magnitudes of 10⁻³⁰⁷ to 10³⁰⁸ with 53-bit precision (~15 significant figures), as well as the special values infinity and NaN.
* `INTEGER` (`INT`): 64-bit signed integer numbers with a range of ±2⁶³-1.
* `STRING` (`CHAR`, `TEXT`, `VARCHAR`): UTF-8 encoded strings up to 1024 bytes.

In addition, the special `NULL` value is used for an unknown value, following the rules of [three-valued logic](https://en.wikipedia.org/wiki/Three-valued_logic).

Numeric types are not interchangable; a float value (even without a fractional part) cannot be stored in an integer column and vice-versa.

## SQL Syntax

### Keywords

Keywords are reserved words with special meaning in SQL statements. They are case-insensitive, and must be quoted with `"` to be used as identifiers. The complete list is:

`AS`, `ASC`, `AND`, `BEGIN`, `BOOL`, `BOOLEAN`, `BY`, `CHAR`, `COMMIT`, `CREATE`, `CROSS`, `DEFAULT`,`DELETE`, `DESC`, `DOUBLE`, `DROP`, `EXPLAIN`, `FALSE`, `FLOAT`, `FROM`, `GROUP`, `HAVING`, `INDEX`, `INFINITY`, `INNER`, `INSERT`, `INT`, `INTEGER`, `INTO`, `IS`, `JOIN`, `KEY`, `LEFT`, `LIKE`, `LIMIT`, `NAN`, `NOT`, `NULL`, `OF`, `OFFSET`, `ON`, `ONLY`, `OR`, `ORDER`, `OUTER`, `PRIMARY`, `READ`, `REFERENCES`, `RIGHT`, `ROLLBACK`, `SELECT`, `SET`, `STRING`, `SYSTEM`, `TABLE`, `TEXT`, `TIME`, `TRANSACTION`, `TRUE`, `UNIQUE`, `UPDATE`, `VALUES`, `VARCHAR`, `WHERE`, `WRITE`

### Identifiers

Identifiers are names for database objects such as tables and columns. Unless quoted with `"`, they must begin with a Unicode letter followed by any combination of letters, numbers, and `_`, and cannot be reserved keywords. `""` can be used to escape a double quote character. They are always converted to lowercase.

### Constants

#### Named constants

The following keywords evaluate to constants:

* `FALSE`: the boolean false value.
* `INFINITY`: the floating-point value for infinity.
* `NAN`: the floating-point value for NaN (not a number).
* `NULL`: an unknown value.
* `TRUE`: the boolean true value.

#### String literals

String literals are surrounded by single quotes `'`, and can contain any valid UTF-8 character. Single quotes must be escaped by an additional single quote, i.e. `''`, no other escape sequences are supported. For example:

```
'A string with ''quotes'' and emojis 😀'
```

#### Numeric literals

Sequences of digits `0-9` are parsed as a 64-bit signed integer. Numbers with decimal points or in scientific notation are parsed as 64-bit floating point numbers. The following pattern is supported:

```
999[.[999]][e[+-]999]
```

The `-` prefix operator can be used to take negative numbers.

### Expressions

Expressions can be used wherever a value is expected, e.g. as `SELECT` fields and `INSERT` values. They are made up of constants, a column references, an operator invocations, and a function calls.

Column references can either be unqualified, e.g. `name`, or prefixed with the relation identifier separated by `.`, e.g. `person.name`. Unqualified identifiers must be unambiguous.

## SQL Operators

### Logical operators

Logical operators apply standard logic operations on boolean operands.

* `AND`: the logical conjunction, e.g. `TRUE AND TRUE` yields `TRUE`.
* `OR`: the logical disjunction, e.g. `TRUE OR FALSE` yields `TRUE`.
* `NOT`: the logical negation, e.g. `NOT TRUE` yields `FALSE`.

The complete truth tables are:

| `AND`       | `TRUE`  | `FALSE` | `NULL`  |
| ----------- | ------- | ------- | ------- |
| **`TRUE`**  | `TRUE`  | `FALSE` | `NULL`  |
| **`FALSE`** | `FALSE` | `FALSE` | `FALSE` |
| **`NULL`**  | `NULL`  | `FALSE` | `NULL`  |

| `OR`        | `TRUE` | `FALSE` | `NULL` |
| ----------- | ------ | ------- | ------ |
| **`TRUE`**  | `TRUE` | `TRUE`  | `TRUE` |
| **`FALSE`** | `TRUE` | `FALSE` | `NULL` |
| **`NULL`**  | `TRUE` | `NULL`  | `NULL` |

| `NOT`       |         |
| ----------- | ------- |
| **`TRUE`**  | `FALSE` |
| **`FALSE`** | `TRUE`  |
| **`NULL`**  | `NULL`  |

### Comparison operators

Comparison operators compare values of the same data type, and return `TRUE` if the comparison holds or `FALSE` otherwise. `INTEGER` and `FLOAT` values are interchangeable. `STRING` comparisons use the string's byte values, i.e. case-sensitive with `'B' < 'a'` due to their UTF-8 code points. `FALSE` is considered lesser than `TRUE`. Comparison with `NULL` always yields `NULL` (even `NULL = NULL`).

Binary operators:

* `=`: equality, e.g. `1 = 1` yields `TRUE`.
* `!=`: inequality, e.g. `1 != 2` yields `TRUE`.
* `>`: greater than, e.g. `2 > 1` yields `TRUE`.
* `>=`: greater than or equal, e.g. `1 >= 1` yields `TRUE`.
* `<`: lesser than, e.g. `1 < 2` yields `TRUE`.
* `<=`: lesser than or equal, e.g. `1 <= 1` yields `TRUE`.

Unary operators:

* `IS NULL`: checks if the value is `NULL`, e.g. `NULL IS NULL` yields `TRUE`.
* `IS NOT NULL`: checks if the value is not `NULL`, e.g. `TRUE IS NOT NULL` yields `TRUE`.

### Mathematical operators

Mathematical operators apply standard math operations on numeric (`INTEGER` or `FLOAT`) operands. If either operand is a `FLOAT`, both operands are converted to `FLOAT` and the result is a `FLOAT`. If either operand is `NULL`, the result is `NULL`. The special values `INFINITY` and `NAN` are handled according to the IEEE 754 spec.

For `INTEGER` operands, failure conditions such as overflow and division by zero yield an error. For `FLOAT` operands, these return `INFINITY` or `NAN` as appropriate.

Binary operators:

* `+`: addition, e.g. `1 + 2` yields `3`.
* `-`: subtraction, e.g. `3 - 2` yields `1`.
* `*`: multiplication, e.g. `3 * 2` yields `6`.
* `/`: division, e.g. `6 / 2` yields `3`.
* `^`: exponentiation, e.g. `2 ^ 4` yields `16`.
* `%`: modulo or remainder, e.g. `8 % 3` yields `2`. The result has the sign of the divisor.

Unary operators:

* `+` (prefix): identity, e.g. `+1` yields `1`.
* `-` (prefix): negation, e.g. `- -2` yields `2`.
* `!` (postfix): factorial, e.g. `5!` yields `15`.

### String operators

String operators operate on string operands.

* `LIKE`: compares a string with the given pattern, using `%` as multi-character wildcard and `_` as single-character wildcard, returning `TRUE` if the string matches the pattern - e.g. `'abc' LIKE 'a%'` yields `TRUE`.  Literal `%` and `_` can be escaped as `%%` and `__`.

### Operator precedence

The operator precedence (order of operations) is as follows:

| Precedence | Operator                 | Associativity |
| ---------- | ------------------------ | ------------- |
| 9          | `+`, `-`, `NOT` (prefix) | Right         |
| 8          | `!`, `IS` (postfix)      | Left          |
| 7          | `^`                      | Right         |
| 6          | `*`, `/`, `%`            | Left          |
| 5          | `+`, `-`                 | Left          |
| 4          | `>`, `>=`, `<`, `<=`     | Left          |
| 3          | `=`, `!=`, `LIKE`        | Left          |
| 2          | `AND`                    | Left          |
| 1          | `OR`                     | Left          |

Precedence can be overridden by wrapping an expression in parentheses, e.g. `(1 + 2) * 3`.

### Aggregate functions

Aggregate function aggregate an expression across all rows, optionally grouped into buckets given by `GROUP BY`, and results can be filtered via `HAVING`.

* `AVG(expr)`: returns the average of numerical values.

* `COUNT(expr)`: returns the number of rows for which ***`expr`*** evaluates to a non-`NULL` value. `COUNT(*)` can be used to count all rows.

* `MAX(expr)`: returns the maximum value, according to the datatype's ordering.

* `MIN(expr)`: returns the minimum value, according to the datatype's ordering.

* `SUM(expr)`: returns the sum of numerical values.

## SQL Statements

### `BEGIN`

Starts a new [transaction](#transactions).

<pre>
BEGIN [ TRANSACTION ] [ READ ONLY | READ WRITE ] [ AS OF SYSTEM TIME <b><i>txn_id</i></b> ]
</pre>

* ***`txn_id`***: A past transaction ID to run a read-only transaction for, for time-travel queries.

### `COMMIT`

Commits an active [transaction](#transactions).

### `CREATE TABLE`

Creates a new table.

<pre>
CREATE TABLE <b><i>table_name</i></b> (
    [ <b><i>column_name</i></b> <b><i>data_type</i></b> [ <b><i>column_constraint</i></b> [ ... ] ]  [ INDEX ] [, ... ] ]
)

where <b><i>column_constraint</i></b> is:

{ NOT NULL | NULL | PRIMARY KEY | DEFAULT <b><i>expr</i></b> | REFERENCES <b><i>ref_table</i></b> | UNIQUE }
</pre>

* ***`table_name`***: The name of the table. Must be a [valid identifier](#identifiers). Errors if a table with this name already exists.

* ***`column_name`***: The name of the column. Must be a [valid identifier](#identifiers), and unique within the table.

* ***`data_type`***: The data type of the column, see [data types](#data-types) for valid types.

* `NOT NULL`: The column may not contain `NULL` values.

* `NULL`: The column may contain `NULL` values. This is the default.

* `PRIMARY KEY`: The column should act as a primary key, i.e. the main row identifier. A table must have exactly one primary key column, and it must be unique and non-nullable.

* `DEFAULT`***`expr`***: Specifies a default value for the column when `INSERT` statements do not give a value. ***`expr`*** can be any constant expression of an appropriate data type, e.g. `'abc'` or `1 + 2 * 3`. For nullable columns, the default value is `NULL` unless specified otherwise.

* `REFERENCES`***`ref_table`***: The column is a foreign key to ***`ref_table`***'s primary key, enforcing referential integrity.

* `UNIQUE`: The column may only contain unique (distinct) values. `NULL` values are not considered equal, thus a `UNIQUE` column which allows `NULL` may contain multiple `NULL` values. `PRIMARY KEY` columns are implicitly `UNIQUE`.

* `INDEX`: Create an index for the column.

#### Example

```sql
CREATE TABLE movie (
    id INTEGER PRIMARY KEY,
    title STRING NOT NULL,
    release_year INTEGER INDEX,
    imdb_id STRING INDEX UNIQUE,
    bluray BOOLEAN NOT NULL DEFAULT TRUE
)
```

### `DELETE`

Deletes rows in a table.

<pre>
DELETE FROM <b><i>table_name</i></b>
    [ WHERE <b><i>predicate</i></b> ]
</pre>

Deletes rows where ***`predicate`*** evaluates to `TRUE`, or all rows if no `WHERE` clause is given.

* ***`table_name`***: the table to delete from. Errors if it does not exist.

* ***`predicate`***: an expression which determines which rows to delete by evaluting to `TRUE`. Must evaluate to a `BOOLEAN` or `NULL`, otherwise an error is returned.

#### Example

```sql
DELETE FROM movie
WHERE release_year < 2000 AND bluray = FALSE
```

### `DROP TABLE`

Deletes a table and all contained data.

<pre>
DROP TABLE <b><i>table_name</i></b>
</pre>

* ***`table_name`***: the table to delete. Errors if it does not exist.

### `EXPLAIN`

Outputs the execution plan for the given statement.

<pre>
EXPLAIN [ <b><i>statement</i></b> ]
</pre>

### `INSERT`

Inserts rows into a table.

<pre>
INSERT INTO <b><i>table_name</i></b>
    [ ( <b><i>column_name</i></b> [, ... ] ) ]
    VALUES ( <b><i>expression</i></b> [, ... ] ) [, ... ]
</pre>

If column names are given, an identical number of values must be given. If no column names are given, values must be given in the table's column order. Omitted columns will get a default value if specified, otherwise an error will be returned.

* ***`table_name`***: the table to insert into. Errors if it does not exist.

* ***`column_name`***: a column to insert into in the given table. Errors if it does not exist.

* ***`expression`***: an expression to insert into the corresponding column. Must be a constant expression, i.e. it cannot refer to table fields.

#### Example

```sql
INSERT INTO movie
    (id, title, release_year)
VALUES
    (1, 'Sicario', 2015),
    (2, 'Stalker', 1979),
    (3, 'Her', 2013)
```

### `ROLLBACK`

Rolls back an active [transaction](#transactions).

### `SELECT`

Selects rows from a table.

<pre>
SELECT [ * | <b><i>expression</i></b> [ [ AS ] <b><i>output_name</i></b> [, ...] ] ]
    [ FROM <b><i>from_item</i></b> [, ...] ]
    [ WHERE <b><i>predicate</i></b> ]
    [ GROUP BY <b><i>group_expr</i></b> [, ...] ]
    [ HAVING <b><i>having_expr</i></b> ]
    [ ORDER BY <b><i>order_expr</i></b> [ ASC | DESC ] [, ...] ]
    [ LIMIT <b><i>count</i></b> ]
    [ OFFSET <b><i>start</i></b> ]

where <b><i>from_item</i></b> is one of:

<b><i>table_name</i></b> [ [ AS ] <b><i>alias</i></b> ]
<b><i>from_item</i></b> <b><i>join_type</i></b> <b><i>from_item</i></b> [ ON <b><i>join_predicate</i></b> ]

where <b><i>join_type</i></b> is one of:

CROSS JOIN
[ INNER ] JOIN
LEFT [ OUTER ] JOIN
RIGHT [ OUTER ] JOIN

</pre>

Fetches rows or expressions, either from table ***`table_name`*** (if given) or generated.

* ***`expression`***: [expression](#expressions) to fetch (can be a simple field name).

* ***`output_name`***: output column [identifier](#identifier), defaults to field name (if single field) otherwise nothing (displayed as `?`).

* ***`table_name`***: table to fetch rows from.

* ***`alias`***: table alias.

* ***`predicate`***: only return rows for which this [expression](#expressions) evaluates to `TRUE`.

* ***`group_expr`***: an expression to group aggregates by. Non-aggregate `SELECT` expressions must either reference a field given in `group_expr`, be idential with a `group_expr`, or have an `output_name` that is referenced by a `group_expr` field.

* ***`having_expr`***: only return aggregate results for which this [expression](#expressions) evaluates to `TRUE`.

* ***`order_expr`***: order rows by this expression (can be a simple field name).

* ***`count`***: maximum number of rows to return. Must be a constant integer expression.

* ***`start`***: number of rows to skip. Must be a constant integer expression.

* ***`join_predicate`***: only return rows for which this [expression](#expressions) evaluates to `TRUE`.

Join types:

* `CROSS JOIN`: returns the Carthesian product of the joined tables. Does not accept a join predicate (`ON` clause).

* `INNER JOIN`: returns the rows of the tables' Carthesian product for which  ***`join_predicate`*** evaluates to `TRUE`.

* `LEFT OUTER JOIN`: returns the rows joined on the ***`join_predicate`***, or for any rows in the left table that does not have a match in the right table a single row is returned with the right table's columns set to `NULL`.

* `RIGHT OUTER JOIN`: the same as a `LEFT OUTER JOIN` but with the left and right tables switched.

#### Example

```sql
SELECT id, title, 2020 - released AS age
FROM movies
WHERE released >= 2000 AND ultrahd
ORDER BY released DESC, title ASC
LIMIT 10
OFFSET 10
```

### `UPDATE`

Updates rows in a table.

<pre>
UPDATE <b><i>table_name</i></b>
    SET <b><i>column_name</i></b> = <b><i>expression</i></b> [, ... ]
    [ WHERE <b><i>predicate</i></b> ]
</pre>

Updates columns given by ***`column_name`*** to the corresponding ***`expression`*** for all rows where ***`predicate`*** evaluates to `TRUE`. If no `WHERE` clause is given, all rows are updated.

* ***`table_name`***: the table to update. Errors if it does not exist.

* ***`column_name`***: a column to update. Errors if it does not exist.

* ***`expression`***: an expression whose evaluated value will be set for the corresponding column and row. Expressions can refer to column values, and must evaluate to the same datatype as the updated column.

* ***`predicate`***: an expression which determines which rows to update by evaluting to `TRUE`. Must evaluate to a `BOOLEAN` or `NULL`, otherwise an error is returned.

#### Example

```sql
UPDATE movie
SET bluray = TRUE
WHERE release_year >= 2000 AND bluray = FALSE
```

## Transactions

toyDB supports ACID transactions using MVCC-based snapshot isolation, protecting from the following anomalies: dirty writes, dirty reads, lost updates, fuzzy reads, read skew, and phantom reads. However, write skew anomalies are possible since serializable snapshot isolation is not implemented.

A new transaction is started with `BEGIN`, and ended with either `COMMIT` (atomically writing all changes) or `ROLLBACK` (discarding all changes). If any conflicts occur between concurrent transactions, the lowest transaction ID wins and the others will fail with a serialization error and must retry.

All past data is versioned and retained, and can be queried as of a given transaction ID via `BEGIN TRANSACTION READ ONLY AS OF SYSTEM TIME <txn_id>`.

A transaction is still valid for use if a contained statement returns an error. It is up to the client to take appropriate action.
