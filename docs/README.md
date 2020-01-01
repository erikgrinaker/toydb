# ToyDB Documentation

## Data Types

The following data types are supported, and can be used as column types:

* `BOOLEAN` (`BOOL`): logical truth values, i.e. true and false.
* `FLOAT` (`DOUBLE`): 64-bit signed floating point numbers, using [IEEE 754 `binary64`](https://en.wikipedia.org/wiki/binary64) encoding. Supports a magnitude of 10⁻³⁰⁷ to 10³⁰⁸, and 53-bit precision (exact up to 15 significant figures), as well as the special values infinity and NaN.
* `INTEGER` (`INT`): 64-bit signed integer numbers with a range of ±2⁶³-1.
* `STRING` (`CHAR`, `TEXT`, `VARCHAR`): UTF-8 encoded strings up to 1024 bytes.

In addition, the special `NULL` value is used for an unknown value of unknown data type, following the rules of [three-valued logic](https://en.wikipedia.org/wiki/Three-valued_logic).

Numeric types are not interchangable when stored; a float value cannot be stored in an integer column and vice-versa, even if the float value does not have a fractional part (e.g. `3.0`).

## SQL Syntax

### Keywords

Keywords are reserved words that have special meaning in SQL statements, such as `SELECT`. These cannot be used as identifiers, e.g. table names. Keywords are case-insensitive. The complete list of keywords is:

`AS`, `ASC`, `AND`, `BEGIN`, `BOOL`, `BOOLEAN`, `BY`, `CHAR`, `COMMIT`, `CREATE`, `DELETE`, `DESC`, `DOUBLE`, `DROP`, `FALSE`, `FLOAT`, `FROM`, `INFINITY`, `INSERT`, `INT`, `INTEGER`, `INTO`, `KEY`, `LIMIT`, `NAN`, `NOT`, `NULL`, `OF`, `OFFSET`, `ONLY`, `OR`, `ORDER`, `PRIMARY`, `READ`, `ROLLBACK`, `SELECT`, `SET`, `STRING`, `SYSTEM`, `TABLE`, `TEXT`, `TIME`, `TRANSACTION`, `TRUE`, `UPDATE`, `VALUES`, `VARCHAR`, `WHERE`, `WRITE`

### Identifiers

Identifiers are names for database objects, such as tables and columns. Identifiers must begin with any Unicode letter, and can contain any combination of Unicode letters, numbers, and underscores (`_`). Identifiers are automatically converted to lowercase, and have no length limit. Reserved keywords (see above) cannot be used as identifiers, and it is not possible to quote or escape identifiers.

### Constants

#### Named constants

The following case-insensitive keywords evaluate to constants:

* `FALSE`: the `BOOLEAN` false value.
* `INFINITY`: the IEEE 754 `binary64` floating-point value for infinity.
* `NAN`: the IEEE 754 `binary64` floating-point value for NaN (not a number).
* `NULL`: the unknown value.
* `TRUE`: the `BOOLEAN` true value.

#### String literals

String literals are surrounded by single quotes `'`, and may contain any valid UTF-8 character. Single quotes must be escaped by an additional single quote, i.e. `''`, no other escape sequences are supported. For example:

```
'A string with ''quotes'' and emojis 😀'
```

#### Numeric literals

Sequences of digits `0-9` are parsed as a 64-bit signed integer. Numbers with decimal points or in scientific notation are parsed as 64-bit floating point numbers. The following pattern is supported:

```
999[.[999]][e[+-]999]
```

The `-` prefix operator can be used to take negative numbers.

### Operators

#### Logical operators

Logical operators apply standard logic operations on boolean operands.

* `AND`: the logical conjunction, e.g. `TRUE AND TRUE` yields `TRUE`.
* `OR`: the logical disjunction, e.g. `TRUE OR FALSE` yields `TRUE`.
* `NOT`: the logical negation, e.g. `NOT TRUE` yields `FALSE`.

The complete truth tables are:

| `AND`       | `TRUE`  | `FALSE` | `NULL`  |
|-------------|---------|---------|---------|
| **`TRUE`**  | `TRUE`  | `FALSE` | `NULL`  |
| **`FALSE`** | `FALSE` | `FALSE` | `FALSE` |
| **`NULL`**  | `NULL`  | `FALSE` | `NULL`  |

| `OR`        | `TRUE` | `FALSE` | `NULL` |
|-------------|--------|---------|--------|
| **`TRUE`**  | `TRUE` | `TRUE`  | `TRUE` |
| **`FALSE`** | `TRUE` | `FALSE` | `NULL` |
| **`NULL`**  | `TRUE` | `NULL`  | `NULL` |

| `NOT`       |         |
|-------------|---------|
| **`TRUE`**  | `FALSE` |
| **`FALSE`** | `TRUE`  |
| **`NULL`**  | `NULL`  |
