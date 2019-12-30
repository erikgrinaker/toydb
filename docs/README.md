# ToyDB Documentation

## Data Types

The following data types are supported, and can be used as column types:

* `BOOLEAN` (`BOOL`): logical truth values, i.e. true and false.
* `FLOAT` (`DOUBLE`): 64-bit signed floating point numbers, using [IEEE 754 `binary64`](https://en.wikipedia.org/wiki/binary64) encoding. Supports a magnitude of 10⁻³⁰⁷ to 10³⁰⁸, and 53-bit precision (exact up to 15 significant figures), as well as the special values infinity and NaN.
* `INTEGER` (`INT`): 64-bit signed integer numbers with a range of -2⁶³.
* `STRING` (`CHAR`, `TEXT`, `VARCHAR`): UTF-8 encoded strings up to 1024 bytes.

In addition, the special `NULL` value is used for an unknown value of unknown data type, following the rules of [three-valued logic](https://en.wikipedia.org/wiki/Three-valued_logic).

## SQL Syntax

### Keywords

Keywords are reserved words that have special meaning in SQL statements, such as `SELECT`. These cannot be used as identifiers, e.g. table names. Keywords are case-insensitive. The complete list of keywords is:

`AS`, `ASC`, `AND`, `BEGIN`, `BOOL`, `BOOLEAN`, `BY`, `CHAR`, `COMMIT`, `CREATE`, `DELETE`, `DESC`, `DOUBLE`, `DROP`, `FALSE`, `FLOAT`, `FROM`, `INSERT`, `INT`, `INTEGER`, `INTO`, `KEY`, `LIMIT`, `NOT`, `NULL`, `OF`, `OFFSET`, `ONLY`, `OR`, `ORDER`, `PRIMARY`, `READ`, `ROLLBACK`, `SELECT`, `SET`, `STRING`, `SYSTEM`, `TABLE`, `TEXT`, `TIME`, `TRANSACTION`, `TRUE`, `UPDATE`, `VALUES`, `VARCHAR`, `WHERE`, `WRITE`

### Identifiers

Identifiers are names for database objects, such as tables and columns. Identifiers must begin with any Unicode letter, and can contain any combination of Unicode letters, numbers, and underscores (`_`). Identifiers are automatically converted to lowercase, and have no length limit. Reserved keywords (see above) cannot be used as identifiers, and it is not possible to quote or escape identifiers.
