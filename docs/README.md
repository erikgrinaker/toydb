# ToyDB Documentation

## Data Types

* `BOOLEAN`: logical truth values, i.e. true and false.
* `FLOAT`: 64-bit signed floating point numbers, using [IEEE 754 `binary64`](https://en.wikipedia.org/wiki/binary64) encoding. Supports a magnitude of 10⁻³⁰⁷ to 10³⁰⁸, and 53-bit precision (exact up to 15 significant figures), as well as the special values infinity and NaN.
* `INTEGER`: 64-bit signed integer numbers with a range of -2⁶³.
* `VARCHAR`: UTF-8 encoded strings up to 1024 bytes.

In addition, the special `NULL` value is used for an unknown value of unknown data type, following the rules of [three-valued logic](https://en.wikipedia.org/wiki/Three-valued_logic).