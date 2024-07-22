# Tests the > greater than operator.

# Booleans.
> TRUE > FALSE
> FALSE > TRUE
> TRUE > TRUE
> FALSE > FALSE
---
TRUE
FALSE
FALSE
FALSE

# Integers.
> 3 > 2
> 3 > 3
> 3 > 4
> -1 > 0
> 0 > -1
---
TRUE
FALSE
FALSE
FALSE
TRUE

# Floats.
> 3.14 > 3.13
> 3.14 > 3.14
> 3.14 > 3.15
> 0.0 > -0.0
---
TRUE
FALSE
FALSE
FALSE

# Float special values.
> INFINITY > 1e300
> INFINITY > INFINITY
> INFINITY > -INFINITY
> NAN > NAN
> NAN > INFINITY
> INFINITY > NAN
> NAN > 0.0
---
TRUE
FALSE
TRUE
FALSE
FALSE
FALSE
FALSE

# Mixed integer/float values.
> 3 > 3.0
> 3 > 2.9
> 3 > 3.1
> 0 > -0.0
---
FALSE
TRUE
FALSE
FALSE

# Strings.
> 'abc' > 'abc'
> 'abc' > 'abb'
> 'abc' > 'ab'
> 'b' > 'abc'
---
FALSE
TRUE
TRUE
TRUE

# Empty strings.
> '' > ''
> 'a' > ''
> '' > 'a'
---
FALSE
TRUE
FALSE

# String case comparisons.
> 'a' > 'B'
> 'z' > 'B'
> 'A' > 'b'
> 'Z' > 'b'
---
TRUE
TRUE
FALSE
FALSE

# Unicode strings.
> '🙁' > '😀'
> '😀' > '😀'
> '😀' > '🙁'
---
TRUE
FALSE
FALSE

# NULLs.
> TRUE > NULL
> NULL > TRUE
> 1 > NULL
> NULL > 1
> 3.14 > NULL
> NULL > 3.14
> '' > NULl
> NULL > ''
> NULL > NULL
> NULL > NAN
---
NULL
NULL
NULL
NULL
NULL
NULL
NULL
NULL
NULL
NULL

# Type conflicts.
!> TRUE > 1
!> TRUE > ''
!> '' > 1
---
Error: invalid input: can't compare TRUE and 1
Error: invalid input: can't compare TRUE and ''
Error: invalid input: can't compare '' and 1
