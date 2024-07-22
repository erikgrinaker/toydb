# Tests the = equality operator.

# Booleans.
> TRUE = TRUE
> TRUE = FALSE
> FALSE = TRUE
---
TRUE
FALSE
FALSE

# Integers.
> 1 = 1
> 1 = 2
---
TRUE
FALSE

# Floats.
> 3.14 = 3.14
> 3.14 = 2.718
---
TRUE
FALSE

# Float special values.
> 0.0 = -0.0
> INFINITY = INFINITY
> NAN = NAN
---
TRUE
TRUE
FALSE

# Mixed integers and floats.
> 3.0 = 3
> 3.01 = 3
> 3 = 3.01
> -0.0 = 0
---
TRUE
FALSE
FALSE
TRUE

# Strings.
> 'abc' = 'abc'
> 'abc' = 'ab'
> 'abc' = 'abcd'
> 'abc' = 'ABC'
> '😀' = '😀'
> '😀' = '🙁'
---
TRUE
FALSE
FALSE
FALSE
TRUE
FALSE

# NULLs.
> 1 = NULL
> 3.14 = NULL
> FALSE = NULL
> '' = NULL
> NULL = NULL
> NAN = NULL
> INFINITY = NULL
---
NULL
NULL
NULL
NULL
NULL
NULL
NULL

# Type mismatches.
!> true = 1
!> 'true' = true
---
Error: invalid input: can't compare TRUE and 1
Error: invalid input: can't compare 'true' and TRUE
