# Tests the < less than operator.

# Booleans.
> FALSE < TRUE
> TRUE < FALSE
> TRUE < TRUE
> FALSE < FALSE
---
TRUE
FALSE
FALSE
FALSE

# Integers.
> 3 < 2
> 3 < 3
> 3 < 4
> -1 < 0
> 0 < -1
---
FALSE
FALSE
TRUE
TRUE
FALSE

# Floats.
> 3.14 < 3.13
> 3.14 < 3.14
> 3.14 < 3.15
> -0.0 < 0.0
---
FALSE
FALSE
TRUE
FALSE

# Float special values.
> 1e300 < INFINITY
> INFINITY < INFINITY
> -INFINITY < INFINITY
> NAN < NAN
> NAN < INFINITY
> INFINITY < NAN
> 0.0 < NAN
---
TRUE
FALSE
TRUE
FALSE
FALSE
FALSE
FALSE

# Mixed integer/float values.
> 3 < 2.9
> 3 < 3.0
> 3 < 3.1
> -0.0 < 0
---
FALSE
FALSE
TRUE
FALSE

# Strings.
> 'abc' < 'abc'
> 'abb' < 'abc'
> 'ab' < 'abc'
> 'abc' < 'b'
---
FALSE
TRUE
TRUE
TRUE

# Empty strings.
> '' < ''
> '' < 'a'
> 'a' < ''
---
FALSE
TRUE
FALSE

# String case comparisons.
> 'B' < 'a'
> 'B' < 'z'
> 'B' < 'A'
> 'B' < 'Z'
---
TRUE
TRUE
FALSE
TRUE

# Unicode strings.
> '😀' < '🙁' 
> '😀' < '😀' 
> '🙁' < '😀' 
---
TRUE
FALSE
FALSE

# NULLs.
> TRUE < NULL
> NULL < TRUE
> 1 < NULL
> NULL < 1
> 3.14 < NULL
> NULL < 3.14
> '' < NULl
> NULL < ''
> NULL < NULL
> NULL < NAN
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
!> TRUE < 1
!> TRUE < ''
!> '' < 1
---
Error: invalid input: can't compare TRUE and 1
Error: invalid input: can't compare TRUE and ''
Error: invalid input: can't compare '' and 1
