# Tests the - negation prefix operator.

# Integer and float works.
[expr]> -1
[expr]> -3.14
---
-1 ← Negate(Constant(Integer(1)))
-3.14 ← Negate(Constant(Float(3.14)))

# NULL, infinity and NaN.
> -NULL
> -INFINITY
> -NAN
---
NULL
-inf
NaN

# Multiple applications work.
[expr]> ---1
[expr]> ----1
---
-1 ← Negate(Negate(Negate(Constant(Integer(1)))))
1 ← Negate(Negate(Negate(Negate(Constant(Integer(1))))))

# Bool and string fails.
!> -TRUE
!> -'a'
---
Error: invalid input: can't negate TRUE
Error: invalid input: can't negate 'a'
