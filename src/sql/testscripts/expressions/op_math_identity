# Tests the + identity prefix operator.

# Integer and float works.
[expr]> +1
[expr]> +3.14
---
1 ← Identity(Constant(Integer(1)))
3.14 ← Identity(Constant(Float(3.14)))

# NULL, infinity and NaN.
> +NULL
> +INFINITY
> +NAN
---
NULL
inf
NaN

# Multiple applications work.
[expr]> +++1
---
1 ← Identity(Identity(Identity(Constant(Integer(1)))))

# Bool and string fails.
!> +TRUE
!> +'a'
---
Error: invalid input: can't take the identity of TRUE
Error: invalid input: can't take the identity of 'a'
