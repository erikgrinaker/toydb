# Tests the * multiplication operator.

# Integers.
[expr]> 2 * 3
[expr]> 2 * -3
---
6 ← Multiply(Constant(Integer(2)), Constant(Integer(3)))
-6 ← Multiply(Constant(Integer(2)), Negate(Constant(Integer(3))))

# Float.
[expr]> 3.14 * 2.71
[expr]> 3.14 * -2.71
---
8.5094 ← Multiply(Constant(Float(3.14)), Constant(Float(2.71)))
-8.5094 ← Multiply(Constant(Float(3.14)), Negate(Constant(Float(2.71))))

# Mixed.
> 3.14 * 2
> -2 * 3.14
---
6.28
-6.28

# Integer and float overflow, underflow, and precision loss.
!> 9223372036854775807 * 2
!> 9223372036854775807 * -2
> 2e308 * 2
> 9223372036854775807 * 2.0
---
Error: invalid input: integer overflow
Error: invalid input: integer overflow
inf
1.8446744073709552e19


# NULLs always yield NULL.
> 1 * NULL
> NULL * 3.14
> NULL * NULL
---
NULL
NULL
NULL

# Infinity.
> 2 * INFINITY
> -2 * INFINITY
> 3.14 * -INFINITY
> INFINITY * INFINITY
> INFINITY * -INFINITY
---
inf
-inf
-inf
inf
-inf

# NaN.
> 2 * NAN
> -3.14 * NAN
> INFINITY * NAN
> NAN * NAN
---
NaN
NaN
NaN
NaN

# Bools and strings.
!> TRUE * FALSE
!> 'a' * 'b'
---
Error: invalid input: can't multiply TRUE and FALSE
Error: invalid input: can't multiply 'a' and 'b'
