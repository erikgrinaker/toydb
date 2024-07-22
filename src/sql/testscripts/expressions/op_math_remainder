# Tests the % remainder operator.
#
# Note that remainder is not the same as modulo: the former has the sign of the
# dividend, while the latter always has a positive value.

# Integers.
[expr]> 5 % 3
[expr]> -5 % 3
[expr]> 5 % -3
---
2 ← Remainder(Constant(Integer(5)), Constant(Integer(3)))
-2 ← Remainder(Negate(Constant(Integer(5))), Constant(Integer(3)))
2 ← Remainder(Constant(Integer(5)), Negate(Constant(Integer(3))))

# Floats.
[expr]> 6.28 % 2.2
[expr]> 6.28 % -2.2
---
1.88 ← Remainder(Constant(Float(6.28)), Constant(Float(2.2)))
1.88 ← Remainder(Constant(Float(6.28)), Negate(Constant(Float(2.2))))

# Mixed.
> 3.15 % 2
> 6 % 3.15
> 3.15 % -2
---
1.15
2.85
1.15

# Division by zero.
!> 7 % 0
> 6.28 % 0.0
---
Error: invalid input: can't divide by zero
NaN

# NULLs.
> 1 % NULL
> NULL % 3
> 3.14 % NULL
> NULL % NULL
---
NULL
NULL
NULL
NULL

# Infinity and NaN.
> INFINITY % 7
> 7 % INFINITY
> 7 % -INFINITY
> INFINITY % INFINITY
> 7 % NAN
> NAN % 7
> NAN % NAN
---
NaN
7.0
7.0
NaN
NaN
NaN
NaN

# Bools and strings.
!> TRUE % FALSE
!> 'a' % 'b'
---
Error: invalid input: can't take remainder of TRUE and FALSE
Error: invalid input: can't take remainder of 'a' and 'b'
