# Tests the / division operator.

# Integers.
[expr]> 9 / 3
[expr]> 8 / 3
[expr]> 8 / -3
---
3 ← Divide(Constant(Integer(9)), Constant(Integer(3)))
2 ← Divide(Constant(Integer(8)), Constant(Integer(3)))
-2 ← Divide(Constant(Integer(8)), Negate(Constant(Integer(3))))

# Floats.
[expr]> 4.16 / 3.2
[expr]> 4.16 / -3.2
---
1.3 ← Divide(Constant(Float(4.16)), Constant(Float(3.2)))
-1.3 ← Divide(Constant(Float(4.16)), Negate(Constant(Float(3.2))))

# Mixed always yields floats.
> 3 / 1.2
> 1.2 / 3
> 9.0 / 3
> 0.0 / 1
---
2.5
0.39999999999999997
3.0
0.0

# Division by zero errors for integers, yields infinity or nan for floats.
!> 1 / 0
!> 0 / 0
!> -1 / 0
> 1.0 / 0.0
> 0.0 / 0.0
> -1.0 / 0.0
> 1.0 / -0.0
---
Error: invalid input: can't divide by zero
Error: invalid input: can't divide by zero
Error: invalid input: can't divide by zero
inf
NaN
-inf
-inf

# Division with NULL always yields NULL.
> 1 / NULL
> NULL / 1
> 1.0 / NULL
> NULL / 1.0
> NULL / NULL
> NULL / 0
---
NULL
NULL
NULL
NULL
NULL
NULL

# Division by infinity.
> 3.14 / INFINITY
> 3.14 / -INFINITY
> -3.14 / INFINITY
> INFINITY / 10
> 0 / INFINITY
> INFINITY / 0.0
> INFINITY / INFINITY
> -INFINITY / -INFINITY
---
0.0
-0.0
-0.0
inf
0.0
inf
NaN
NaN

# Division by NaN.
> 1 / NAN
> NAN / 1
> NAN / NAN
> NAN / 0
---
NaN
NaN
NaN
NaN

# Bools and strings error.
!> TRUE / FALSE
!> 'a' / 'b'
---
Error: invalid input: can't divide TRUE and FALSE
Error: invalid input: can't divide 'a' and 'b'
