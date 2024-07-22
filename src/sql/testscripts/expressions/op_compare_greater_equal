# Tests the >= greater than operator.

# This is implemented as > OR =, just verify this for a few basic cases.

[expr]> 0 >= 1
[expr]> 0 >= 0
[expr]> 0 >= -1
---
FALSE ← Or(GreaterThan(Constant(Integer(0)), Constant(Integer(1))), Equal(Constant(Integer(0)), Constant(Integer(1))))
TRUE ← Or(GreaterThan(Constant(Integer(0)), Constant(Integer(0))), Equal(Constant(Integer(0)), Constant(Integer(0))))
TRUE ← Or(GreaterThan(Constant(Integer(0)), Negate(Constant(Integer(1)))), Equal(Constant(Integer(0)), Negate(Constant(Integer(1)))))

[expr]> -0.0 >= 0.0
[expr]> INFINITY >= INFINITY
[expr]> NAN >= NAN
---
TRUE ← Or(GreaterThan(Negate(Constant(Float(0.0))), Constant(Float(0.0))), Equal(Negate(Constant(Float(0.0))), Constant(Float(0.0))))
TRUE ← Or(GreaterThan(Constant(Float(inf)), Constant(Float(inf))), Equal(Constant(Float(inf)), Constant(Float(inf))))
FALSE ← Or(GreaterThan(Constant(Float(NaN)), Constant(Float(NaN))), Equal(Constant(Float(NaN)), Constant(Float(NaN))))

[expr]> NULL >= 1
[expr]> NULL >= NAN
[expr]> NULL >= NULL
---
NULL ← Or(GreaterThan(Constant(Null), Constant(Integer(1))), Equal(Constant(Null), Constant(Integer(1))))
NULL ← Or(GreaterThan(Constant(Null), Constant(Float(NaN))), Equal(Constant(Null), Constant(Float(NaN))))
NULL ← Or(GreaterThan(Constant(Null), Constant(Null)), Equal(Constant(Null), Constant(Null)))
