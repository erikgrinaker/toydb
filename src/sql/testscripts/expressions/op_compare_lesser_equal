# Tests the <= less than or equal operator.

# This is implemented as < OR =, just verify this for a few basic cases.

[expr]> 1 <= 0
[expr]> 0 <= 0
[expr]> -1 <= 0
---
FALSE ← Or(LessThan(Constant(Integer(1)), Constant(Integer(0))), Equal(Constant(Integer(1)), Constant(Integer(0))))
TRUE ← Or(LessThan(Constant(Integer(0)), Constant(Integer(0))), Equal(Constant(Integer(0)), Constant(Integer(0))))
TRUE ← Or(LessThan(Negate(Constant(Integer(1))), Constant(Integer(0))), Equal(Negate(Constant(Integer(1))), Constant(Integer(0))))

[expr]> 0.0 <= -0.0
[expr]> INFINITY <= INFINITY
[expr]> NAN <= NAN
---
TRUE ← Or(LessThan(Constant(Float(0.0)), Negate(Constant(Float(0.0)))), Equal(Constant(Float(0.0)), Negate(Constant(Float(0.0)))))
TRUE ← Or(LessThan(Constant(Float(inf)), Constant(Float(inf))), Equal(Constant(Float(inf)), Constant(Float(inf))))
FALSE ← Or(LessThan(Constant(Float(NaN)), Constant(Float(NaN))), Equal(Constant(Float(NaN)), Constant(Float(NaN))))

[expr]> NULL <= 1
[expr]> NULL <= NAN
[expr]> NULL <= NULL
---
NULL ← Or(LessThan(Constant(Null), Constant(Integer(1))), Equal(Constant(Null), Constant(Integer(1))))
NULL ← Or(LessThan(Constant(Null), Constant(Float(NaN))), Equal(Constant(Null), Constant(Float(NaN))))
NULL ← Or(LessThan(Constant(Null), Constant(Null)), Equal(Constant(Null), Constant(Null)))
