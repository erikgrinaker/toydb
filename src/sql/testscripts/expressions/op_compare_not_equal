# Tests the != inequality operator.

# != is a combination of NOT and =, just verify that for a few basic cases.

[expr]> 1 != 1
[expr]> 1 != 3
[expr]> 1 != NULL
---
FALSE ← Not(Equal(Constant(Integer(1)), Constant(Integer(1))))
TRUE ← Not(Equal(Constant(Integer(1)), Constant(Integer(3))))
NULL ← Not(Equal(Constant(Integer(1)), Constant(Null)))

[expr]> 3.0 != 3
[expr]> 0.0 != -0.0
---
FALSE ← Not(Equal(Constant(Float(3.0)), Constant(Integer(3))))
FALSE ← Not(Equal(Constant(Float(0.0)), Negate(Constant(Float(0.0)))))

[expr]> NAN != NAN
[expr]> INFINITY != INFINITY
[expr]> NULL != NULL
---
TRUE ← Not(Equal(Constant(Float(NaN)), Constant(Float(NaN))))
FALSE ← Not(Equal(Constant(Float(inf)), Constant(Float(inf))))
NULL ← Not(Equal(Constant(Null), Constant(Null)))
