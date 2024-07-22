# Tests the OR logical operator.

# Basic truth table.
> TRUE OR TRUE
> TRUE OR FALSE
> FALSE OR TRUE
> FALSE OR FALSE
---
TRUE
TRUE
TRUE
FALSE

# Trinary logic.
> TRUE OR NULL
> NULL OR TRUE
> FALSE OR NULL
> NULL OR FALSE
> NULL OR NULL
---
TRUE
TRUE
NULL
NULL
NULL

# Non-booleans.
!> 1 OR TRUE
!> TRUE OR 1
!> 1 OR 1
!> 3.14 OR TRUE
!> TRUE OR 3.14
!> 3.14 OR 3.14
!> 'true' OR TRUE
!> TRUE OR 'true'
!> 'true' OR 'true'
---
Error: invalid input: can't OR 1 and TRUE
Error: invalid input: can't OR TRUE and 1
Error: invalid input: can't OR 1 and 1
Error: invalid input: can't OR 3.14 and TRUE
Error: invalid input: can't OR TRUE and 3.14
Error: invalid input: can't OR 3.14 and 3.14
Error: invalid input: can't OR 'true' and TRUE
Error: invalid input: can't OR TRUE and 'true'
Error: invalid input: can't OR 'true' and 'true'
