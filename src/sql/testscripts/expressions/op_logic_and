# Tests the AND logical operator.

# Basic truth table.
> TRUE AND TRUE
> TRUE AND FALSE
> FALSE AND TRUE
> FALSE AND FALSE
---
TRUE
FALSE
FALSE
FALSE

# Trinary logic.
> TRUE AND NULL
> NULL AND TRUE
> FALSE AND NULL
> NULL AND FALSE
> NULL AND NULL
---
NULL
NULL
FALSE
FALSE
NULL

# Non-booleans.
!> 1 AND TRUE
!> TRUE AND 1
!> 1 AND 1
!> 3.14 AND TRUE
!> TRUE AND 3.14
!> 3.14 AND 3.14
!> 'true' AND TRUE
!> TRUE AND 'true'
!> 'true' AND 'true'
---
Error: invalid input: can't AND 1 and TRUE
Error: invalid input: can't AND TRUE and 1
Error: invalid input: can't AND 1 and 1
Error: invalid input: can't AND 3.14 and TRUE
Error: invalid input: can't AND TRUE and 3.14
Error: invalid input: can't AND 3.14 and 3.14
Error: invalid input: can't AND 'true' and TRUE
Error: invalid input: can't AND TRUE and 'true'
Error: invalid input: can't AND 'true' and 'true'
