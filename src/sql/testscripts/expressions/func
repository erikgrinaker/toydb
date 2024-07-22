# Tests function calls.

# Function names are case-insensitive.
> sqrt(1)
> SQRT(1)
---
1.0
1.0

# A space is allowed around the arguments.
> sqrt ( 1 )
---
1.0

# Wrong number of arguments errors.
!> sqrt()
!> sqrt(1, 2)
---
Error: invalid input: unknown function sqrt with 0 arguments
Error: invalid input: unknown function sqrt with 2 arguments

# Unknown functions error.
!> unknown()
!> unknown(1, 2, 3)
---
Error: invalid input: unknown function unknown with 0 arguments
Error: invalid input: unknown function unknown with 3 arguments

# Parse errors.
!> unknown(1, 2, 3
!> unknown(1, 2, 3,)
!> unknown(1, 2 3)
---
Error: invalid input: unexpected end of input
Error: invalid input: expected expression atom, found )
Error: invalid input: expected token ,, found 3
