# Tests the LIKE string pattern matching operator.

# Multi-character matches.
> 'abcde' LIKE 'a%e'
> 'abcde' LIKE 'abc%'
> 'abcde' LIKE '%cde'
> 'abcde' LIKE '%'
---
TRUE
TRUE
TRUE
TRUE

# Multi-character mismatches.
> 'abcde' LIKE 'a%f'
> 'abcde' LIKE 'b%e'
> 'abcde' LIKE 'b%'
> 'abcde' LIKE '%d'
---
FALSE
FALSE
FALSE
FALSE

# Multi-character wildcards match 0 characters.
> 'abcde' LIKE 'abc%de'
> 'abcde' LIKE '%abcde'
> 'abcde' LIKE 'abcde%'
> '' LIKE '%'
---
TRUE
TRUE
TRUE
TRUE

# Single-character matches.
> 'abcde' LIKE 'ab_de'
> 'abcde' LIKE '_bcde'
> 'abcde' LIKE 'abcd_'
---
TRUE
TRUE
TRUE

# Single-character mismatches.
> 'abcde' LIKE 'ab_e'
> 'abcde' LIKE 'abc_'
> 'abcde' LIKE '_bcd'
---
FALSE
FALSE
FALSE

# Single-character wildcards require at least one match.
> 'abcde' LIKE 'abc_de'
> 'abcde' LIKE '_abcde'
> 'abcde' LIKE 'abcde_'
> '' LIKE '_'
---
FALSE
FALSE
FALSE
FALSE

# Exact matches. Submatches are not sufficient.
> 'abcde' LIKE 'abcde'
> 'abcde' LIKE 'abc'
> 'abcde' LIKE 'abcdef'
---
TRUE
FALSE
FALSE

# Patterns are case-sensitive.
> 'abcde' LIKE 'ABCDE'
> 'abcde' LIKE 'A%'
---
FALSE
FALSE

# Wildcards can be mixed and used multiple times, and % can match nothing.
> 'abcde' LIKE 'a%c%e'
> 'abcde' LIKE '%%e'
> 'abcde' LIKE '%%abcde'
> 'abcde' LIKE 'a___e'
> 'abcdefghijklmno' LIKE 'a_c%f%i_kl%m_o'
---
TRUE
TRUE
TRUE
TRUE
TRUE

# NULLs.
> NULL LIKE '%'
> NULL LIKE '_'
> 'abc' LIKE NULL
> NULL LIKE NULL
---
NULL
NULL
NULL
NULL

# * and ? are not valid patterns.
> 'abcde' LIKE 'a*e'
> 'abcde' LIKE 'ab?de'
---
FALSE
FALSE

# Fails with non-strings.
!> 'abc' LIKE 1
!> 1 LIKE 'abc'
!> 'abc' LIKE 3.14
!> 3.14 LIKE 'abc'
!> 'abc' LIKE TRUE
!> TRUE LIKE 'abc'
---
Error: invalid input: can't LIKE 'abc' and 1
Error: invalid input: can't LIKE 1 and 'abc'
Error: invalid input: can't LIKE 'abc' and 3.14
Error: invalid input: can't LIKE 3.14 and 'abc'
Error: invalid input: can't LIKE 'abc' and TRUE
Error: invalid input: can't LIKE TRUE and 'abc'
