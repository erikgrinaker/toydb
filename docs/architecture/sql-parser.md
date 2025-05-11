# SQL Parsing

And so we finally arrive at SQL. The SQL parser is the first stage in processing SQL
queries and statements, located in the [`src/sql/parser`](https://github.com/erikgrinaker/toydb/tree/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser)
module.

The SQL parser's job is to take a raw SQL string and turn it into a structured form that's more
convenient to work with. In doing so, it will validate that the string is in fact valid SQL
_syntax_. However, it doesn't know if the SQL statement actually makes sense -- it has no idea which
tables or columns exist, what their data types are, and so on. That's the job of the planner, which
we'll look at later.

For example, let's say the parser is given the following SQL query:

```sql
SELECT name, price, price * 25 / 100 AS vat
FROM products JOIN categories ON products.category_id = categories.id
WHERE categories.code = 'BLURAY' AND stock > 0
ORDER BY price DESC
LIMIT 10
```

It will generate a structure that looks something like this (in simplified syntax):

```rust
// A SELECT statement.
Statement::Select {
    // SELECT name, price, price * 25 / 100 AS vat
    select: [
        (Column("name"), None),
        (Column("price"), None),
        (
            Divide(
                Multiply(Column("price"), Integer(25)),
                Integer(100)
            ),
            Some("vat"),
        ),
    ]

    // FROM products JOIN categories ON products.category_id = categories.id
    from: [
        Join {
            left: Table("products"),
            right: Table("categories"),
            type: Inner,
            predicate: Some(
                Equal(
                    Column("products.category_id)",
                    Column("categories.id"),
                )
            )
        }
    ]

    // WHERE categories.code = 'BLURAY' AND stock > 0
    where: Some(
        And(
            Equal(
                Column("categories.code"),
                String("BLURAY"),
            ),
            GreaterThan(
                Column("stock"),
                Integer(0),
            )
        )
    )

    // ORDER BY price DESC
    order: [
        (Column("price"), Descending),
    ]

    // LIMIT 10
    limit: Some(Integer(10))
}
```

Let's have a look at how this happens.

## Lexer

We begin with the `sql::parser::Lexer`, which takes the raw SQL string and performs
[lexical analysis](https://en.wikipedia.org/wiki/Lexical_analysis) to convert it into a sequence of
tokens. These tokens are things like number, string, identifier, SQL keyword, and so on.

This preprocessing is useful to deal with some of the "noise" of SQL text, such as whitespace,
string quotes, identifier normalization, and so on. It also specifies which symbols and keywords are
valid in our SQL queries. This makes the parser's life a lot easier.

The lexer doesn't care about SQL structure at all, only that the individual pieces (tokens) of a
string are well-formed. For example, the following input string:

```
'foo' ) 3.14 SELECT + x
```

Will result in these tokens:

```
String("foo"), CloseParen, Number("3.14"), Keyword(Select), Plus, Ident("x")
```

Tokens and keywords are represented by the `sql::parser::Token` and `sql::parser::Keyword` enums
respectively:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/lexer.rs#L8-L47

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/lexer.rs#L86-L155

The lexer takes an input string and emits tokens as an iterator:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/lexer.rs#L311-L337

It does this by repeatedly attempting to scan the next token until it reaches the end of the string
(or errors). It can determine the kind of token by looking at the first character:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/lexer.rs#L358-L373

And then scan across the following characters as appropriate to generate a valid token. For example,
this is how a quoted string (e.g. `'foo'`) is lexed into a `Token::String` (including handling of
any escaped quotes inside the string):

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/lexer.rs#L435-L451

These tokens become the input to the parser.

## Abstract Syntax Tree

The end result of the parsing process will be an [abstract syntax tree](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
(AST), which is a structured representation of a SQL statement, located in the
[`sql::parser::ast`](https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/ast.rs) module.

The root of this tree is the `sql::parser::ast::Statement` enum, which represents all the different
kinds of SQL statements that we support, along with their contents:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/ast.rs#L6-L145

The nested tree structure is particularly apparent with _expressions_ -- these represent values and
operations which will eventually _evaluate_ to a single value. For example, the expression
`2 * 3 - 4 / 2`, which evaluates to the value `4`.

These expressions are represented as `sql::parser::ast::Expression`, and can be nested indefinitely
into a tree structure.

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/ast.rs#L147-L170

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/ast.rs#L204-L234

For example, `2 * 3 - 4 / 2` is represented as:

```rust
Expression::Operator(Operator::Subtract(
    // The left-hand operand of -
    Expression::Operator(Operator::Multiply(
        // The left-hand operand of *
        Expression::Literal(Literal::Integer(2)),
        // The right-hand operand of *
        Expression::Literal(Literal::Integer(3)),
    )),
    // The right-hand operand of -
    Expression::Operator(Operator::Divide(
        // The left-hand operand of /
        Expression::Literal(Literal::Integer(4)),
        // The right-hand operand of /
        Expression::Literal(Literal::Integer(2)),
    )),
))
```

## Parser

The parser, `sql::parser::Parser`, takes lexer tokens as input and builds an `ast::Statement`
from them:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/parser.rs#L9-L32

We can determine the kind of statement we're parsing simply by looking at the first keyword:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/parser.rs#L109-L130

Let's see how a `SELECT` statement is parsed. The different clauses in a `SELECT` (e.g. `FROM`,
`WHERE`, etc.) must always be given in a specific order, and they always begin with the appropriate
keyword, so we can simply try to parse each clause in the expected order:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/parser.rs#L330-L342

Parsing each clause is also just a matter of parsing the expected parts in order. For example, the
initial `SELECT` clause is just a comma-separated list of expressions with an optional alias:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/parser.rs#L344-L365

The `FROM` clause is a comma-separated list of table name, optionally joined with other tables:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/parser.rs#L367-L427

And the `WHERE` clause is just a predicate expression to filter by:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/parser.rs#L429-L435

Expression parsing is where this gets tricky, because we have to respect the rules of operator
precedence and associativity. For example, according to mathematical order of operations (aka
"PEMDAS") the expression `2 * 3 - 4 / 2` must be parsed as `(2 * 3) - (4 / 2)` which yields 4, not
`2 * (3 - 4) / 2` which yields -1.

toyDB does this using the [precedence climbing algorithm](https://en.wikipedia.org/wiki/Operator-precedence_parser#Precedence_climbing_method),
which is a fairly simple and compact algorithm as far as these things go. In a nutshell, it will
greedily and recursively group operators together as long as their precedence is the same or higher
than that of the operators preceding them (hence "precedence climbing"). For example:

```
-----   ----- Precedence 2: * and /
------------- Precedence 1: -
2 * 3 - 4 / 2
```

The algorithm is documented in more detail on `Parser::parse_expression`:

https://github.com/erikgrinaker/toydb/blob/39c6b60afc4c235f19113dc98087176748fa091d/src/sql/parser/parser.rs#L501-L696

---

<p align="center">
← <a href="sql-raft.md">SQL Raft Replication</a> &nbsp; | &nbsp; <a href="sql-planner.md">SQL Planning</a> →
</p>