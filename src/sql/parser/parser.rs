#![allow(clippy::module_inception)]

use super::ast;
use super::{Keyword, Lexer, Token};
use crate::errinput;
use crate::error::Result;
use crate::sql::types::DataType;

/// The SQL parser takes tokens from the lexer and parses the SQL syntax into an
/// AST (Abstract Syntax Tree). This nested structure represents the semantic
/// components of a SQL query (e.g. the SELECT and FROM clauses, values,
/// arithmetic expressions, etc.), but only makes sure it is well-formed. It
/// does not know e.g. whether a given table or column exists, or which kind of
/// join to use -- that is the job of the planner.
pub struct Parser<'a> {
    lexer: std::iter::Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    /// Creates a new parser for the given SQL string.
    pub fn new(statement: &str) -> Parser {
        Parser { lexer: Lexer::new(statement).peekable() }
    }

    /// Parses the input string into an AST statement. We expect to parse the
    /// whole string as a single statement, ending with an optional semicolon.
    pub fn parse(&mut self) -> Result<ast::Statement> {
        let statement = self.parse_statement()?;
        self.next_is(Token::Semicolon);
        if let Some(token) = self.lexer.next().transpose()? {
            return errinput!("unexpected token {token}");
        }
        Ok(statement)
    }

    /// Fetches the next lexer token, or throws an error if none is found.
    fn next(&mut self) -> Result<Token> {
        self.lexer.next().transpose()?.ok_or_else(|| errinput!("unexpected end of input"))
    }

    /// Returns the next identifier, or errors if not found.
    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => errinput!("expected identifier, got {token}"),
        }
    }

    /// Returns the next lexer token if it satisfies the predicate.
    fn next_if(&mut self, predicate: impl Fn(&Token) -> bool) -> Option<Token> {
        self.peek().unwrap_or(None).filter(|t| predicate(t))?;
        self.next().ok()
    }

    /// Passes the next lexer token through the closure, consuming it if the
    /// closure returns Some.
    fn next_if_map<T>(&mut self, f: impl Fn(&Token) -> Option<T>) -> Result<Option<T>> {
        let out = self.peek()?.and_then(f);
        if out.is_some() {
            self.next()?;
        }
        Ok(out)
    }

    /// Grabs the next keyword if there is one.
    fn next_if_keyword(&mut self) -> Option<Keyword> {
        match self.next_if(|t| matches!(t, Token::Keyword(_))) {
            Some(Token::Keyword(keyword)) => Some(keyword),
            Some(_) | None => None,
        }
    }

    /// Consumes the next lexer token if it is the given token, returning true.
    fn next_is(&mut self, token: Token) -> bool {
        self.next_if(|t| t == &token).is_some()
    }

    /// Consumes the next lexer token if it's the expected token, or errors.
    fn expect(&mut self, expect: Token) -> Result<()> {
        let token = self.next()?;
        if token != expect {
            return errinput!("expected token {expect}, found {token}");
        }
        Ok(())
    }

    /// Consumes the next lexer token if it is the given token. Mostly
    /// equivalent to next_is(), but expresses intent better.
    fn skip(&mut self, token: Token) {
        self.next_is(token);
    }

    /// Peeks the next lexer token if any, but transposes it for convenience.
    fn peek(&mut self) -> Result<Option<&Token>> {
        self.lexer.peek().map(|r| r.as_ref().map_err(|err| err.clone())).transpose()
    }

    /// Parses a SQL statement.
    fn parse_statement(&mut self) -> Result<ast::Statement> {
        match self.peek()? {
            Some(Token::Keyword(Keyword::Begin)) => self.parse_begin(),
            Some(Token::Keyword(Keyword::Commit)) => self.parse_commit(),
            Some(Token::Keyword(Keyword::Rollback)) => self.parse_rollback(),
            Some(Token::Keyword(Keyword::Explain)) => self.parse_explain(),

            Some(Token::Keyword(Keyword::Create)) => self.parse_create_table(),
            Some(Token::Keyword(Keyword::Drop)) => self.parse_drop_table(),

            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_update(),

            Some(token) => errinput!("unexpected token {token}"),
            None => errinput!("unexpected end of input"),
        }
    }

    /// Parses a BEGIN statement.
    fn parse_begin(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Begin.into())?;
        self.skip(Keyword::Transaction.into());

        let mut read_only = false;
        if self.next_is(Keyword::Read.into()) {
            match self.next()? {
                Token::Keyword(Keyword::Only) => read_only = true,
                Token::Keyword(Keyword::Write) => {}
                token => return errinput!("unexpected token {token}"),
            }
        }

        let mut as_of = None;
        if self.next_is(Keyword::As.into()) {
            self.expect(Keyword::Of.into())?;
            self.expect(Keyword::System.into())?;
            self.expect(Keyword::Time.into())?;
            match self.next()? {
                Token::Number(n) => as_of = Some(n.parse()?),
                token => return errinput!("unexpected token {token}, wanted number"),
            }
        }
        Ok(ast::Statement::Begin { read_only, as_of })
    }

    /// Parses a COMMIT statement.
    fn parse_commit(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Commit.into())?;
        Ok(ast::Statement::Commit)
    }

    /// Parses a ROLLBACK statement.
    fn parse_rollback(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Rollback.into())?;
        Ok(ast::Statement::Rollback)
    }

    /// Parses an EXPLAIN statement.
    fn parse_explain(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Explain.into())?;
        if self.next_is(Keyword::Explain.into()) {
            return errinput!("cannot nest EXPLAIN statements");
        }
        Ok(ast::Statement::Explain(Box::new(self.parse_statement()?)))
    }

    /// Parses a CREATE TABLE statement.
    fn parse_create_table(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Create.into())?;
        self.expect(Keyword::Table.into())?;
        let name = self.next_ident()?;
        self.expect(Token::OpenParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_create_table_column()?);
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        self.expect(Token::CloseParen)?;
        Ok(ast::Statement::CreateTable { name, columns })
    }

    /// Parses a CREATE TABLE column definition.
    fn parse_create_table_column(&mut self) -> Result<ast::Column> {
        let name = self.next_ident()?;
        let datatype = match self.next()? {
            Token::Keyword(Keyword::Bool | Keyword::Boolean) => DataType::Boolean,
            Token::Keyword(Keyword::Float | Keyword::Double) => DataType::Float,
            Token::Keyword(Keyword::Int | Keyword::Integer) => DataType::Integer,
            Token::Keyword(Keyword::String | Keyword::Text | Keyword::Varchar) => DataType::String,
            token => return errinput!("unexpected token {token}"),
        };
        let mut column = ast::Column {
            name,
            datatype,
            primary_key: false,
            nullable: None,
            default: None,
            unique: false,
            index: false,
            references: None,
        };
        while let Some(keyword) = self.next_if_keyword() {
            match keyword {
                Keyword::Primary => {
                    self.expect(Keyword::Key.into())?;
                    column.primary_key = true;
                }
                Keyword::Null => {
                    if column.nullable.is_some() {
                        return errinput!("nullability already set for column {}", column.name);
                    }
                    column.nullable = Some(true)
                }
                Keyword::Not => {
                    self.expect(Keyword::Null.into())?;
                    if column.nullable.is_some() {
                        return errinput!("nullability already set for column {}", column.name);
                    }
                    column.nullable = Some(false)
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                Keyword::Unique => column.unique = true,
                Keyword::Index => column.index = true,
                Keyword::References => column.references = Some(self.next_ident()?),
                keyword => return errinput!("unexpected keyword {keyword}"),
            }
        }
        Ok(column)
    }

    /// Parses a DROP TABLE statement.
    fn parse_drop_table(&mut self) -> Result<ast::Statement> {
        self.expect(Token::Keyword(Keyword::Drop))?;
        self.expect(Token::Keyword(Keyword::Table))?;
        let mut if_exists = false;
        if self.next_is(Keyword::If.into()) {
            self.expect(Token::Keyword(Keyword::Exists))?;
            if_exists = true;
        }
        let name = self.next_ident()?;
        Ok(ast::Statement::DropTable { name, if_exists })
    }

    /// Parses a DELETE statement.
    fn parse_delete(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Delete.into())?;
        self.expect(Keyword::From.into())?;
        let table = self.next_ident()?;
        Ok(ast::Statement::Delete { table, r#where: self.parse_where_clause()? })
    }

    /// Parses an INSERT statement.
    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Insert.into())?;
        self.expect(Keyword::Into.into())?;
        let table = self.next_ident()?;

        let mut columns = None;
        if self.next_is(Token::OpenParen) {
            let columns = columns.insert(Vec::new());
            loop {
                columns.push(self.next_ident()?);
                if !self.next_is(Token::Comma) {
                    break;
                }
            }
            self.expect(Token::CloseParen)?;
        }

        self.expect(Keyword::Values.into())?;

        let mut values = Vec::new();
        loop {
            let mut row = Vec::new();
            self.expect(Token::OpenParen)?;
            loop {
                row.push(self.parse_expression()?);
                if !self.next_is(Token::Comma) {
                    break;
                }
            }
            self.expect(Token::CloseParen)?;
            values.push(row);
            if !self.next_is(Token::Comma) {
                break;
            }
        }

        Ok(ast::Statement::Insert { table, columns, values })
    }

    /// Parses an UPDATE statement.
    fn parse_update(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Update.into())?;
        let table = self.next_ident()?;
        self.expect(Keyword::Set.into())?;

        let mut set = std::collections::BTreeMap::new();
        loop {
            let column = self.next_ident()?;
            self.expect(Token::Equal)?;
            let expr = self.parse_expression()?;
            if set.contains_key(&column) {
                return errinput!("column {column} set multiple times");
            }
            set.insert(column, expr);
            if !self.next_is(Token::Comma) {
                break;
            }
        }

        Ok(ast::Statement::Update { table, set, r#where: self.parse_where_clause()? })
    }

    /// Parses a SELECT statement.
    fn parse_select(&mut self) -> Result<ast::Statement> {
        Ok(ast::Statement::Select {
            select: self.parse_select_clause()?,
            from: self.parse_from_clause()?,
            r#where: self.parse_where_clause()?,
            group_by: self.parse_group_by_clause()?,
            having: self.parse_having_clause()?,
            order: self.parse_order_by_clause()?,
            limit: self
                .next_is(Keyword::Limit.into())
                .then(|| self.parse_expression())
                .transpose()?,
            offset: self
                .next_is(Keyword::Offset.into())
                .then(|| self.parse_expression())
                .transpose()?,
        })
    }

    /// Parses a SELECT clause, if present.
    fn parse_select_clause(&mut self) -> Result<Vec<(ast::Expression, Option<String>)>> {
        if !self.next_is(Keyword::Select.into()) {
            return Ok(Vec::new());
        }
        if self.next_is(Token::Asterisk) {
            return Ok(Vec::new());
        }
        let mut select = Vec::new();
        loop {
            let expr = self.parse_expression()?;
            let mut label = None;
            if self.next_is(Keyword::As.into()) || matches!(self.peek()?, Some(Token::Ident(_))) {
                label = Some(self.next_ident()?);
            }
            select.push((expr, label));
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(select)
    }

    /// Parses a FROM clause, if present.
    fn parse_from_clause(&mut self) -> Result<Vec<ast::From>> {
        if !self.next_is(Keyword::From.into()) {
            return Ok(Vec::new());
        }
        let mut from = Vec::new();
        loop {
            let mut item = self.parse_from_table()?;
            while let Some(r#type) = self.parse_from_join()? {
                let left = Box::new(item);
                let right = Box::new(self.parse_from_table()?);
                let mut predicate = None;
                if r#type != ast::JoinType::Cross {
                    self.expect(Keyword::On.into())?;
                    predicate = Some(self.parse_expression()?)
                }
                item = ast::From::Join { left, right, r#type, predicate };
            }
            from.push(item);
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(from)
    }

    // Parses a FROM table.
    fn parse_from_table(&mut self) -> Result<ast::From> {
        let name = self.next_ident()?;
        let mut alias = None;
        if self.next_is(Keyword::As.into()) || matches!(self.peek()?, Some(Token::Ident(_))) {
            alias = Some(self.next_ident()?)
        };
        Ok(ast::From::Table { name, alias })
    }

    // Parses a FROM JOIN type, if present.
    fn parse_from_join(&mut self) -> Result<Option<ast::JoinType>> {
        if self.next_is(Keyword::Cross.into()) {
            self.expect(Keyword::Join.into())?;
            Ok(Some(ast::JoinType::Cross))
        } else if self.next_is(Keyword::Inner.into()) {
            self.expect(Keyword::Join.into())?;
            Ok(Some(ast::JoinType::Inner))
        } else if self.next_is(Keyword::Join.into()) {
            Ok(Some(ast::JoinType::Inner))
        } else if self.next_is(Keyword::Left.into()) {
            self.skip(Keyword::Outer.into());
            self.expect(Keyword::Join.into())?;
            Ok(Some(ast::JoinType::Left))
        } else if self.next_is(Keyword::Right.into()) {
            self.skip(Keyword::Outer.into());
            self.expect(Keyword::Join.into())?;
            Ok(Some(ast::JoinType::Right))
        } else {
            Ok(None)
        }
    }

    /// Parses a WHERE clause, if present.
    fn parse_where_clause(&mut self) -> Result<Option<ast::Expression>> {
        if !self.next_is(Keyword::Where.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }

    /// Parses a GROUP BY clause, if present.
    fn parse_group_by_clause(&mut self) -> Result<Vec<ast::Expression>> {
        if !self.next_is(Keyword::Group.into()) {
            return Ok(Vec::new());
        }
        let mut group_by = Vec::new();
        self.expect(Keyword::By.into())?;
        loop {
            group_by.push(self.parse_expression()?);
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(group_by)
    }

    /// Parses a HAVING clause, if present.
    fn parse_having_clause(&mut self) -> Result<Option<ast::Expression>> {
        if !self.next_is(Keyword::Having.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }

    /// Parses an ORDER BY clause, if present.
    fn parse_order_by_clause(&mut self) -> Result<Vec<(ast::Expression, ast::Order)>> {
        if !self.next_is(Keyword::Order.into()) {
            return Ok(Vec::new());
        }
        let mut order_by = Vec::new();
        self.expect(Keyword::By.into())?;
        loop {
            let expr = self.parse_expression()?;
            let order = if self.next_is(Keyword::Asc.into()) {
                ast::Order::Ascending
            } else if self.next_is(Keyword::Desc.into()) {
                ast::Order::Descending
            } else {
                ast::Order::Ascending
            };
            order_by.push((expr, order));
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(order_by)
    }

    /// Parses an expression consisting of at least one atom operated on by any
    /// number of operators, using the precedence climbing algorithm.
    /// TODO: write a description of the algorithm.
    fn parse_expression(&mut self) -> Result<ast::Expression> {
        self.parse_expression_at(0)
    }

    /// Parses an expression at the given minimum precedence.
    fn parse_expression_at(&mut self, min_precedence: Precedence) -> Result<ast::Expression> {
        // If there is a prefix operator, parse it and its right-hand operand.
        // Otherwise, parse the left-hand atom.
        let mut lhs = if let Some(prefix) = PrefixOperator::parse(self, min_precedence)? {
            let at_precedence = prefix.precedence() + prefix.associativity();
            prefix.build(self.parse_expression_at(at_precedence)?)
        } else {
            self.parse_expression_atom()?
        };
        // Apply any postfix operators.
        while let Some(postfix) = PostfixOperator::parse(self, min_precedence)? {
            lhs = postfix.build(lhs)
        }
        // Apply any binary infix operators, parsing the right-hand operand.
        while let Some(infix) = InfixOperator::parse(self, min_precedence)? {
            let at_precedence = infix.precedence() + infix.associativity();
            let rhs = self.parse_expression_at(at_precedence)?;
            lhs = infix.build(lhs, rhs)
        }
        Ok(lhs)
    }

    /// Parses an expression atom. This is either:
    ///
    /// * A literal value.
    /// * A field name.
    /// * A function call.
    /// * A parenthesized expression.
    fn parse_expression_atom(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            // Literal value.
            Token::Number(n) if n.chars().all(|c| c.is_ascii_digit()) => {
                ast::Literal::Integer(n.parse()?).into()
            }
            Token::Number(n) => ast::Literal::Float(n.parse()?).into(),
            Token::String(s) => ast::Literal::String(s).into(),
            Token::Keyword(Keyword::True) => ast::Literal::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Literal::Boolean(false).into(),
            Token::Keyword(Keyword::Infinity) => ast::Literal::Float(std::f64::INFINITY).into(),
            Token::Keyword(Keyword::NaN) => ast::Literal::Float(std::f64::NAN).into(),
            Token::Keyword(Keyword::Null) => ast::Literal::Null.into(),

            // Function call.
            Token::Ident(name) if self.next_is(Token::OpenParen) => {
                let mut args = Vec::new();
                while !self.next_is(Token::CloseParen) {
                    if !args.is_empty() {
                        self.expect(Token::Comma)?;
                    }
                    if name == "count" && self.next_is(Token::Asterisk) {
                        // TODO: ugly hack to handle COUNT(*).
                        args.push(ast::Expression::Literal(ast::Literal::Boolean(true)));
                    } else {
                        args.push(self.parse_expression()?);
                    }
                }
                ast::Expression::Function(name, args)
            }

            // Field name, either qualified as table.field or unqualified.
            Token::Ident(table) if self.next_is(Token::Period) => {
                ast::Expression::Field(Some(table), self.next_ident()?)
            }
            Token::Ident(field) => ast::Expression::Field(None, field),

            // Parenthesized expression.
            Token::OpenParen => {
                let expr = self.parse_expression()?;
                self.expect(Token::CloseParen)?;
                expr
            }

            token => return errinput!("expected expression atom, found {token}"),
        })
    }
}

/// Operator precedence.
type Precedence = u8;

const LEFT_ASSOCIATIVE: Precedence = 1;
const RIGHT_ASSOCIATIVE: Precedence = 0;

/// Prefix operators.
enum PrefixOperator {
    Minus,
    Not,
    Plus,
}

impl PrefixOperator {
    /// Parses a prefix operator, if there is one and it's precedence is at
    /// least min_precedence.
    fn parse(parser: &mut Parser, min_precedence: Precedence) -> Result<Option<Self>> {
        parser.next_if_map(|token| {
            let operator = match token {
                Token::Keyword(Keyword::Not) => Self::Not,
                Token::Minus => Self::Minus,
                Token::Plus => Self::Plus,
                _ => return None,
            };
            Some(operator).filter(|op| op.precedence() >= min_precedence)
        })
    }

    /// The operator precedence. Prefix operators have the highest precedence.
    fn precedence(&self) -> Precedence {
        9
    }

    // The operator associativity. Prefix operators are right-associative by
    // definition.
    fn associativity(&self) -> Precedence {
        RIGHT_ASSOCIATIVE
    }

    /// Builds an AST expression for the operator.
    fn build(&self, rhs: ast::Expression) -> ast::Expression {
        let rhs = Box::new(rhs);
        let operator = match self {
            Self::Plus => ast::Operator::Identity(rhs),
            Self::Minus => ast::Operator::Negate(rhs),
            Self::Not => ast::Operator::Not(rhs),
        };
        ast::Expression::Operator(operator)
    }
}

/// Infix operators.
enum InfixOperator {
    Add,
    And,
    Divide,
    Equal,
    Exponentiate,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Like,
    Modulo,
    Multiply,
    NotEqual,
    Or,
    Subtract,
}

impl InfixOperator {
    /// Parses an infix operator, if there is one and it's precedence is at
    /// least min_precedence.
    fn parse(parser: &mut Parser, min_precedence: Precedence) -> Result<Option<Self>> {
        parser.next_if_map(|token| {
            let operator = match token {
                Token::Asterisk => Self::Multiply,
                Token::Caret => Self::Exponentiate,
                Token::Equal => Self::Equal,
                Token::GreaterThan => Self::GreaterThan,
                Token::GreaterThanOrEqual => Self::GreaterThanOrEqual,
                Token::Keyword(Keyword::And) => Self::And,
                Token::Keyword(Keyword::Like) => Self::Like,
                Token::Keyword(Keyword::Or) => Self::Or,
                Token::LessOrGreaterThan => Self::NotEqual,
                Token::LessThan => Self::LessThan,
                Token::LessThanOrEqual => Self::LessThanOrEqual,
                Token::Minus => Self::Subtract,
                Token::NotEqual => Self::NotEqual,
                Token::Percent => Self::Modulo,
                Token::Plus => Self::Add,
                Token::Slash => Self::Divide,
                _ => return None,
            };
            Some(operator).filter(|op| op.precedence() >= min_precedence)
        })
    }

    /// The operator precedence.
    fn precedence(&self) -> Precedence {
        match self {
            Self::Or => 1,
            Self::And => 2,
            Self::Equal | Self::NotEqual | Self::Like => 3,
            Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::LessThan
            | Self::LessThanOrEqual => 4,
            Self::Add | Self::Subtract => 5,
            Self::Multiply | Self::Divide | Self::Modulo => 6,
            Self::Exponentiate => 7,
        }
    }

    /// The operator associativity.
    fn associativity(&self) -> Precedence {
        match self {
            Self::Exponentiate => RIGHT_ASSOCIATIVE,
            _ => LEFT_ASSOCIATIVE,
        }
    }

    /// Builds an AST expression for the infix operator.
    fn build(&self, lhs: ast::Expression, rhs: ast::Expression) -> ast::Expression {
        let (lhs, rhs) = (Box::new(lhs), Box::new(rhs));
        let operator = match self {
            Self::Add => ast::Operator::Add(lhs, rhs),
            Self::And => ast::Operator::And(lhs, rhs),
            Self::Divide => ast::Operator::Divide(lhs, rhs),
            Self::Equal => ast::Operator::Equal(lhs, rhs),
            Self::Exponentiate => ast::Operator::Exponentiate(lhs, rhs),
            Self::GreaterThan => ast::Operator::GreaterThan(lhs, rhs),
            Self::GreaterThanOrEqual => ast::Operator::GreaterThanOrEqual(lhs, rhs),
            Self::LessThan => ast::Operator::LessThan(lhs, rhs),
            Self::LessThanOrEqual => ast::Operator::LessThanOrEqual(lhs, rhs),
            Self::Like => ast::Operator::Like(lhs, rhs),
            Self::Modulo => ast::Operator::Modulo(lhs, rhs),
            Self::Multiply => ast::Operator::Multiply(lhs, rhs),
            Self::NotEqual => ast::Operator::NotEqual(lhs, rhs),
            Self::Or => ast::Operator::Or(lhs, rhs),
            Self::Subtract => ast::Operator::Subtract(lhs, rhs),
        };
        ast::Expression::Operator(operator)
    }
}

/// Postfix operators.
enum PostfixOperator {
    Factorial,
    IsNotNull,
    IsNull,
}

impl PostfixOperator {
    /// Parses a postfix operator, if there is one and it's precedence is at
    /// least min_precedence.
    fn parse(parser: &mut Parser, min_precedence: Precedence) -> Result<Option<Self>> {
        // Handle IS (NOT) NULL separately, since it's multiple tokens.
        if let Some(Token::Keyword(Keyword::Is)) = parser.peek()? {
            // We can't consume tokens unless the precedence is satisfied, so we
            // assume IS NULL (they both have the same precedence).
            if Self::IsNull.precedence() < min_precedence {
                return Ok(None);
            }
            parser.expect(Keyword::Is.into())?;
            if parser.next_is(Keyword::Not.into()) {
                parser.expect(Keyword::Null.into())?;
                return Ok(Some(Self::IsNotNull));
            }
            parser.expect(Keyword::Null.into())?;
            return Ok(Some(Self::IsNull));
        }

        parser.next_if_map(|token| {
            let op = match token {
                Token::Exclamation => Self::Factorial,
                _ => return None,
            };
            Some(op).filter(|op| op.precedence() >= min_precedence)
        })
    }

    // The operator precedence. Postfix operators are below prefix operators.
    fn precedence(&self) -> Precedence {
        8
    }

    /// Builds an AST expression for the operator.
    fn build(&self, lhs: ast::Expression) -> ast::Expression {
        let lhs = Box::new(lhs);
        let operator = match self {
            Self::Factorial => ast::Operator::Factorial(lhs),
            Self::IsNotNull => ast::Operator::Not(Box::new(ast::Operator::IsNull(lhs).into())),
            Self::IsNull => ast::Operator::IsNull(lhs),
        };
        ast::Expression::Operator(operator)
    }
}
