pub mod ast;
pub mod lexer;

use crate::Error;
use lexer::{Keyword, Lexer, Token};

/// An SQL parser
pub struct Parser<'a> {
    lexer: std::iter::Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    /// Creates a new parser for the given string input
    pub fn new(query: &str) -> Parser {
        Parser { lexer: Lexer::new(query).peekable() }
    }

    /// Parses the input string into an AST statement
    pub fn parse(&mut self) -> Result<ast::Statement, Error> {
        let statement = self.parse_statement()?;
        self.next_expect(None)?;
        Ok(statement)
    }

    /// Grabs the next lexer token, or throws an error if none is found.
    fn next(&mut self) -> Result<Token, Error> {
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse("Unexpected end of input".into())))
    }

    /// Grabs the next lexer token, and returns it if it was expected or
    /// otherwise throws an error.
    fn next_expect(&mut self, expect: Option<Token>) -> Result<Option<Token>, Error> {
        if let Some(t) = expect {
            let token = self.next()?;
            if token == t {
                Ok(Some(token))
            } else {
                Err(Error::Parse(format!("Expected token {}, found {}", t, token)))
            }
        } else if let Some(token) = self.peek()? {
            Err(Error::Parse(format!("Unexpected token {}", token)))
        } else {
            Ok(None)
        }
    }

    /// Grabs the next lexer token if it satisfies the predicate function
    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(|t| predicate(&t))?;
        self.next().ok()
    }

    /// Grabs the next operator if it satisfies the type and precedence
    fn next_if_operator<O: Operator>(&mut self, min_prec: u8) -> Option<O> {
        let operator = self
            .peek()
            .unwrap_or(None)
            .and_then(|token| O::from(&token))
            .filter(|op| op.prec() >= min_prec)?;
        self.next().ok();
        Some(operator)
    }

    /// Grabs the next lexer token if it is a given token
    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
    }

    /// Peeks the next lexer token if any, but converts it from
    /// Option<Result<Token, Error>> to Result<Option<Token>, Error> which is
    /// more convenient to work with (the Iterator trait requires Option<T>).
    fn peek(&mut self) -> Result<Option<Token>, Error> {
        self.lexer.peek().cloned().transpose()
    }

    /// Parses an SQL statement
    fn parse_statement(&mut self) -> Result<ast::Statement, Error> {
        match self.peek()? {
            Some(Token::Keyword(Keyword::Select)) => self.parse_statement_select(),
            Some(token) => Err(Error::Parse(format!("Unexpected token {}", token))),
            None => Err(Error::Parse("Unexpected end of input".into())),
        }
    }

    /// Parses a select statement
    fn parse_statement_select(&mut self) -> Result<ast::Statement, Error> {
        Ok(ast::Statement::Select { select: self.parse_clause_select()?.unwrap() })
    }

    /// Parses a select clause
    fn parse_clause_select(&mut self) -> Result<Option<ast::SelectClause>, Error> {
        if self.next_if_token(Keyword::Select.into()).is_none() {
            return Ok(None);
        }
        let mut clause = ast::SelectClause { expressions: Vec::new(), labels: Vec::new() };
        loop {
            clause.expressions.push(self.parse_expression(0)?);
            clause.labels.push(match self.peek()? {
                Some(Token::Keyword(Keyword::As)) => {
                    self.next()?;
                    match self.next()? {
                        Token::Ident(ident) => Some(ident),
                        token => {
                            return Err(Error::Parse(format!(
                                "Expected identifier, found {}",
                                token
                            )))
                        }
                    }
                }
                Some(Token::Ident(ident)) => {
                    self.next()?;
                    Some(ident)
                }
                _ => None,
            });
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(Some(clause))
    }

    /// Parses an expression consisting of at least one atom operated on by any
    /// number of operators, using the precedence climbing algorithm.
    fn parse_expression(&mut self, min_prec: u8) -> Result<ast::Expression, Error> {
        let mut lhs = if let Some(prefix) = self.next_if_operator::<PrefixOperator>(min_prec) {
            prefix.build(self.parse_expression(prefix.prec() + prefix.assoc())?)
        } else {
            self.parse_expression_atom()?
        };
        while let Some(postfix) = self.next_if_operator::<PostfixOperator>(min_prec) {
            lhs = postfix.build(lhs)
        }
        while let Some(infix) = self.next_if_operator::<InfixOperator>(min_prec) {
            lhs = infix.build(lhs, self.parse_expression(infix.prec() + infix.assoc())?)
        }
        Ok(lhs)
    }

    /// Parses an expression atom
    fn parse_expression_atom(&mut self) -> Result<ast::Expression, Error> {
        Ok(match self.next()? {
            Token::Number(n) => {
                if n.chars().all(|c| c.is_digit(10)) {
                    ast::Literal::Integer(n.parse()?).into()
                } else {
                    ast::Literal::Float(n.parse()?).into()
                }
            }
            Token::String(s) => ast::Literal::String(s).into(),
            Token::Keyword(Keyword::False) => ast::Literal::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => ast::Literal::Null.into(),
            Token::Keyword(Keyword::True) => ast::Literal::Boolean(true).into(),
            t => return Err(Error::Parse(format!("Expected expression atom, found {}", t))),
        })
    }
}

/// An operator trait, to help with parsing of operators
trait Operator: Sized {
    /// Looks up the corresponding operator for a token, if one exists
    fn from(token: &Token) -> Option<Self>;
    /// Returns the operator's associativity
    fn assoc(&self) -> u8;
    /// Returns the operator's precedence
    fn prec(&self) -> u8;
}

const ASSOC_LEFT: u8 = 1;
const ASSOC_RIGHT: u8 = 0;

/// Prefix operators
enum PrefixOperator {
    Plus,
    Minus,
    Not,
}

impl PrefixOperator {
    fn build(&self, rhs: ast::Expression) -> ast::Expression {
        match self {
            Self::Plus => rhs,
            Self::Minus => ast::Operation::Negate(Box::new(rhs)).into(),
            Self::Not => ast::Operation::Not(Box::new(rhs)).into(),
        }
    }
}

impl Operator for PrefixOperator {
    fn from(token: &Token) -> Option<Self> {
        match token {
            Token::Exclamation => Some(Self::Not),
            Token::Minus => Some(Self::Minus),
            Token::Keyword(Keyword::Not) => Some(Self::Not),
            Token::Plus => Some(Self::Plus),
            _ => None,
        }
    }

    fn assoc(&self) -> u8 {
        ASSOC_RIGHT
    }

    fn prec(&self) -> u8 {
        9
    }
}

enum InfixOperator {
    Add,
    And,
    Divide,
    Equals,
    Exponentiate,
    GreatherThan,
    LesserThan,
    Modulo,
    Multiply,
    Or,
    Subtract,
}

impl InfixOperator {
    fn build(&self, lhs: ast::Expression, rhs: ast::Expression) -> ast::Expression {
        let (lhs, rhs) = (Box::new(lhs), Box::new(rhs));
        match self {
            Self::Add => ast::Operation::Add(lhs, rhs),
            Self::And => ast::Operation::And(lhs, rhs),
            Self::Divide => ast::Operation::Divide(lhs, rhs),
            Self::Equals => ast::Operation::Equals(lhs, rhs),
            Self::Exponentiate => ast::Operation::Exponentiate(lhs, rhs),
            Self::GreatherThan => ast::Operation::GreaterThan(lhs, rhs),
            Self::LesserThan => ast::Operation::LesserThan(lhs, rhs),
            Self::Modulo => ast::Operation::Modulo(lhs, rhs),
            Self::Multiply => ast::Operation::Multiply(lhs, rhs),
            Self::Or => ast::Operation::Or(lhs, rhs),
            Self::Subtract => ast::Operation::Subtract(lhs, rhs),
        }
        .into()
    }
}

impl Operator for InfixOperator {
    fn from(token: &Token) -> Option<Self> {
        Some(match token {
            Token::Plus => Self::Add,
            Token::Minus => Self::Subtract,
            Token::Asterisk => Self::Multiply,
            Token::Slash => Self::Divide,
            Token::Percent => Self::Modulo,
            Token::Caret => Self::Exponentiate,
            Token::Keyword(Keyword::And) => Self::And,
            Token::Keyword(Keyword::Or) => Self::Or,
            _ => return None,
        })
    }

    fn assoc(&self) -> u8 {
        match self {
            Self::Exponentiate => ASSOC_RIGHT,
            _ => ASSOC_LEFT,
        }
    }

    fn prec(&self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            Self::Equals => 3,
            Self::GreatherThan | Self::LesserThan => 4,
            Self::Add | Self::Subtract => 5,
            Self::Multiply | Self::Divide | Self::Modulo => 6,
            Self::Exponentiate => 7,
        }
    }
}

enum PostfixOperator {
    Factorial,
}

impl PostfixOperator {
    fn build(&self, lhs: ast::Expression) -> ast::Expression {
        match self {
            Self::Factorial => ast::Operation::Factorial(Box::new(lhs)),
        }
        .into()
    }
}

impl Operator for PostfixOperator {
    fn from(token: &Token) -> Option<Self> {
        match token {
            Token::Exclamation => Some(Self::Factorial),
            _ => None,
        }
    }

    fn assoc(&self) -> u8 {
        ASSOC_LEFT
    }

    fn prec(&self) -> u8 {
        8
    }
}
