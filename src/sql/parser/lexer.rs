use crate::error::{Error, Result};

use std::iter::Peekable;
use std::str::Chars;

// A lexer token
#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    Number(String),
    String(String),
    Ident(String),
    Keyword(Keyword),
    Period,
    Equal,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    LessOrGreaterThan,
    Plus,
    Minus,
    Asterisk,
    Slash,
    Caret,
    Percent,
    Exclamation,
    NotEqual,
    Question,
    OpenParen,
    CloseParen,
    Comma,
    Semicolon,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Token::Number(n) => n,
            Token::String(s) => s,
            Token::Ident(s) => s,
            Token::Keyword(k) => k.to_str(),
            Token::Period => ".",
            Token::Equal => "=",
            Token::GreaterThan => ">",
            Token::GreaterThanOrEqual => ">=",
            Token::LessThan => "<",
            Token::LessThanOrEqual => "<=",
            Token::LessOrGreaterThan => "<>",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Asterisk => "*",
            Token::Slash => "/",
            Token::Caret => "^",
            Token::Percent => "%",
            Token::Exclamation => "!",
            Token::NotEqual => "!=",
            Token::Question => "?",
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
        })
    }
}

impl From<Keyword> for Token {
    fn from(keyword: Keyword) -> Self {
        Self::Keyword(keyword)
    }
}

/// Lexer keywords
#[derive(Clone, Debug, PartialEq)]
pub enum Keyword {
    And,
    As,
    Asc,
    Begin,
    Bool,
    Boolean,
    By,
    Char,
    Commit,
    Create,
    Cross,
    Default,
    Delete,
    Desc,
    Double,
    Drop,
    Exists,
    Explain,
    False,
    Float,
    From,
    Group,
    Having,
    If,
    Index,
    Infinity,
    Inner,
    Insert,
    Int,
    Integer,
    Into,
    Is,
    Join,
    Key,
    Left,
    Like,
    Limit,
    NaN,
    Not,
    Null,
    Of,
    Offset,
    On,
    Only,
    Or,
    Order,
    Outer,
    Primary,
    Read,
    References,
    Right,
    Rollback,
    Select,
    Set,
    String,
    System,
    Table,
    Text,
    Time,
    Transaction,
    True,
    Unique,
    Update,
    Values,
    Varchar,
    Where,
    Write,
}

impl Keyword {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(ident: &str) -> Option<Self> {
        Some(match ident.to_uppercase().as_ref() {
            "AS" => Self::As,
            "ASC" => Self::Asc,
            "AND" => Self::And,
            "BEGIN" => Self::Begin,
            "BOOL" => Self::Bool,
            "BOOLEAN" => Self::Boolean,
            "BY" => Self::By,
            "CHAR" => Self::Char,
            "COMMIT" => Self::Commit,
            "CREATE" => Self::Create,
            "CROSS" => Self::Cross,
            "DEFAULT" => Self::Default,
            "DELETE" => Self::Delete,
            "DESC" => Self::Desc,
            "DOUBLE" => Self::Double,
            "DROP" => Self::Drop,
            "EXISTS" => Self::Exists,
            "EXPLAIN" => Self::Explain,
            "FALSE" => Self::False,
            "FLOAT" => Self::Float,
            "FROM" => Self::From,
            "GROUP" => Self::Group,
            "HAVING" => Self::Having,
            "IF" => Self::If,
            "INDEX" => Self::Index,
            "INFINITY" => Self::Infinity,
            "INNER" => Self::Inner,
            "INSERT" => Self::Insert,
            "INT" => Self::Int,
            "INTEGER" => Self::Integer,
            "INTO" => Self::Into,
            "IS" => Self::Is,
            "JOIN" => Self::Join,
            "KEY" => Self::Key,
            "LEFT" => Self::Left,
            "LIKE" => Self::Like,
            "LIMIT" => Self::Limit,
            "NAN" => Self::NaN,
            "NOT" => Self::Not,
            "NULL" => Self::Null,
            "OF" => Self::Of,
            "OFFSET" => Self::Offset,
            "ON" => Self::On,
            "ONLY" => Self::Only,
            "OR" => Self::Or,
            "ORDER" => Self::Order,
            "OUTER" => Self::Outer,
            "PRIMARY" => Self::Primary,
            "READ" => Self::Read,
            "REFERENCES" => Self::References,
            "RIGHT" => Self::Right,
            "ROLLBACK" => Self::Rollback,
            "SELECT" => Self::Select,
            "SET" => Self::Set,
            "STRING" => Self::String,
            "SYSTEM" => Self::System,
            "TABLE" => Self::Table,
            "TEXT" => Self::Text,
            "TIME" => Self::Time,
            "TRANSACTION" => Self::Transaction,
            "TRUE" => Self::True,
            "UNIQUE" => Self::Unique,
            "UPDATE" => Self::Update,
            "VALUES" => Self::Values,
            "VARCHAR" => Self::Varchar,
            "WHERE" => Self::Where,
            "WRITE" => Self::Write,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::And => "AND",
            Self::Begin => "BEGIN",
            Self::Bool => "BOOL",
            Self::Boolean => "BOOLEAN",
            Self::By => "BY",
            Self::Char => "CHAR",
            Self::Commit => "COMMIT",
            Self::Create => "CREATE",
            Self::Cross => "CROSS",
            Self::Default => "DEFAULT",
            Self::Delete => "DELETE",
            Self::Desc => "DESC",
            Self::Double => "DOUBLE",
            Self::Drop => "DROP",
            Self::Exists => "EXISTS",
            Self::Explain => "EXPLAIN",
            Self::False => "FALSE",
            Self::Float => "FLOAT",
            Self::From => "FROM",
            Self::Group => "GROUP",
            Self::Having => "HAVING",
            Self::If => "IF",
            Self::Index => "INDEX",
            Self::Infinity => "INFINITY",
            Self::Inner => "INNER",
            Self::Insert => "INSERT",
            Self::Int => "INT",
            Self::Integer => "INTEGER",
            Self::Into => "INTO",
            Self::Is => "IS",
            Self::Join => "JOIN",
            Self::Key => "KEY",
            Self::Left => "LEFT",
            Self::Like => "LIKE",
            Self::Limit => "LIMIT",
            Self::NaN => "NAN",
            Self::Not => "NOT",
            Self::Null => "NULL",
            Self::Of => "OF",
            Self::Offset => "OFFSET",
            Self::On => "ON",
            Self::Only => "ONLY",
            Self::Outer => "OUTER",
            Self::Or => "OR",
            Self::Order => "ORDER",
            Self::Primary => "PRIMARY",
            Self::Read => "READ",
            Self::References => "REFERENCES",
            Self::Right => "RIGHT",
            Self::Rollback => "ROLLBACK",
            Self::Select => "SELECT",
            Self::Set => "SET",
            Self::String => "STRING",
            Self::System => "SYSTEM",
            Self::Table => "TABLE",
            Self::Text => "TEXT",
            Self::Time => "TIME",
            Self::Transaction => "TRANSACTION",
            Self::True => "TRUE",
            Self::Unique => "UNIQUE",
            Self::Update => "UPDATE",
            Self::Values => "VALUES",
            Self::Varchar => "VARCHAR",
            Self::Where => "WHERE",
            Self::Write => "WRITE",
        }
    }
}

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

/// A lexer tokenizes an input string as an iterator
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Result<Token>> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => {
                self.iter.peek().map(|c| Err(Error::Parse(format!("Unexpected character {}", c))))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Lexer<'a> {
    /// Creates a new lexer for the given input string
    #[allow(dead_code)]
    pub fn new(input: &'a str) -> Lexer<'a> {
        Lexer { iter: input.chars().peekable() }
    }

    /// Consumes any whitespace characters
    fn consume_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    /// Grabs the next character if it matches the predicate function
    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        self.iter.peek().filter(|&c| predicate(*c))?;
        self.iter.next()
    }

    /// Grabs the next single-character token if the tokenizer function returns one
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, tokenizer: F) -> Option<Token> {
        let token = self.iter.peek().and_then(|&c| tokenizer(c))?;
        self.iter.next();
        Some(token)
    }

    /// Grabs the next characters that match the predicate, as a string
    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String> {
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate) {
            value.push(c)
        }
        Some(value).filter(|v| !v.is_empty())
    }

    /// Scans the input for the next token if any, ignoring leading whitespace
    fn scan(&mut self) -> Result<Option<Token>> {
        self.consume_whitespace();
        match self.iter.peek() {
            Some('\'') => self.scan_string(),
            Some('"') => self.scan_ident_quoted(),
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()),
            Some(c) if c.is_alphabetic() => Ok(self.scan_ident()),
            Some(_) => Ok(self.scan_symbol()),
            None => Ok(None),
        }
    }

    /// Scans the input for the next ident or keyword token, if any
    fn scan_ident(&mut self) -> Option<Token> {
        let mut name = self.next_if(|c| c.is_alphabetic())?.to_string();
        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
            name.push(c)
        }
        Keyword::from_str(&name)
            .map(Token::Keyword)
            .or_else(|| Some(Token::Ident(name.to_lowercase())))
    }

    /// Scans the input for the next quoted ident, if any
    fn scan_ident_quoted(&mut self) -> Result<Option<Token>> {
        if self.next_if(|c| c == '"').is_none() {
            return Ok(None);
        }
        let mut ident = String::new();
        loop {
            match self.iter.next() {
                Some('"') if self.next_if(|c| c == '"').is_some() => ident.push('"'),
                Some('"') => break,
                Some(c) => ident.push(c),
                None => return Err(Error::Parse("Unexpected end of quoted identifier".into())),
            }
        }
        Ok(Some(Token::Ident(ident)))
    }

    /// Scans the input for the next number token, if any
    fn scan_number(&mut self) -> Option<Token> {
        let mut num = self.next_while(|c| c.is_ascii_digit())?;
        if let Some(sep) = self.next_if(|c| c == '.') {
            num.push(sep);
            while let Some(dec) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(dec)
            }
        }
        if let Some(exp) = self.next_if(|c| c == 'e' || c == 'E') {
            num.push(exp);
            if let Some(sign) = self.next_if(|c| c == '+' || c == '-') {
                num.push(sign)
            }
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(c)
            }
        }
        Some(Token::Number(num))
    }

    /// Scans the input for the next string literal, if any
    fn scan_string(&mut self) -> Result<Option<Token>> {
        if self.next_if(|c| c == '\'').is_none() {
            return Ok(None);
        }
        let mut s = String::new();
        loop {
            match self.iter.next() {
                Some('\'') if self.next_if(|c| c == '\'').is_some() => s.push('\''),
                Some('\'') => break,
                Some(c) => s.push(c),
                None => return Err(Error::Parse("Unexpected end of string literal".into())),
            }
        }
        Ok(Some(Token::String(s)))
    }

    /// Scans the input for the next symbol token, if any, and
    /// handle any multi-symbol tokens
    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            '.' => Some(Token::Period),
            '=' => Some(Token::Equal),
            '>' => Some(Token::GreaterThan),
            '<' => Some(Token::LessThan),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '*' => Some(Token::Asterisk),
            '/' => Some(Token::Slash),
            '^' => Some(Token::Caret),
            '%' => Some(Token::Percent),
            '!' => Some(Token::Exclamation),
            '?' => Some(Token::Question),
            '(' => Some(Token::OpenParen),
            ')' => Some(Token::CloseParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            _ => None,
        })
        .map(|token| match token {
            Token::Exclamation => {
                if self.next_if(|c| c == '=').is_some() {
                    Token::NotEqual
                } else {
                    token
                }
            }
            Token::LessThan => {
                if self.next_if(|c| c == '>').is_some() {
                    Token::LessOrGreaterThan
                } else if self.next_if(|c| c == '=').is_some() {
                    Token::LessThanOrEqual
                } else {
                    token
                }
            }
            Token::GreaterThan => {
                if self.next_if(|c| c == '=').is_some() {
                    Token::GreaterThanOrEqual
                } else {
                    token
                }
            }
            _ => token,
        })
    }
}
