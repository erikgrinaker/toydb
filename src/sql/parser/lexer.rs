use std::fmt::Display;
use std::iter::Peekable;
use std::str::Chars;

use crate::errinput;
use crate::error::Result;

/// The lexer (lexical analyzer) preprocesses raw SQL strings into a sequence of
/// lexical tokens (e.g. keyword, number, string, etc), which are passed on to
/// the SQL parser. In doing so, it strips away basic syntactic noise such as
/// whitespace, case, and quotes, and performs initial symbol validation.
pub struct Lexer<'a> {
    chars: Peekable<Chars<'a>>,
}

/// A lexical token.
///
/// For simplicity, these carry owned String copies rather than &str references
/// into the original input string. This causes frequent allocations, but that's
/// fine for our purposes here.
#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    /// A numeric string, with digits, decimal points, and/or exponents. Leading
    /// signs (e.g. -) are separate tokens.
    Number(String),
    /// A Unicode string, with quotes stripped and escape sequences resolved.
    String(String),
    /// An identifier, with any quotes stripped.
    Ident(String),
    /// A SQL keyword.
    Keyword(Keyword),
    Period,             // .
    Equal,              // =
    NotEqual,           // !=
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    LessThan,           // <
    LessThanOrEqual,    // <=
    LessOrGreaterThan,  // <>
    Plus,               // +
    Minus,              // -
    Asterisk,           // *
    Slash,              // /
    Caret,              // ^
    Percent,            // %
    Exclamation,        // !
    Question,           // ?
    Comma,              // ,
    Semicolon,          // ;
    OpenParen,          // (
    CloseParen,         // )
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::Number(n) => n,
            Self::String(s) => s,
            Self::Ident(s) => s,
            Self::Keyword(k) => return k.fmt(f),
            Self::Period => ".",
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::LessOrGreaterThan => "<>",
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Asterisk => "*",
            Self::Slash => "/",
            Self::Caret => "^",
            Self::Percent => "%",
            Self::Exclamation => "!",
            Self::Question => "?",
            Self::Comma => ",",
            Self::Semicolon => ";",
            Self::OpenParen => "(",
            Self::CloseParen => ")",
        })
    }
}

impl From<Keyword> for Token {
    fn from(keyword: Keyword) -> Self {
        Self::Keyword(keyword)
    }
}

/// Reserved SQL keywords.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Keyword {
    And,
    As,
    Asc,
    Begin,
    Bool,
    Boolean,
    By,
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

impl TryFrom<&str> for Keyword {
    // Use a cheap static string, since this just indicates it's not a keyword.
    type Error = &'static str;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        // Only compare lowercase, which is enforced by the lexer. This avoids
        // allocating a string to change the case. Assert this.
        debug_assert!(value.chars().all(|c| !c.is_uppercase()), "keyword must be lowercase");
        Ok(match value {
            "as" => Self::As,
            "asc" => Self::Asc,
            "and" => Self::And,
            "begin" => Self::Begin,
            "bool" => Self::Bool,
            "boolean" => Self::Boolean,
            "by" => Self::By,
            "commit" => Self::Commit,
            "create" => Self::Create,
            "cross" => Self::Cross,
            "default" => Self::Default,
            "delete" => Self::Delete,
            "desc" => Self::Desc,
            "double" => Self::Double,
            "drop" => Self::Drop,
            "exists" => Self::Exists,
            "explain" => Self::Explain,
            "false" => Self::False,
            "float" => Self::Float,
            "from" => Self::From,
            "group" => Self::Group,
            "having" => Self::Having,
            "if" => Self::If,
            "index" => Self::Index,
            "infinity" => Self::Infinity,
            "inner" => Self::Inner,
            "insert" => Self::Insert,
            "int" => Self::Int,
            "integer" => Self::Integer,
            "into" => Self::Into,
            "is" => Self::Is,
            "join" => Self::Join,
            "key" => Self::Key,
            "left" => Self::Left,
            "like" => Self::Like,
            "limit" => Self::Limit,
            "nan" => Self::NaN,
            "not" => Self::Not,
            "null" => Self::Null,
            "of" => Self::Of,
            "offset" => Self::Offset,
            "on" => Self::On,
            "only" => Self::Only,
            "or" => Self::Or,
            "order" => Self::Order,
            "outer" => Self::Outer,
            "primary" => Self::Primary,
            "read" => Self::Read,
            "references" => Self::References,
            "right" => Self::Right,
            "rollback" => Self::Rollback,
            "select" => Self::Select,
            "set" => Self::Set,
            "string" => Self::String,
            "system" => Self::System,
            "table" => Self::Table,
            "text" => Self::Text,
            "time" => Self::Time,
            "transaction" => Self::Transaction,
            "true" => Self::True,
            "unique" => Self::Unique,
            "update" => Self::Update,
            "values" => Self::Values,
            "varchar" => Self::Varchar,
            "where" => Self::Where,
            "write" => Self::Write,
            _ => return Err("not a keyword"),
        })
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Display keywords as uppercase.
        f.write_str(match self {
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::And => "AND",
            Self::Begin => "BEGIN",
            Self::Bool => "BOOL",
            Self::Boolean => "BOOLEAN",
            Self::By => "BY",
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
        })
    }
}

/// The lexer is used as a token iterator.
impl Iterator for Lexer<'_> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Result<Token>> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            // If there's any remaining chars, the lexer didn't recognize them.
            // Otherwise, we're done lexing.
            Ok(None) => self.chars.peek().map(|c| errinput!("unexpected character {c}")),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Lexer<'a> {
    /// Creates a new lexer for the given string.
    pub fn new(input: &'a str) -> Lexer<'a> {
        Lexer { chars: input.chars().peekable() }
    }

    /// Returns the next character if it satisfies the predicate.
    fn next_if(&mut self, predicate: impl Fn(char) -> bool) -> Option<char> {
        self.chars.peek().filter(|&&c| predicate(c))?;
        self.chars.next()
    }

    /// Applies a function to the next character, returning its result and
    /// consuming the next character if it's Some.
    fn next_if_map<T>(&mut self, map: impl Fn(char) -> Option<T>) -> Option<T> {
        let value = self.chars.peek().and_then(|&c| map(c))?;
        self.chars.next();
        Some(value)
    }

    /// Returns true if the next character is the given character, consuming it.
    fn next_is(&mut self, c: char) -> bool {
        self.next_if(|n| n == c).is_some()
    }

    /// Scans the next token, if any.
    fn scan(&mut self) -> Result<Option<Token>> {
        // Ignore whitespace.
        self.skip_whitespace();
        // The first character tells us the token type.
        match self.chars.peek() {
            Some('\'') => self.scan_string(),
            Some('"') => self.scan_ident_quoted(),
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()),
            Some(c) if c.is_alphabetic() => Ok(self.scan_ident_or_keyword()),
            Some(_) => Ok(self.scan_symbol()),
            None => Ok(None),
        }
    }

    /// Scans the next identifier or keyword, if any. It's converted to
    /// lowercase, by SQL convention.
    fn scan_ident_or_keyword(&mut self) -> Option<Token> {
        // The first character must be alphabetic. The rest can be numeric.
        let mut name = self.next_if(|c| c.is_alphabetic())?.to_lowercase().to_string();
        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
            name.extend(c.to_lowercase())
        }
        // Check if the identifier matches a keyword.
        match Keyword::try_from(name.as_str()).ok() {
            Some(keyword) => Some(Token::Keyword(keyword)),
            None => Some(Token::Ident(name)),
        }
    }

    /// Scans the next quoted identifier, if any. Case is preserved.
    fn scan_ident_quoted(&mut self) -> Result<Option<Token>> {
        if !self.next_is('"') {
            return Ok(None);
        }
        let mut ident = String::new();
        loop {
            match self.chars.next() {
                // "" is the escape sequence for ".
                Some('"') if self.next_is('"') => ident.push('"'),
                Some('"') => break,
                Some(c) => ident.push(c),
                None => return errinput!("unexpected end of quoted identifier"),
            }
        }
        Ok(Some(Token::Ident(ident)))
    }

    /// Scans the next number, if any.
    fn scan_number(&mut self) -> Option<Token> {
        // Scan the integer part. There must be one digit.
        let mut number = self.next_if(|c| c.is_ascii_digit())?.to_string();
        while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
            number.push(c)
        }
        // Scan the fractional part, if any.
        if self.next_is('.') {
            number.push('.');
            while let Some(dec) = self.next_if(|c| c.is_ascii_digit()) {
                number.push(dec)
            }
        }
        // Scan the exponent, if any.
        if let Some(exp) = self.next_if(|c| c == 'e' || c == 'E') {
            number.push(exp);
            if let Some(sign) = self.next_if(|c| c == '+' || c == '-') {
                number.push(sign)
            }
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                number.push(c)
            }
        }
        Some(Token::Number(number))
    }

    /// Scans the next quoted string literal, if any.
    fn scan_string(&mut self) -> Result<Option<Token>> {
        if !self.next_is('\'') {
            return Ok(None);
        }
        let mut string = String::new();
        loop {
            match self.chars.next() {
                // '' is the escape sequence for '.
                Some('\'') if self.next_is('\'') => string.push('\''),
                Some('\'') => break,
                Some(c) => string.push(c),
                None => return errinput!("unexpected end of string literal"),
            }
        }
        Ok(Some(Token::String(string)))
    }

    /// Scans the next symbol token, if any.
    fn scan_symbol(&mut self) -> Option<Token> {
        let mut token = self.next_if_map(|c| {
            Some(match c {
                '.' => Token::Period,
                '=' => Token::Equal,
                '>' => Token::GreaterThan,
                '<' => Token::LessThan,
                '+' => Token::Plus,
                '-' => Token::Minus,
                '*' => Token::Asterisk,
                '/' => Token::Slash,
                '^' => Token::Caret,
                '%' => Token::Percent,
                '!' => Token::Exclamation,
                '?' => Token::Question,
                ',' => Token::Comma,
                ';' => Token::Semicolon,
                '(' => Token::OpenParen,
                ')' => Token::CloseParen,
                _ => return None,
            })
        })?;
        // Handle two-character tokens, e.g. !=.
        token = match token {
            Token::Exclamation if self.next_is('=') => Token::NotEqual,
            Token::GreaterThan if self.next_is('=') => Token::GreaterThanOrEqual,
            Token::LessThan if self.next_is('>') => Token::LessOrGreaterThan,
            Token::LessThan if self.next_is('=') => Token::LessThanOrEqual,
            token => token,
        };
        Some(token)
    }

    /// Skips any whitespace.
    fn skip_whitespace(&mut self) {
        while self.next_if(|c| c.is_whitespace()).is_some() {}
    }
}

/// Returns true if the entire given string is a single valid identifier.
pub fn is_ident(ident: &str) -> bool {
    let mut lexer = Lexer::new(ident);
    let Some(Ok(Token::Ident(_))) = lexer.next() else {
        return false;
    };
    lexer.next().is_none() // if further tokens, it's not a lone identifier
}
