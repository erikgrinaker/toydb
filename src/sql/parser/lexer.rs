use crate::Error;
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
    Equals,
    GreaterThan,
    LessThan,
    Plus,
    Minus,
    Asterisk,
    Slash,
    Caret,
    Percent,
    Exclamation,
    Question,
    OpenParen,
    CloseParen,
    Comma,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Token::Number(n) => n,
            Token::String(s) => s,
            Token::Ident(s) => s,
            Token::Keyword(k) => k.to_str(),
            Token::Period => ".",
            Token::Equals => "=",
            Token::GreaterThan => ">",
            Token::LessThan => "<",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Asterisk => "*",
            Token::Slash => "/",
            Token::Caret => "^",
            Token::Percent => "%",
            Token::Exclamation => "!",
            Token::Question => "?",
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
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
    As,
    Select,
}

impl Keyword {
    fn from_str(ident: &str) -> Option<Self> {
        Some(match ident.to_uppercase().as_ref() {
            "AS" => Self::As,
            "SELECT" => Self::Select,
            _ => return None,
        })
    }

    fn to_str(&self) -> &str {
        match self {
            Self::As => "AS",
            Self::Select => "SELECT",
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
    type Item = Result<Token, Error>;

    fn next(&mut self) -> Option<Result<Token, Error>> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => match self.iter.peek() {
                Some(c) => Some(Err(Error::Parse(format!("Unexpected character {}", c)))),
                None => None,
            },
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
    fn scan(&mut self) -> Result<Option<Token>, Error> {
        self.consume_whitespace();
        match self.iter.peek() {
            Some('\'') => self.scan_string(),
            Some(c) if c.is_digit(10) => Ok(self.scan_number()),
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
        Keyword::from_str(&name).map(Token::Keyword).or(Some(Token::Ident(name)))
    }

    /// Scans the input for the next number token, if any
    fn scan_number(&mut self) -> Option<Token> {
        let mut num = self.next_while(|c| c.is_digit(10))?;
        if let Some(sep) = self.next_if(|c| c == '.') {
            num.push(sep);
            while let Some(dec) = self.next_if(|c| c.is_digit(10)) {
                num.push(dec)
            }
        }
        if let Some(exp) = self.next_if(|c| c == 'e' || c == 'E') {
            num.push(exp);
            if let Some(sign) = self.next_if(|c| c == '+' || c == '-') {
                num.push(sign)
            }
            while let Some(c) = self.next_if(|c| c.is_digit(10)) {
                num.push(c)
            }
        }
        Some(Token::Number(num))
    }

    /// Scans the input for the next string literal, if any
    fn scan_string(&mut self) -> Result<Option<Token>, Error> {
        if self.next_if(|c| c == '\'').is_none() {
            return Ok(None);
        }
        let mut s = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => {
                    if let Some(c) = self.next_if(|c| c == '\'') {
                        s.push(c)
                    } else {
                        break;
                    }
                }
                Some(c) => s.push(c),
                None => return Err(Error::Parse("Unexpected end of string literal".into())),
            }
        }
        Ok(Some(Token::String(s)))
    }

    /// Scans the input for the next symbol token, if any
    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            '.' => Some(Token::Period),
            '=' => Some(Token::Equals),
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
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_scan(input: &str, expect: Vec<Token>) {
        let actual: Vec<Token> = Lexer::new(input).map(|r| r.unwrap()).collect();
        assert_eq!(expect, actual);
    }

    #[test]
    fn literal_string() {
        assert_scan(
            r#"A 'literal string with ''single'' and "double" quotes inside 😀'."#,
            vec![
                Token::Ident("A".into()),
                Token::String(
                    r#"literal string with 'single' and "double" quotes inside 😀"#.into(),
                ),
                Token::Period,
            ],
        );
    }

    #[test]
    fn literal_number() {
        assert_scan(
            "0 1 3.14 293. -2.718 3.14e3 2.718E-2",
            vec![
                Token::Number("0".into()),
                Token::Number("1".into()),
                Token::Number("3.14".into()),
                Token::Number("293.".into()),
                Token::Minus,
                Token::Number("2.718".into()),
                Token::Number("3.14e3".into()),
                Token::Number("2.718E-2".into()),
            ],
        )
    }

    #[test]
    fn select() {
        use super::Keyword;
        use Token::*;
        assert_scan(
            "
            SELECT artist.name, album.name, EXTRACT(YEAR FROM NOW()) - album.release_year AS age
            FROM artist INNER JOIN album ON album.artist_id = artist.id
            WHERE album.genre != 'country' AND album.release_year >= 1980
            ORDER BY artist.name ASC, age DESC",
            vec![
                Keyword::Select.into(),
                Ident("artist".into()),
                Period,
                Ident("name".into()),
                Comma,
                Ident("album".into()),
                Period,
                Ident("name".into()),
                Comma,
                Ident("EXTRACT".into()),
                OpenParen,
                Ident("YEAR".into()),
                Ident("FROM".into()),
                Ident("NOW".into()),
                OpenParen,
                CloseParen,
                CloseParen,
                Minus,
                Ident("album".into()),
                Period,
                Ident("release_year".into()),
                Keyword::As.into(),
                Ident("age".into()),
                Ident("FROM".into()),
                Ident("artist".into()),
                Ident("INNER".into()),
                Ident("JOIN".into()),
                Ident("album".into()),
                Ident("ON".into()),
                Ident("album".into()),
                Period,
                Ident("artist_id".into()),
                Equals,
                Ident("artist".into()),
                Period,
                Ident("id".into()),
                Ident("WHERE".into()),
                Ident("album".into()),
                Period,
                Ident("genre".into()),
                Exclamation,
                Equals,
                String("country".into()),
                Ident("AND".into()),
                Ident("album".into()),
                Period,
                Ident("release_year".into()),
                GreaterThan,
                Equals,
                Number("1980".into()),
                Ident("ORDER".into()),
                Ident("BY".into()),
                Ident("artist".into()),
                Period,
                Ident("name".into()),
                Ident("ASC".into()),
                Comma,
                Ident("age".into()),
                Ident("DESC".into()),
            ],
        )
    }
}
