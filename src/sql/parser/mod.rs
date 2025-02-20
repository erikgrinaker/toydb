//! Parses raw SQL strings into a structured Abstract Syntax Tree.

pub mod ast;
mod lexer;
mod parser;

pub use lexer::{Keyword, Lexer, Token, is_ident};
pub use parser::Parser;
