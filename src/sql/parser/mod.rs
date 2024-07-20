//! Parses raw SQL strings into a structured Abstract Syntax Tree.

pub mod ast;
mod lexer;
mod parser;

pub use lexer::{is_ident, Keyword, Lexer, Token};
pub use parser::Parser;
