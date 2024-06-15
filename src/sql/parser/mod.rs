pub mod ast;
mod lexer;
mod parser;

pub use lexer::{Keyword, Lexer, Token};
pub use parser::Parser;

use regex::Regex;

// Formats an identifier by quoting it as appropriate.
// TODO: move this elsewhere.
pub(super) fn format_ident(ident: &str) -> String {
    static RE_IDENT: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re_ident = RE_IDENT.get_or_init(|| Regex::new(r#"^\w[\w_]*$"#).unwrap());

    if re_ident.is_match(ident) && Keyword::try_from(ident.to_lowercase().as_str()).is_err() {
        ident.to_string()
    } else {
        format!("\"{}\"", ident.replace('\"', "\"\""))
    }
}
