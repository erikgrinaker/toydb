pub mod engine;
pub mod execution;
pub mod parser;
pub mod planner;
pub mod types;

#[cfg(test)]
mod tests {
    use crate::sql::engine::{Engine, Local};
    use crate::sql::planner::{Planner, Scope};
    use crate::sql::types::Value;
    use crate::storage;
    use std::error::Error;
    use std::fmt::Write as _;
    use std::result::Result;
    use test_each_file::test_each_path;

    use super::parser::Parser;

    // Run goldenscript tests in src/sql/testscripts.
    test_each_path! { in "src/sql/testscripts/expressions" as expressions => test_goldenscript }

    fn test_goldenscript(path: &std::path::Path) {
        goldenscript::run(&mut ExpressionRunner::new(), path).expect("goldenscript failed")
    }

    struct ExpressionRunner {
        engine: Local<storage::Memory>,
    }

    type Catalog<'a> = <Local<storage::Memory> as Engine<'a>>::Transaction;

    impl ExpressionRunner {
        fn new() -> Self {
            let engine = Local::new(storage::Memory::new());
            Self { engine }
        }
    }

    impl goldenscript::Runner for ExpressionRunner {
        fn run(&mut self, command: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
            // The entire command is the expression to evaluate. There are no args.
            if !command.args.is_empty() {
                return Err("expressions should be given as a command with no args".into());
            }
            let input = &command.name;
            let mut tags = command.tags.clone();

            // Evaluate the expression.
            let mut output = String::new();
            let mut session = self.engine.session();
            let value: Value = session.execute(&format!("SELECT {input}"))?.try_into()?;
            write!(output, "{value:?}")?;

            // If requested, parse and dump the expression.
            if tags.remove("expr") {
                let ast = Parser::new(input).parse_expression()?;
                let expr = Planner::<Catalog>::build_expression(ast, &Scope::new())?;
                write!(output, " ← {expr:?}")?;
            }

            // Reject unknown tags.
            if let Some(tag) = tags.iter().next() {
                return Err(format!("unknown tag {tag}").into());
            }

            writeln!(output)?;
            Ok(output)
        }
    }
}
