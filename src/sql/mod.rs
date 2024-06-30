pub mod engine;
pub mod execution;
pub mod parser;
pub mod planner;
pub mod types;

#[cfg(test)]
mod tests {
    use crate::encoding::format::{self, Formatter as _};
    use crate::sql::engine::{Engine, Local, StatementResult};
    use crate::sql::planner::{Planner, Scope};
    use crate::sql::types::Value;
    use crate::storage::engine::test::{Emit, Mirror, Operation};
    use crate::storage::{self, Engine as _};
    use crossbeam::channel::Receiver;
    use itertools::Itertools as _;
    use std::error::Error;
    use std::fmt::Write as _;
    use std::result::Result;
    use test_each_file::test_each_path;

    use super::engine::{Catalog as _, Session};
    use super::parser::Parser;

    // Run goldenscript tests in src/sql/testscripts.
    test_each_path! { in "src/sql/testscripts/schema" as schema => test_goldenscript }
    test_each_path! { in "src/sql/testscripts/writes" as writes => test_goldenscript }
    test_each_path! { in "src/sql/testscripts/expressions" as expressions => test_goldenscript_expr }

    fn test_goldenscript(path: &std::path::Path) {
        // Since the runner's Session can't reference an Engine stored in the
        // same struct, we borrow the engine. Use both a BitCask and a Memory
        // engine, and mirror operations across them. Emit engine operations to
        // op_rx.
        let (op_tx, op_rx) = crossbeam::channel::unbounded();
        let tempdir = tempfile::TempDir::with_prefix("toydb").expect("tempdir failed");
        let bitcask =
            storage::BitCask::new(tempdir.path().join("bitcask")).expect("bitcask failed");
        let memory = storage::Memory::new();
        let engine = Local::new(Emit::new(Mirror::new(bitcask, memory), op_tx));
        let mut runner = SQLRunner::new(&engine, op_rx);

        goldenscript::run(&mut runner, path).expect("goldenscript failed")
    }

    fn test_goldenscript_expr(path: &std::path::Path) {
        goldenscript::run(&mut ExpressionRunner::new(), path).expect("goldenscript failed")
    }

    /// A SQL test runner.
    struct SQLRunner<'a> {
        engine: &'a TestEngine,
        session: Session<'a, TestEngine>,
        op_rx: Receiver<Operation>,
    }

    type TestEngine = Local<Emit<Mirror<storage::BitCask, storage::Memory>>>;

    impl<'a> SQLRunner<'a> {
        fn new(engine: &'a TestEngine, op_rx: Receiver<Operation>) -> Self {
            let session = engine.session();
            Self { engine, session, op_rx }
        }
    }

    impl<'a> goldenscript::Runner for SQLRunner<'a> {
        fn run(&mut self, command: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
            let mut output = String::new();

            // Handle runner commands.
            match command.name.as_str() {
                // dump
                "dump" => {
                    command.consume_args().reject_rest()?;
                    let mut engine = self.engine.mvcc.engine.lock().expect("mutex failed");
                    let mut iter = engine.scan(..);
                    while let Some((key, value)) = iter.next().transpose()? {
                        writeln!(
                            output,
                            "{} [{}]",
                            format::MVCC::<format::SQL>::key_value(&key, &value),
                            format::Raw::key_value(&key, &value)
                        )?;
                    }
                    return Ok(output);
                }

                // schema [TABLE...]
                "schema" => {
                    let mut args = command.consume_args();
                    let tables = args.rest_pos().iter().map(|arg| arg.value.clone()).collect_vec();
                    args.reject_rest()?;

                    let schemas = if tables.is_empty() {
                        self.session.with_txn(true, |txn| txn.list_tables())?
                    } else {
                        tables
                            .into_iter()
                            .map(|t| self.session.with_txn(true, |txn| txn.must_get_table(&t)))
                            .collect::<Result<_, _>>()?
                    };
                    return Ok(schemas.into_iter().map(|s| s.to_string()).join("\n"));
                }

                // Otherwise, fall through to SQL execution.
                _ => {}
            }

            // The entire command is the statement to execute. There are no args.
            if !command.args.is_empty() {
                return Err("expressions should be given as a command with no args".into());
            }
            let input = &command.name;
            let mut tags = command.tags.clone();

            // Execute the statement.
            let result = self.session.execute(input)?;

            // Output the plan if requested.
            if tags.remove("plan") {
                let query = format!("EXPLAIN {input}");
                let StatementResult::Explain(plan) = self.session.execute(&query)? else {
                    return Err("unexpected explain response".into());
                };
                writeln!(output, "{plan}")?;
            }

            // Output the result if requested. SELECT results are always output,
            // but the columns only if result is given.
            if let StatementResult::Select { columns, rows } = result {
                if tags.remove("columns") {
                    writeln!(output, "{}", columns.into_iter().map(|c| c.to_string()).join(","))?;
                }
                for row in rows {
                    writeln!(output, "{}", row.into_iter().map(|v| v.to_string()).join(","))?;
                }
            } else if tags.remove("result") {
                writeln!(output, "{result:?}")?;
            }

            // Output engine ops if requested.
            if tags.remove("ops") {
                while let Ok(op) = self.op_rx.try_recv() {
                    match op {
                        Operation::Delete { key } => writeln!(
                            output,
                            "storage delete {} [{}]",
                            format::MVCC::<format::SQL>::key(&key),
                            format::Raw::key(&key),
                        )?,
                        Operation::Flush => writeln!(output, "storage flush")?,
                        Operation::Set { key, value } => writeln!(
                            output,
                            "storage set {} [{}]",
                            format::MVCC::<format::SQL>::key_value(&key, &value),
                            format::Raw::key_value(&key, &value),
                        )?,
                    }
                }
            }

            // Reject unknown tags.
            if let Some(tag) = tags.iter().next() {
                return Err(format!("unknown tag {tag}").into());
            }

            Ok(output)
        }

        /// Drain unprocessed operations after each command.
        fn end_command(&mut self, _: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
            while self.op_rx.try_recv().is_ok() {}
            Ok(String::new())
        }
    }

    /// A test runner for expressions specifically. Evaluates expressions to
    /// values, and can optionally emit the expression tree.
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
