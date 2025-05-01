//! Implements a SQL execution engine. A SQL statement flows through the engine
//! as follows:
//!
//! 1. The `toySQL` client connects to the server, which creates a new
//!    `sql::execution::Session` in `Server::sql_session`.
//!
//! 2. `toySQL` submits a SQL `SELECT` string, which the server executes via
//!     `Session::execute`.
//!
//! 3. `Session::execute` calls `Parser::parse` to parse the SQL `SELECT` string
//!     into an `ast::Statement::Select` AST (Abstract Syntax Tree). The parser
//!     uses the `Lexer` for initial tokenization.
//!     
//! 4. `Session::execute` obtains a new read-only `sql::engine::Transaction` via
//!    `Session::with_txn`. We'll gloss over the details here.
//!
//! 5. `Session::execute` calls `Plan::build` to construct an execution plan
//!    from the AST via the `Planner`, using the `Transaction`'s
//!    `sql::engine::Catalog` trait to look up table schema information.
//!
//! 6. `Session::execute` calls `Plan::optimize` to optimize the execution plan
//!    via the optimizers in `sql::planner::optimizer`. This e.g. performs
//!    filter pushdown to filter rows during storage scans, uses secondary
//!    indexes where appropriate, and chooses more efficient join types.
//!
//! 7. `Session::execute` calls `Plan::execute` to actually execute the plan,
//!    using the `Transaction` to access the `sql::engine::Engine`.  It uses the
//!    executors in `sql::execution` to recursively execute the
//!    `sql::planner::Node` nodes, which stream and process `sql::types::Row`
//!    vectors via `sql::types::Rows` iterators.
//!
//! 8. At the tip of the execution plan there's typically a `Node::Scan` which
//!    performs full table scans from storage. It is executed by
//!    `sql::execution::source::scan`, which calls `Transaction::scan`.
//!
//! 9. The upper `sql::engine::Raft` engine submits a `Read::Scan` request to
//!    Raft via `Raft::read` and `Raft::execute`. This is submitted through the
//!    crossbeam channel `Raft::tx`, which is routed to the local Raft node in
//!    `Server::raft_route` via `raft::Node::step`.
//!
//! 10. We'll skip Raft details, but see the `raft` module documentation. The
//!     `Read::Scan` request eventually makes its way to the SQL state machine
//!     `sql::engine::raft::State` that's managed by Raft. Since this is a read
//!     request, it is executed only on the leader node, calling `State::read`.
//!
//! 11. `State` wraps the `sql::engine::Local` SQL execution engine that runs
//!     on each node, using local storage. `State::read` calls
//!     `Transaction::scan` using a `Local::Transaction`.
//!
//! 12. The `Local` engine uses a `storage::BitCask` engine for local storage,
//!     with `storage::mvcc` providing transactions. See their documentation
//!     for details.
//!
//! 13. `Transaction::scan` uses `sql::engine::KeyPrefix::Table` to obtain the
//!     key prefix for the scanned table, encoded via `encoding::keycode`. It
//!     scans rows under this prefix by calling `MVCC::scan_prefix`, which in
//!     turn dispatches to `BitCask::scan_prefix`. It returns a row iterator.
//!
//! 14. A row iterator is propagated back up through the stack:
//!     `BitCask` → `MVCC` → `Local` → `State` → `Raft` → `scan` → `Plan::execute`
//!
//! 15. `Plan::execute` collects the results in a `ExecutionResult::Select`,
//!     and returns it to `Session::execute`. It in turns returns it to
//!     `Server::sql_session`, which encodes it and sends it across the wire
//!     to `toySQL`, which displays them to the user.
//!
//! TODO: expand this into a "Life of a SQL statement" document.

pub mod engine;
pub mod execution;
pub mod parser;
pub mod planner;
pub mod types;

/// SQL tests are implemented as goldenscripts under src/sql/testscripts.
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::fmt::Write as _;
    use std::path::Path;
    use std::result::Result;

    use crossbeam::channel::Receiver;
    use itertools::Itertools as _;
    use tempfile::TempDir;
    use test_each_file::test_each_path;

    use super::engine::Catalog as _;
    use super::execution::{Session, StatementResult};
    use super::parser::Parser;
    use super::planner::{OPTIMIZERS, Plan};
    use crate::encoding::format::{self, Formatter as _};
    use crate::sql::engine::{Engine, Local};
    use crate::sql::planner::{Planner, Scope};
    use crate::storage::engine::test as testengine;
    use crate::storage::{self, Engine as _};

    // Run goldenscript tests in src/sql/testscripts.
    test_each_path! { in "src/sql/testscripts/expressions" as expressions => test_goldenscript_expr }
    test_each_path! { in "src/sql/testscripts/optimizers" as optimizers => test_goldenscript }
    test_each_path! { in "src/sql/testscripts/queries" as queries => test_goldenscript }
    test_each_path! { in "src/sql/testscripts/schema" as schema => test_goldenscript }
    test_each_path! { in "src/sql/testscripts/transactions" as transactions => test_goldenscript }
    test_each_path! { in "src/sql/testscripts/writes" as writes => test_goldenscript }

    /// Runs SQL goldenscripts.
    fn test_goldenscript(path: &Path) {
        // The runner's Session can't borrow from an Engine in the same struct,
        // so pass an engine reference. Use both BitCask and Memory engines and
        // mirror operations across them. Emit engine operations to op_rx.
        let (op_tx, op_rx) = crossbeam::channel::unbounded();
        let tempdir = TempDir::with_prefix("toydb").expect("tempdir failed");
        let bitcask =
            storage::BitCask::new(tempdir.path().join("bitcask")).expect("bitcask failed");
        let memory = storage::Memory::new();
        let engine =
            Local::new(testengine::Emit::new(testengine::Mirror::new(bitcask, memory), op_tx));
        let mut runner = SQLRunner::new(&engine, op_rx);

        goldenscript::run(&mut runner, path).expect("goldenscript failed")
    }

    /// Runs expression goldenscripts.
    fn test_goldenscript_expr(path: &Path) {
        goldenscript::run(&mut ExpressionRunner, path).expect("goldenscript failed")
    }

    /// The SQL test runner.
    struct SQLRunner<'a> {
        engine: &'a TestEngine,
        sessions: HashMap<String, Session<'a, TestEngine>>,
        op_rx: Receiver<testengine::Operation>,
    }

    type TestEngine =
        Local<testengine::Emit<testengine::Mirror<storage::BitCask, storage::Memory>>>;

    impl<'a> SQLRunner<'a> {
        fn new(engine: &'a TestEngine, op_rx: Receiver<testengine::Operation>) -> Self {
            Self { engine, sessions: HashMap::new(), op_rx }
        }
    }

    impl goldenscript::Runner for SQLRunner<'_> {
        fn run(&mut self, command: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
            let mut output = String::new();

            // Obtain a session based on the command prefix ("" if none).
            let prefix = command.prefix.clone().unwrap_or_default();
            let session = self.sessions.entry(prefix).or_insert_with(|| self.engine.session());

            // Handle runner commands.
            match command.name.as_str() {
                // dump
                "dump" => {
                    command.consume_args().reject_rest()?;
                    let mut engine = self.engine.mvcc.engine.lock().expect("mutex failed");
                    let mut iter = engine.scan(..);
                    while let Some((key, value)) = iter.next().transpose()? {
                        let fmtkv = format::MVCC::<format::SQL>::key_value(&key, &value);
                        let rawkv = format::Raw::key_value(&key, &value);
                        writeln!(output, "{fmtkv} [{rawkv}]",)?;
                    }
                    return Ok(output);
                }

                // schema [TABLE...]
                "schema" => {
                    let mut args = command.consume_args();
                    let tables = args.rest_pos().iter().map(|arg| arg.value.clone()).collect_vec();
                    args.reject_rest()?;

                    let schemas = if tables.is_empty() {
                        session.with_txn(true, |txn| txn.list_tables())?
                    } else {
                        tables
                            .into_iter()
                            .map(|t| session.with_txn(true, |txn| txn.must_get_table(&t)))
                            .try_collect()?
                    };
                    return Ok(schemas.into_iter().join("\n"));
                }

                // Otherwise, fall through to SQL execution.
                _ => {}
            }

            // The entire command is the SQL statement. There are no args.
            if !command.args.is_empty() {
                return Err("SQL statements should be given as a command with no args".into());
            }
            let input = &command.name;
            let mut tags = command.tags.clone();

            // Output the plan if requested.
            if tags.remove("plan") {
                let ast = Parser::parse(input)?;
                let plan =
                    session.with_txn(true, |txn| Planner::new(txn).build(ast)?.optimize())?;
                writeln!(output, "{plan}")?;
            }

            // Output plan optimizations if requested.
            if tags.remove("opt") {
                if tags.contains("plan") {
                    return Err("using both plan and opt is redundant".into());
                }
                let ast = Parser::parse(input)?;
                let plan = session.with_txn(true, |txn| Planner::new(txn).build(ast))?;
                let Plan::Select(mut root) = plan else {
                    return Err("can only use opt with SELECT plans".into());
                };
                writeln!(output, "{}", format!("Initial:\n{root}").replace('\n', "\n   "))?;
                for optimizer in OPTIMIZERS.iter() {
                    let prev = root.clone();
                    root = optimizer.optimize(root)?;
                    if root != prev {
                        writeln!(
                            output,
                            "{}",
                            format!("{optimizer:?}:\n{root}").replace('\n', "\n   ")
                        )?;
                    }
                }
            }

            // Execute the statement.
            let result = session.execute(input)?;

            // Output engine ops if requested.
            if tags.remove("ops") {
                while let Ok(op) = self.op_rx.try_recv() {
                    match op {
                        testengine::Operation::Delete { key } => {
                            let fmtkey = format::MVCC::<format::SQL>::key(&key);
                            let rawkey = format::Raw::key(&key);
                            writeln!(output, "delete {fmtkey} [{rawkey}]")?;
                        }
                        testengine::Operation::Flush => writeln!(output, "flush")?,
                        testengine::Operation::Set { key, value } => {
                            let fmtkv = format::MVCC::<format::SQL>::key_value(&key, &value);
                            let rawkv = format::Raw::key_value(&key, &value);
                            writeln!(output, "set {fmtkv} [{rawkv}]")?;
                        }
                    }
                }
            }

            // Output the result if requested. SELECT results are always output.
            match result {
                StatementResult::Select { columns, rows } => {
                    if tags.remove("header") {
                        writeln!(output, "{}", columns.into_iter().join(", "))?;
                    }
                    for row in rows {
                        writeln!(output, "{}", row.into_iter().join(", "))?;
                    }
                }
                result if tags.remove("result") => writeln!(output, "{result:?}")?,
                _ => {}
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

    /// A test runner for expressions. Evaluates expressions to values, and
    /// optionally emits the expression tree.
    struct ExpressionRunner;

    type Catalog<'a> = <Local<storage::Memory> as Engine<'a>>::Transaction;

    impl goldenscript::Runner for ExpressionRunner {
        fn run(&mut self, command: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
            let mut output = String::new();

            // The entire command is the expression to evaluate. There are no args.
            if !command.args.is_empty() {
                return Err("expressions should be given as a command with no args".into());
            }
            let input = &command.name;
            let mut tags = command.tags.clone();

            // Parse and build the expression.
            let ast = Parser::parse_expr(input)?;
            let expr = Planner::<Catalog>::build_expression(ast, &Scope::new())?;

            // Evaluate the expression.
            let value = expr.evaluate(None)?;
            write!(output, "{value}")?;

            // If requested, convert the expression to conjunctive normal form
            // and dump it. Assert that it produces the same result.
            if tags.remove("cnf") {
                let cnf = expr.clone().into_cnf();
                assert_eq!(value, cnf.evaluate(None)?, "CNF result differs");
                write!(output, " ← {cnf}")?;
            }

            // If requested, debug-dump the parsed expression.
            if tags.remove("expr") {
                write!(output, " ← {:?}", expr)?;
            }
            writeln!(output)?;

            // Reject unknown tags.
            if let Some(tag) = tags.iter().next() {
                return Err(format!("unknown tag {tag}").into());
            }

            Ok(output)
        }
    }
}
