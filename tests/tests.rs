//! A basic set of end-to-end tests as Goldenscripts under tests/scripts/. These
//! spin up actual clusters using the built binary and run operations against
//! them from multiple clients.
//!
//! There are more comprehensive tests elsewhere in the codebase, see the various
//! src/*/testscript scripts.

#![warn(clippy::all)]

mod testcluster;

use std::collections::HashMap;
use std::error::Error;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::{LazyLock, Mutex};

use itertools::Itertools as _;
use test_each_file::test_each_path;

use testcluster::TestCluster;
use toydb::{Client, StatementResult};

// Run goldenscript tests in tests/scripts.
test_each_path! { in "tests/scripts" => test_goldenscript }

fn test_goldenscript(path: &Path) {
    // We can't run tests concurrently, because the test clusters end up using
    // the same ports. We also don't want to run a bunch of them concurrently.
    // We can't use the #[serial_test] macro either, since it doesn't work with
    // test_each_path. Just use a mutex to serialize them.
    static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(Mutex::default);
    let _guard = MUTEX.lock().ok(); // ignore poisoning

    goldenscript::run(&mut Runner::new(), path).expect("goldenscript failed")
}

/// Runs Raft goldenscript tests. See run() for available commands.
#[derive(Default)]
struct Runner {
    cluster: Option<TestCluster>,
    clients: HashMap<String, Client>,
}

impl Runner {
    fn new() -> Self {
        Self::default()
    }

    /// Fetches a client for the given prefix, or creates a new one.
    fn get_client(&mut self, prefix: &Option<String>) -> Result<&mut Client, Box<dyn Error>> {
        let name = Self::client_name(prefix);
        if !self.clients.contains_key(name) {
            let Some(cluster) = self.cluster.as_mut() else {
                return Err("no cluster".into());
            };
            let client = cluster.connect()?;
            self.clients.insert(name.to_string(), client);
        }
        Ok(self.clients.get_mut(name).expect("no client"))
    }

    /// Returns a client name for a prefix.
    fn client_name(prefix: &Option<String>) -> &str {
        prefix.as_deref().unwrap_or_default()
    }
}

impl goldenscript::Runner for Runner {
    /// Runs a goldenscript command.
    fn run(&mut self, command: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        let mut tags = command.tags.clone();

        // Handle simple, non-SQL commands.
        match command.name.as_str() {
            // close
            "close" => {
                command.consume_args().reject_rest()?;
                let name = Self::client_name(&command.prefix);
                if self.clients.remove(name).is_none() {
                    return Err("no client to close".into());
                }
                return Ok(output);
            }

            // cluster nodes=N
            "cluster" => {
                let mut args = command.consume_args();
                let nodes = args.lookup_parse("nodes")?.unwrap_or(0);
                args.reject_rest()?;
                if self.cluster.is_some() {
                    return Err("cluster already exists".into());
                }
                self.cluster = Some(TestCluster::run(nodes)?);
                return Ok(output);
            }

            // status
            "status" => {
                command.consume_args().reject_rest()?;
                let status = self.get_client(&command.prefix)?.status()?;
                write!(output, "{status:#?}")?;
                return Ok(output);
            }

            // table [TABLE]
            "table" => {
                let mut args = command.consume_args();
                let name = &args.next_pos().ok_or("table not given")?.value;
                let raw = args.lookup_parse("raw")?.unwrap_or(false);
                args.reject_rest()?;
                let table = self.get_client(&command.prefix)?.get_table(name)?;
                if raw {
                    write!(output, "{table:#?}")?;
                } else {
                    write!(output, "{table}")?;
                }
                return Ok(output);
            }

            // tables
            "tables" => {
                command.consume_args().reject_rest()?;
                let tables = self.get_client(&command.prefix)?.list_tables()?;
                for table in tables {
                    writeln!(output, "{table}")?;
                }
                return Ok(output);
            }

            _ => {}
        }

        // Otherwise, interpret the entire command as a SQL statement.
        if !command.args.is_empty() {
            return Err("statements should be given as a command with no args".into());
        }
        let client = self.get_client(&command.prefix)?;
        let input = &command.name;

        // Execute the command and display the result if requested.
        // SELECT and EXPLAIN results are always output.
        let result = client.execute(input)?;

        match result {
            StatementResult::Select { columns, rows } => {
                if tags.remove("header") {
                    writeln!(output, "{}", columns.into_iter().join(", "))?;
                }
                for row in rows {
                    writeln!(output, "{}", row.into_iter().join(", "))?;
                }
            }
            StatementResult::Explain(root) => writeln!(output, "{root}")?,
            result if tags.remove("result") => writeln!(output, "{result:?}")?,
            _ => {}
        }

        if let Some(tag) = tags.iter().next() {
            return Err(format!("invalid tag {tag}").into());
        }

        Ok(output)
    }
}
