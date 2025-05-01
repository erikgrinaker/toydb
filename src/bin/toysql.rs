//! toySQL is a command-line client for toyDB. It connects to a toyDB node
//! (default localhost:9601) and executes SQL statements against it via an
//! interactive shell interface. Command history is stored in .toysql.history.

#![warn(clippy::all)]

use std::path::PathBuf;

use clap::Parser as _;
use itertools::Itertools as _;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Editor, Modifiers};
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};

use toydb::Client;
use toydb::errinput;
use toydb::error::Result;
use toydb::sql::execution::StatementResult;
use toydb::sql::parser::{Lexer, Token};

fn main() {
    if let Err(error) = Command::parse().run() {
        eprintln!("Error: {error}");
    }
}

/// The toySQL command.
#[derive(clap::Parser)]
#[command(about = "A toyDB client.", version, propagate_version = true)]
struct Command {
    /// A SQL statement to execute, then exit.
    #[arg()]
    statement: Option<String>,
    /// Host to connect to.
    #[arg(short = 'H', long, default_value = "localhost")]
    host: String,
    /// Port number to connect to.
    #[arg(short = 'p', long, default_value = "9601")]
    port: u16,
}

impl Command {
    /// Runs the command.
    fn run(self) -> Result<()> {
        let mut shell = Shell::new(&self.host, self.port)?;
        match self.statement {
            Some(statement) => shell.execute(&statement),
            None => shell.run(),
        }
    }
}

/// An interactive toySQL shell.
struct Shell {
    /// The toyDB client.
    client: Client,
    /// The Rustyline command editor.
    editor: Editor<InputValidator, DefaultHistory>,
    /// The path to the history file, if any.
    history_path: Option<PathBuf>,
    /// If true, SELECT column headers will be displayed.
    show_headers: bool,
}

impl Shell {
    /// Creates a new shell connected to the given server.
    fn new(host: &str, port: u16) -> Result<Self> {
        let client = Client::connect((host, port))?;
        // Set up Rustyline. Make sure multiline pastes are handled normally.
        let mut editor = Editor::new()?;
        editor.set_helper(Some(InputValidator));
        editor.bind_sequence(
            rustyline::KeyEvent(rustyline::KeyCode::BracketedPasteStart, Modifiers::NONE),
            rustyline::Cmd::Noop,
        );
        let history_path =
            std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".toysql.history"));
        Ok(Self { client, editor, history_path, show_headers: false })
    }

    /// Executes a SQL statement or ! command.
    fn execute(&mut self, input: &str) -> Result<()> {
        if input.starts_with('!') {
            self.execute_command(input)
        } else if !input.is_empty() {
            self.execute_sql(input)
        } else {
            Ok(())
        }
    }

    /// Executes a toySQL ! command (e.g. !help)
    fn execute_command(&mut self, input: &str) -> Result<()> {
        let mut input = input.split_ascii_whitespace();
        let Some(command) = input.next() else {
            return errinput!("expected command");
        };
        let args = input.collect_vec();

        match (command, args.as_slice()) {
            // Toggles column headers.
            ("!headers", []) => {
                self.show_headers = !self.show_headers;
                match self.show_headers {
                    true => println!("Headers enabled"),
                    false => println!("Headers disabled"),
                }
            }
            ("!headers", _) => return errinput!("!headers takes no arguments"),

            // Displays help.
            ("!help", []) => println!(
                r#"
Enter a SQL statement terminated by a semicolon (;) to execute it, or Ctrl-D to
exit. The following commands are also available:

    !headers           Toggles column headers
    !help              This help message
    !status            Display server status
    !table NAME        Display a table schema
    !tables            List tables
"#
            ),
            ("!help", _) => return errinput!("!help takes no arguments"),

            // Displays server status.
            ("!status", []) => {
                let status = self.client.status()?;
                println!(
                    r#"
Server:       n{server} with Raft leader n{leader} in term {term} for {nodes} nodes
Raft log:     {committed} committed, {applied} applied, {raft_size} MB, {raft_garbage}% garbage ({raft_storage} engine)
Replication:  {raft_match}
SQL storage:  {sql_keys} keys, {sql_size} MB logical, {nodes}x {sql_disk_size} MB disk, {sql_garbage}% garbage ({sql_storage} engine)
Transactions: {active_txns} active, {versions} total
"#,
                    server = status.server,
                    leader = status.raft.leader,
                    term = status.raft.term,
                    nodes = status.raft.match_index.len(),
                    committed = status.raft.commit_index,
                    applied = status.raft.applied_index,
                    raft_size =
                        format_args!("{:.3}", status.raft.storage.size as f64 / 1_000_000.0),
                    raft_garbage =
                        format_args!("{:.0}", status.raft.storage.garbage_disk_percent()),
                    raft_storage = status.raft.storage.name,
                    raft_match =
                        status.raft.match_index.iter().map(|(n, m)| format!("n{n}:{m}")).join(" "),
                    sql_keys = status.mvcc.storage.keys,
                    sql_size = format_args!("{:.3}", status.mvcc.storage.size as f64 / 1_000_000.0),
                    sql_disk_size =
                        format_args!("{:.3}", status.mvcc.storage.disk_size as f64 / 1_000_000.0),
                    sql_garbage = format_args!("{:.0}", status.mvcc.storage.garbage_disk_percent()),
                    sql_storage = status.mvcc.storage.name,
                    active_txns = status.mvcc.active_txns,
                    versions = status.mvcc.versions,
                )
            }
            ("!status", _) => return errinput!("!status takes no arguments"),

            ("!table", [name]) => println!("{}", self.client.get_table(name)?),
            ("!table", _) => return errinput!("!table takes 1 argument"),

            ("!tables", []) => self.client.list_tables()?.iter().for_each(|t| println!("{t}")),
            ("!tables", _) => return errinput!("!tables takes no arguments"),

            (command, _) => return errinput!("unknown command {command}"),
        }
        Ok(())
    }

    /// Executes a SQL statement and displays the results.
    fn execute_sql(&mut self, statement: &str) -> Result<()> {
        use StatementResult::*;
        match self.client.execute(statement)? {
            Begin(state) => match state.read_only {
                true => println!("Began read-only transaction at version {}", state.version),
                false => println!("Began transaction {}", state.version),
            },
            Commit { version } => println!("Committed transaction {version}"),
            Rollback { version } => println!("Rolled back transaction {version}"),
            Insert { count } => println!("Inserted {count} rows"),
            Delete { count } => println!("Deleted {count} rows"),
            Update { count } => println!("Updated {count} rows"),
            CreateTable { name } => println!("Created table {name}"),
            DropTable { name, existed } => match existed {
                true => println!("Dropped table {name}"),
                false => println!("Table {name} does not exist"),
            },
            Explain(plan) => println!("{plan}"),
            Select { columns, rows } => {
                if self.show_headers {
                    println!("{}", columns.iter().map(|c| c.as_header()).join(", "));
                }
                for row in rows {
                    println!("{}", row.iter().join(", "));
                }
            }
        }
        Ok(())
    }

    /// Prompts the user for input. Returns None if the shell should close.
    fn prompt(&mut self) -> rustyline::Result<String> {
        let prompt = match self.client.txn() {
            Some(txn) if txn.read_only => format!("toydb@{}> ", txn.version),
            Some(txn) => format!("toydb:{}> ", txn.version),
            None => "toydb> ".to_string(),
        };
        self.editor.readline(&prompt)
    }

    /// Runs the interactive shell.
    fn run(&mut self) -> Result<()> {
        // Load the history file, if any.
        if let Some(history_path) = &self.history_path {
            match self.editor.load_history(history_path) {
                Ok(()) => {}
                Err(ReadlineError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }

        // Print welcome message.
        let server = self.client.status()?.server;
        println!("Connected to toyDB node n{server}. Enter !help for instructions.");

        // Prompt for commands and execute them.
        loop {
            let input = match self.prompt() {
                Ok(input) => input.trim().to_string(),
                Err(ReadlineError::Interrupted) => continue,
                Err(ReadlineError::Eof) => break,
                Err(error) => return Err(error.into()),
            };
            self.editor.add_history_entry(&input)?;
            if let Err(error) = self.execute(&input) {
                eprintln!("Error: {error}");
            };
        }

        // Save the history file.
        if let Some(history_path) = &self.history_path {
            self.editor.save_history(history_path)?;
        }
        Ok(())
    }
}

/// A Rustyline helper for multiline editing. After a new line is entered, it
/// determines whether the input makes up a complete SQL statement that should
/// be submitted to the server (i.e. it's terminated by ;), or wait for further
/// input.
#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator;

impl Validator for InputValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        let input = ctx.input();
        // Empty lines and ! commands are ready.
        if input.is_empty() || input.starts_with('!') || input == ";" {
            return Ok(ValidationResult::Valid(None));
        }
        // For SQL statements, just look for any semicolon or lexer error, and
        // rely on the server for further validation and error handling.
        if Lexer::new(input).any(|r| matches!(r, Ok(Token::Semicolon) | Err(_))) {
            return Ok(ValidationResult::Valid(None));
        }
        // Otherwise, wait for more input.
        Ok(ValidationResult::Incomplete)
    }

    fn validate_while_typing(&self) -> bool {
        false // only check after completed lines
    }
}
