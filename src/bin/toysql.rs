/*
 * ToySQL is a command-line client for toyDB. It can connect to any toyDB node
 * via gRPC and run SQL queries through a REPL interface.
 */

#![warn(clippy::all)]

use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version};
use rustyline::{error::ReadlineError, Editor};
use toydb::error::{Error, Result};
use toydb::sql::engine::Mode;
use toydb::sql::execution::ResultSet;
use toydb::Client;

#[tokio::main]
async fn main() -> Result<()> {
    let opts = app_from_crate!()
        .arg(clap::Arg::with_name("command"))
        .arg(clap::Arg::with_name("headers").short("H").long("headers").help("Show column headers"))
        .arg(
            clap::Arg::with_name("host")
                .short("h")
                .long("host")
                .help("Host to connect to")
                .takes_value(true)
                .required(true)
                .default_value("127.0.0.1"),
        )
        .arg(
            clap::Arg::with_name("port")
                .short("p")
                .long("port")
                .help("Port number to connect to")
                .takes_value(true)
                .required(true)
                .default_value("9605"),
        )
        .get_matches();

    let mut toysql =
        ToySQL::new(opts.value_of("host").unwrap(), opts.value_of("port").unwrap().parse()?)
            .await?;
    if opts.is_present("headers") {
        toysql.show_headers = true
    }

    if let Some(command) = opts.value_of("command") {
        toysql.execute(&command).await
    } else {
        toysql.run().await
    }
}

/// The ToySQL REPL
struct ToySQL {
    client: Client,
    editor: Editor<()>,
    history_path: Option<std::path::PathBuf>,
    show_headers: bool,
}

impl ToySQL {
    /// Creates a new ToySQL REPL for the given server host and port
    async fn new(host: &str, port: u16) -> Result<Self> {
        Ok(Self {
            client: Client::new((host, port)).await?,
            editor: Editor::<()>::new(),
            history_path: std::env::var_os("HOME")
                .map(|home| std::path::Path::new(&home).join(".toysql.history")),
            show_headers: false,
        })
    }

    /// Executes a line of input
    async fn execute(&mut self, input: &str) -> Result<()> {
        if input.starts_with('!') {
            self.execute_command(&input).await
        } else if !input.is_empty() {
            self.execute_query(&input).await
        } else {
            Ok(())
        }
    }

    /// Handles a REPL command (prefixed by !, e.g. !help)
    async fn execute_command(&mut self, input: &str) -> Result<()> {
        let mut input = input.split_ascii_whitespace();
        let command = input.next().ok_or_else(|| Error::Parse("Expected command.".to_string()))?;

        let getargs = |n| {
            let args: Vec<&str> = input.collect();
            if args.len() != n {
                Err(Error::Parse(format!("{}: expected {} args, got {}", command, n, args.len())))
            } else {
                Ok(args)
            }
        };

        match command {
            "!headers" => match getargs(1)?[0] {
                "on" => {
                    self.show_headers = true;
                    println!("Headers enabled");
                }
                "off" => {
                    self.show_headers = false;
                    println!("Headers disabled");
                }
                v => return Err(Error::Parse(format!("Invalid value {}, expected on or off", v))),
            },
            "!help" => println!(
                r#"
Enter a SQL statement on a single line to execute it and display the result.
Semicolons are not supported. The following commands are also available:

    !headers <on|off>  Enable or disable column headers
    !help              This help message
    !status            Display server status
    !table [table]     Display table schema, if it exists
    !tables            List tables
"#
            ),
            "!status" => {
                let status = self.client.status().await?;
                let mut node_logs = status
                    .raft
                    .node_last_index
                    .iter()
                    .map(|(id, index)| format!("{}:{}", id, index))
                    .collect::<Vec<_>>();
                node_logs.sort();
                println!(
                    r#"
Server:    {server} (leader {leader} in term {term} with {nodes} nodes)
Raft log:  {committed} committed, {applied} applied, {raft_size} MB ({raft_storage} storage)
Node logs: {logs}
SQL txns:  {txns_active} active, {txns} total ({sql_storage} storage)
"#,
                    server = status.raft.server,
                    leader = status.raft.leader,
                    term = status.raft.term,
                    nodes = status.raft.node_last_index.len(),
                    committed = status.raft.commit_index,
                    applied = status.raft.apply_index,
                    raft_storage = status.raft.storage,
                    raft_size = format!("{:.3}", status.raft.storage_size as f64 / 1000.0 / 1000.0),
                    logs = node_logs.join(" "),
                    txns = status.mvcc.txns,
                    txns_active = status.mvcc.txns_active,
                    sql_storage = status.mvcc.storage
                )
            }
            "!table" => {
                let args = getargs(1)?;
                println!("{}", self.client.get_table(args[0]).await?.as_sql());
            }
            "!tables" => {
                getargs(0)?;
                for table in self.client.list_tables().await? {
                    println!("{}", table)
                }
            }
            c => return Err(Error::Parse(format!("Unknown command {}", c))),
        }
        Ok(())
    }

    /// Runs a query and displays the results
    async fn execute_query(&mut self, query: &str) -> Result<()> {
        match self.client.execute(query).await? {
            ResultSet::Begin { id, mode } => match mode {
                Mode::ReadWrite => println!("Began transaction {}", id),
                Mode::ReadOnly => println!("Began read-only transaction {}", id),
                Mode::Snapshot { version, .. } => println!(
                    "Began read-only transaction {} in snapshot at version {}",
                    id, version
                ),
            },
            ResultSet::Commit { id } => println!("Committed transaction {}", id),
            ResultSet::Rollback { id } => println!("Rolled back transaction {}", id),
            ResultSet::Create { count } => println!("Created {} rows", count),
            ResultSet::Delete { count } => println!("Deleted {} rows", count),
            ResultSet::Update { count } => println!("Updated {} rows", count),
            ResultSet::CreateTable { name } => println!("Created table {}", name),
            ResultSet::DropTable { name } => println!("Dropped table {}", name),
            ResultSet::Explain(plan) => println!("{}", plan.to_string()),
            ResultSet::Query { columns, mut rows } => {
                if self.show_headers {
                    println!(
                        "{}",
                        columns
                            .iter()
                            .map(|c| c.name.as_deref().unwrap_or("?"))
                            .collect::<Vec<_>>()
                            .join("|")
                    );
                }
                while let Some(row) = rows.next().transpose()? {
                    println!(
                        "{}",
                        row.into_iter().map(|v| format!("{}", v)).collect::<Vec<_>>().join("|")
                    );
                }
            }
        }
        Ok(())
    }

    /// Prompts the user for input
    fn prompt(&mut self) -> Result<Option<String>> {
        let prompt = match self.client.txn() {
            Some((id, Mode::ReadWrite)) => format!("toydb:{}> ", id),
            Some((id, Mode::ReadOnly)) => format!("toydb:{}> ", id),
            Some((_, Mode::Snapshot { version })) => format!("toydb@{}> ", version),
            None => "toydb> ".into(),
        };
        match self.editor.readline(&prompt) {
            Ok(input) => {
                self.editor.add_history_entry(&input);
                Ok(Some(input.trim().to_string()))
            }
            Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Runs the ToySQL REPL
    async fn run(&mut self) -> Result<()> {
        if let Some(path) = &self.history_path {
            match self.editor.load_history(path) {
                Ok(_) => {}
                Err(ReadlineError::Io(ref err)) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            };
        }

        let status = self.client.status().await?;
        println!(
            "Connected to toyDB node \"{}\". Enter !help for instructions.",
            status.raft.server
        );

        while let Some(input) = self.prompt()? {
            match self.execute(&input).await {
                Ok(()) => {}
                error @ Err(Error::Internal(_)) => return error,
                Err(error) => println!("Error: {}", error.to_string()),
            }
        }

        if let Some(path) = &self.history_path {
            self.editor.save_history(path)?;
        }
        Ok(())
    }
}
