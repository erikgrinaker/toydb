/*
 * ToySQL is a command-line client for ToyDB. It can connect to any ToyDB node
 * via gRPC and run SQL queries through a REPL interface.
 */

#![warn(clippy::all)]

#[macro_use]
extern crate clap;
extern crate rustyline;
extern crate toydb;

use rustyline::error::ReadlineError;

fn main() -> Result<(), toydb::Error> {
    let opts = app_from_crate!()
        .arg(
            clap::Arg::with_name("host")
                .short("h")
                .long("host")
                .help("Host to connect to")
                .takes_value(true)
                .default_value("127.0.0.1"),
        )
        .arg(
            clap::Arg::with_name("port")
                .short("p")
                .long("port")
                .help("Port number to connect to")
                .takes_value(true)
                .default_value("9605"),
        )
        .get_matches();

    ToySQL::new(opts.value_of("host").unwrap(), opts.value_of("port").unwrap().parse()?)?.run()
}

/// The ToySQL REPL
struct ToySQL {
    client: toydb::Client,
    editor: rustyline::Editor<()>,
}

impl ToySQL {
    /// Creates a new ToySQL REPL for the given server host and port
    fn new(host: &str, port: u16) -> Result<Self, toydb::Error> {
        Ok(Self { client: toydb::Client::new(host, port)?, editor: rustyline::Editor::<()>::new() })
    }

    /// Runs the ToySQL REPL
    fn run(&mut self) -> Result<(), toydb::Error> {
        let history_path = match std::env::var_os("HOME") {
            Some(home) => Some(std::path::Path::new(&home).join(".toysql.history")),
            None => None,
        };
        if let Some(path) = &history_path {
            match self.editor.load_history(path) {
                Ok(_) => {}
                Err(ReadlineError::Io(ref err)) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            };
        }

        let status = self.client.status()?;
        println!("Connected to node \"{}\" (version {})", status.id, status.version);

        while let Some(input) = self.prompt()? {
            if input.is_empty() {
                continue;
            } else if input.starts_with('!') {
                self.command(&input)?;
            } else {
                self.query(&input)?;
            }
        }

        if let Some(path) = &history_path {
            self.editor.save_history(path)?;
        }
        Ok(())
    }

    /// Runs a query and displays the results
    fn query(&mut self, query: &str) -> Result<(), toydb::Error> {
        let mut resultset = self.client.query(query)?;
        println!("{}", resultset.columns().join("|"));
        while let Some(Ok(row)) = resultset.next() {
            let formatted: Vec<String> = row.into_iter().map(|v| format!("{}", v)).collect();
            println!("{}", formatted.join("|"));
        }
        Ok(())
    }

    /// Handles a REPL command (prefixed by !, e.g. !help)
    fn command(&mut self, command: &str) -> Result<(), toydb::Error> {
        let mut args = command.split_whitespace();
        match args.next() {
            Some("!help") => println!("Help!"),
            Some("!table") => self.command_table(args.next().unwrap())?,
            Some(c) => return Err(toydb::Error::Parse(format!("Unknown command {}", c))),
            None => return Err(toydb::Error::Parse("Expected command".to_string())),
        };
        Ok(())
    }

    /// Handles the !table command
    fn command_table(&mut self, table: &str) -> Result<(), toydb::Error> {
        println!("{}", self.client.get_table(table)?);
        Ok(())
    }

    /// Prompts the user for input
    fn prompt(&mut self) -> Result<Option<String>, toydb::Error> {
        match self.editor.readline("toydb> ") {
            Ok(input) => {
                self.editor.add_history_entry(&input);
                Ok(Some(input.trim().to_string()))
            }
            Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}
