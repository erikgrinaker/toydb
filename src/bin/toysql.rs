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
    history_path: Option<std::path::PathBuf>,
}

impl ToySQL {
    /// Creates a new ToySQL REPL for the given server host and port
    fn new(host: &str, port: u16) -> Result<Self, toydb::Error> {
        Ok(Self {
            client: toydb::Client::new(host, port)?,
            editor: rustyline::Editor::<()>::new(),
            history_path: std::env::var_os("HOME")
                .map(|home| std::path::Path::new(&home).join(".toysql.history")),
        })
    }

    /// Handles a REPL command (prefixed by !, e.g. !help)
    fn command(&mut self, input: &str) -> Result<(), toydb::Error> {
        let mut input = input.split_ascii_whitespace();
        let command =
            input.next().ok_or_else(|| toydb::Error::Parse("Expected command.".to_string()))?;

        let getargs = |n| {
            let args: Vec<&str> = input.collect();
            if args.len() != n {
                Err(toydb::Error::Parse(format!(
                    "{}: expected {} args, got {}",
                    command,
                    n,
                    args.len()
                )))
            } else {
                Ok(args)
            }
        };

        match command {
            "!help" => println!(
                r#"
Enter an SQL statement on a single line to execute it and display the result.
Semicolons are not supported. The following !-commands are also available:

  !help            This help message
  !table [table]   Display table schema, if it exists
"#
            ),
            "!table" => {
                let args = getargs(1)?;
                println!("{}", self.client.get_table(args[0])?);
            }
            c => return Err(toydb::Error::Parse(format!("Unknown command {}", c))),
        }
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

    /// Runs the ToySQL REPL
    fn run(&mut self) -> Result<(), toydb::Error> {
        if let Some(path) = &self.history_path {
            match self.editor.load_history(path) {
                Ok(_) => {}
                Err(ReadlineError::Io(ref err)) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            };
        }

        let status = self.client.status()?;
        println!(
            "Connected to node \"{}\" (version {}). Enter !help for instructions.",
            status.id, status.version
        );

        while let Some(input) = self.prompt()? {
            if input.starts_with('!') {
                self.command(&input)?;
            } else if !input.is_empty() {
                self.query(&input)?;
            }
        }

        if let Some(path) = &self.history_path {
            self.editor.save_history(path)?;
        }
        Ok(())
    }
}
