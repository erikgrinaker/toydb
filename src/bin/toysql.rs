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
                .help("Host to connect to (IP address)")
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

struct ToySQL {
    client: toydb::Client,
    editor: rustyline::Editor<()>,
}

impl ToySQL {
    fn new(hostname: &str, port: u16) -> Result<Self, toydb::Error> {
        let sa = format!("{}:{}", hostname, port).parse::<std::net::SocketAddr>()?;
        Ok(Self { client: toydb::Client::new(sa)?, editor: rustyline::Editor::<()>::new() })
    }

    fn run(&mut self) -> Result<(), toydb::Error> {
        let status = self.client.status()?;
        println!("Connected to node \"{}\" (version {})", status.id, status.version);

        while let Some(command) = self.prompt()? {
            if command.is_empty() {
                continue;
            } else if command.starts_with('!') {
                self.command(&command)?;
            } else {
                self.query(&command)?;
            }
        }
        Ok(())
    }

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

    fn command_table(&mut self, table: &str) -> Result<(), toydb::Error> {
        println!("{}", self.client.get_table(table)?);
        Ok(())
    }

    fn query(&mut self, query: &str) -> Result<(), toydb::Error> {
        let mut resultset = self.client.query(query)?;
        println!("{}", resultset.columns().join("|"));
        while let Some(Ok(row)) = resultset.next() {
            let formatted: Vec<String> = row.into_iter().map(|v| format!("{}", v)).collect();
            println!("{}", formatted.join("|"));
        }
        Ok(())
    }

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
