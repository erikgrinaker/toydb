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

    let sa = format!("{}:{}", opts.value_of("host").unwrap(), opts.value_of("port").unwrap())
        .parse::<std::net::SocketAddr>()?;

    let client = toydb::Client::new(sa)?;
    let mut editor = rustyline::Editor::<()>::new();
    loop {
        let query = match editor.readline("toydb> ") {
            Ok(input) => {
                editor.add_history_entry(&input);
                input
            }
            Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => break,
            Err(err) => return Err(err.into()),
        };
        let result = client.echo(&query)?;
        println!("{}", result);
    }
    Ok(())
}
