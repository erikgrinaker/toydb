#![warn(clippy::all)]

#[macro_use]
extern crate clap;
extern crate config;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate simplelog;
extern crate toydb;

fn main() -> Result<(), toydb::Error> {
    let opts = app_from_crate!()
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .long("config")
                .help("Configuration file path")
                .takes_value(true)
                .default_value("/etc/toydb.yaml"),
        )
        .get_matches();
    let cfg = Config::new(opts.value_of("config").unwrap())?;
    simplelog::SimpleLogger::init(
        cfg.log_level.parse::<simplelog::LevelFilter>()?,
        simplelog::Config::default(),
    )?;
    toydb::Server { id: cfg.id, addr: cfg.listen, threads: cfg.threads }.listen()
}

#[derive(Debug, Deserialize)]
struct Config {
    id: String,
    listen: String,
    threads: usize,
    log_level: String,
}

impl Config {
    fn new(file: &str) -> Result<Self, config::ConfigError> {
        let mut c = config::Config::new();
        c.set_default("id", "toydb")?;
        c.set_default("listen", "0.0.0.0:9605")?;
        c.set_default("threads", 4)?;
        c.set_default("log_level", "info")?;

        c.merge(config::File::with_name(file))?;
        c.merge(config::Environment::with_prefix("TOYDB"))?;
        c.try_into()
    }
}
