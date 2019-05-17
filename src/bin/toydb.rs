#![warn(clippy::all)]

extern crate config;
extern crate serde;
extern crate simplelog;
extern crate toydb;

#[macro_use]
extern crate serde_derive;

fn main() -> Result<(), toydb::Error> {
    let cfg = Config::new()?;
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
    fn new() -> Result<Self, config::ConfigError> {
        let mut c = config::Config::new();
        c.set_default("id", "toydb")?;
        c.set_default("listen", "0.0.0.0:9605")?;
        c.set_default("threads", 4)?;
        c.set_default("log_level", "info")?;

        c.merge(config::File::with_name("config/toydb.yaml"))?;
        c.merge(config::Environment::with_prefix("TOYDB"))?;
        c.try_into()
    }
}
