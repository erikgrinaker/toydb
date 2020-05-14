#![warn(clippy::all)]

use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version};
use serde_derive::Deserialize;
use std::collections::HashMap;
use toydb::Server;

#[tokio::main]
async fn main() -> Result<(), toydb::Error> {
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

    let loglevel = cfg.log_level.parse::<simplelog::LevelFilter>()?;
    let mut logconfig = simplelog::ConfigBuilder::new();
    if loglevel != simplelog::LevelFilter::Debug {
        logconfig.add_filter_allow_str("toydb");
    }
    simplelog::SimpleLogger::init(loglevel, logconfig.build())?;

    Server::new(&cfg.id, cfg.peers, &cfg.data_dir, cfg.sync)
        .await?
        .listen(&cfg.listen_sql, &cfg.listen_raft)
        .await?
        .serve()
        .await
}

#[derive(Debug, Deserialize)]
struct Config {
    id: String,
    listen_sql: String,
    listen_raft: String,
    log_level: String,
    data_dir: String,
    sync: bool,
    peers: HashMap<String, String>,
}

impl Config {
    fn new(file: &str) -> Result<Self, config::ConfigError> {
        let mut c = config::Config::new();
        c.set_default("id", "toydb")?;
        c.set_default("listen_sql", "0.0.0.0:9605")?;
        c.set_default("listen_raft", "0.0.0.0:9705")?;
        c.set_default("log_level", "info")?;
        c.set_default("data_dir", "/var/lib/toydb")?;
        c.set_default("sync", true)?;

        c.merge(config::File::with_name(file))?;
        c.merge(config::Environment::with_prefix("TOYDB"))?;
        c.try_into()
    }
}
