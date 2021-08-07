/*
 * toydb is the toyDB server. It takes configuration via a configuration file, command-line
 * parameters, and environment variables, then starts up a toyDB TCP server that communicates with
 * SQL clients (port 9605) and Raft peers (port 9705).
 */

#![warn(clippy::all)]

use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version};
use serde_derive::Deserialize;
use std::collections::HashMap;
use toydb::error::{Error, Result};
use toydb::storage;
use toydb::Server;

#[tokio::main]
async fn main() -> Result<()> {
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

    let path = std::path::Path::new(&cfg.data_dir);
    let raft_store: Box<dyn storage::log::Store> = match cfg.storage_raft.as_str() {
        "hybrid" | "" => Box::new(storage::log::Hybrid::new(path, cfg.sync)?),
        "memory" => Box::new(storage::log::Memory::new()),
        name => return Err(Error::Config(format!("Unknown Raft storage engine {}", name))),
    };
    let sql_store: Box<dyn storage::kv::Store> = match cfg.storage_sql.as_str() {
        "memory" | "" => Box::new(storage::kv::Memory::new()),
        "stdmemory" => Box::new(storage::kv::StdMemory::new()),
        name => return Err(Error::Config(format!("Unknown SQL storage engine {}", name))),
    };

    Server::new(&cfg.id, cfg.peers, raft_store, sql_store)
        .await?
        .listen(&cfg.listen_sql, &cfg.listen_raft)
        .await?
        .serve()
        .await
}

#[derive(Debug, Deserialize)]
struct Config {
    id: String,
    peers: HashMap<String, String>,
    listen_sql: String,
    listen_raft: String,
    log_level: String,
    data_dir: String,
    sync: bool,
    storage_raft: String,
    storage_sql: String,
}

impl Config {
    fn new(file: &str) -> Result<Self> {
        let mut c = config::Config::new();
        c.set_default("id", "toydb")?;
        c.set_default("peers", HashMap::<String, String>::new())?;
        c.set_default("listen_sql", "0.0.0.0:9605")?;
        c.set_default("listen_raft", "0.0.0.0:9705")?;
        c.set_default("log_level", "info")?;
        c.set_default("data_dir", "/var/lib/toydb")?;
        c.set_default("sync", true)?;
        c.set_default("storage_raft", "hybrid")?;
        c.set_default("storage_sql", "memory")?;

        c.merge(config::File::with_name(file))?;
        c.merge(config::Environment::with_prefix("TOYDB"))?;
        Ok(c.try_into()?)
    }
}
