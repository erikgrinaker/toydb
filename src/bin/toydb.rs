/*
 * toydb is the toyDB server. It takes configuration via a configuration file, command-line
 * parameters, and environment variables, then starts up a toyDB TCP server that communicates with
 * SQL clients (port 9605) and Raft peers (port 9705).
 */

#![warn(clippy::all)]

use serde_derive::Deserialize;
use std::collections::HashMap;
use toydb::errinput;
use toydb::error::Result;
use toydb::raft;
use toydb::sql;
use toydb::storage;
use toydb::Server;

const COMPACT_MIN_BYTES: u64 = 1024 * 1024;

fn main() -> Result<()> {
    let args = clap::command!()
        .arg(
            clap::Arg::new("config")
                .short('c')
                .long("config")
                .help("Configuration file path")
                .default_value("config/toydb.yaml"),
        )
        .get_matches();
    let cfg = Config::new(args.get_one::<String>("config").unwrap().as_ref())?;

    let loglevel = cfg.log_level.parse::<simplelog::LevelFilter>()?;
    let mut logconfig = simplelog::ConfigBuilder::new();
    if loglevel != simplelog::LevelFilter::Debug {
        logconfig.add_filter_allow_str("toydb");
    }
    simplelog::SimpleLogger::init(loglevel, logconfig.build())?;

    let path = std::path::Path::new(&cfg.data_dir);
    let raft_log = match cfg.storage_raft.as_str() {
        "bitcask" | "" => raft::Log::new(Box::new(storage::BitCask::new_compact(
            path.join("log"),
            cfg.compact_threshold,
            COMPACT_MIN_BYTES,
        )?))?,
        "memory" => raft::Log::new(Box::new(storage::Memory::new()))?,
        name => return errinput!("invalid Raft storage engine {name}"),
    };
    let raft_state: Box<dyn raft::State> = match cfg.storage_sql.as_str() {
        "bitcask" | "" => {
            let engine = storage::BitCask::new_compact(
                path.join("state"),
                cfg.compact_threshold,
                COMPACT_MIN_BYTES,
            )?;
            Box::new(sql::engine::Raft::new_state(engine)?)
        }
        "memory" => {
            let engine = storage::Memory::new();
            Box::new(sql::engine::Raft::new_state(engine)?)
        }
        name => return errinput!("invalid SQL storage engine {name}"),
    };

    Server::new(cfg.id, cfg.peers, raft_log, raft_state)?.serve(&cfg.listen_raft, &cfg.listen_sql)
}

#[derive(Debug, Deserialize)]
struct Config {
    id: raft::NodeID,
    peers: HashMap<raft::NodeID, String>,
    listen_sql: String,
    listen_raft: String,
    log_level: String,
    data_dir: String,
    compact_threshold: f64,
    storage_raft: String,
    storage_sql: String,
}

impl Config {
    fn new(file: &str) -> Result<Self> {
        Ok(config::Config::builder()
            .set_default("id", "1")?
            .set_default("listen_sql", "0.0.0.0:9605")?
            .set_default("listen_raft", "0.0.0.0:9705")?
            .set_default("log_level", "info")?
            .set_default("data_dir", "data")?
            .set_default("compact_threshold", 0.2)?
            .set_default("storage_raft", "bitcask")?
            .set_default("storage_sql", "bitcask")?
            .add_source(config::File::with_name(file))
            .add_source(config::Environment::with_prefix("TOYDB"))
            .build()?
            .try_deserialize()?)
    }
}
