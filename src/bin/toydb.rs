//! The toyDB server. Takes configuration from a config file (default
//! config/toydb.yaml) or corresponding TOYDB_ environment variables. Listens
//! for SQL clients (default port 9605) and Raft connections from other toyDB
//! peers (default port 9705). The Raft log and SQL database are stored at
//! data/raft and data/sql by default.
//!
//! Use the toysql command-line client to connect to the server.

#![warn(clippy::all)]

use toydb::errinput;
use toydb::error::Result;
use toydb::raft;
use toydb::sql;
use toydb::storage;
use toydb::Server;

use clap::Parser as _;
use serde::Deserialize;
use std::collections::HashMap;

fn main() {
    if let Err(error) = Command::parse().run() {
        eprintln!("Error: {error}")
    }
}

/// The toyDB server configuration. Can be provided via config file (default
/// config/toydb.yaml) or TOYDB_ environment variables.
#[derive(Debug, Deserialize)]
struct Config {
    /// The node ID. Must be unique in the cluster.
    id: raft::NodeID,
    /// The other nodes in the cluster, and their Raft TCP addresses.
    peers: HashMap<raft::NodeID, String>,
    /// The Raft listen address.
    listen_raft: String,
    /// The SQL listen address.
    listen_sql: String,
    /// The log level.
    log_level: String,
    /// The path to this node's data directory. The Raft log is stored in
    /// the file "raft", and the SQL state machine in "sql".
    data_dir: String,
    /// The Raft storage engine: bitcask or memory.
    storage_raft: String,
    /// The SQL storage engine: bitcask or memory.
    storage_sql: String,
    /// The garbage fraction threshold at which to trigger compaction.
    compact_threshold: f64,
    /// The minimum bytes of garbage before triggering compaction.
    compact_min_bytes: u64,
}

impl Config {
    /// Loads the configuration from the given file.
    fn load(file: &str) -> Result<Self> {
        Ok(config::Config::builder()
            .set_default("id", "1")?
            .set_default("listen_sql", "localhost:9605")?
            .set_default("listen_raft", "localhost:9705")?
            .set_default("log_level", "info")?
            .set_default("data_dir", "data")?
            .set_default("storage_raft", "bitcask")?
            .set_default("storage_sql", "bitcask")?
            .set_default("compact_threshold", 0.2)?
            .set_default("compact_min_bytes", 1_000_000)?
            .add_source(config::File::with_name(file))
            .add_source(config::Environment::with_prefix("TOYDB"))
            .build()?
            .try_deserialize()?)
    }
}

/// The toyDB server command.
#[derive(clap::Parser)]
#[command(about = "Starts a toyDB server.", version, propagate_version = true)]
struct Command {
    /// The configuration file path.
    #[arg(short = 'c', long, default_value = "config/toydb.yaml")]
    config: String,
}

impl Command {
    /// Runs the toyDB server.
    fn run(self) -> Result<()> {
        // Load the configuration.
        let cfg = Config::load(&self.config)?;

        // Initialize logging.
        let loglevel = cfg.log_level.parse()?;
        let mut logconfig = simplelog::ConfigBuilder::new();
        if loglevel != simplelog::LevelFilter::Debug {
            logconfig.add_filter_allow_str("toydb");
        }
        simplelog::SimpleLogger::init(loglevel, logconfig.build())?;

        // Initialize the Raft log storage engine.
        let datadir = std::path::Path::new(&cfg.data_dir);
        let raft_log = match cfg.storage_raft.as_str() {
            "bitcask" | "" => {
                let engine = storage::BitCask::new_compact(
                    datadir.join("raft"),
                    cfg.compact_threshold,
                    cfg.compact_min_bytes,
                )?;
                raft::Log::new(Box::new(engine))?
            }
            "memory" => raft::Log::new(Box::new(storage::Memory::new()))?,
            name => return errinput!("invalid Raft storage engine {name}"),
        };

        // Initialize the SQL storage engine.
        let raft_state: Box<dyn raft::State> = match cfg.storage_sql.as_str() {
            "bitcask" | "" => {
                let engine = storage::BitCask::new_compact(
                    datadir.join("sql"),
                    cfg.compact_threshold,
                    cfg.compact_min_bytes,
                )?;
                Box::new(sql::engine::Raft::new_state(engine)?)
            }
            "memory" => {
                let engine = storage::Memory::new();
                Box::new(sql::engine::Raft::new_state(engine)?)
            }
            name => return errinput!("invalid SQL storage engine {name}"),
        };

        // Start the server.
        Server::new(cfg.id, cfg.peers, raft_log, raft_state)?
            .serve(&cfg.listen_raft, &cfg.listen_sql)
    }
}
