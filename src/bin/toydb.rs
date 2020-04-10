#![warn(clippy::all)]

#[macro_use]
extern crate clap;
extern crate config;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate simplelog;
extern crate toydb;

use std::collections::HashMap;

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

    let loglevel = cfg.log_level.parse::<simplelog::LevelFilter>()?;
    let mut logconfig = simplelog::ConfigBuilder::new();
    if loglevel != simplelog::LevelFilter::Debug {
        logconfig.add_filter_allow_str("toydb");
    }
    simplelog::SimpleLogger::init(loglevel, logconfig.build())?;

    let mut server = toydb::Server::new(&cfg.id, cfg.parse_peers()?, &cfg.data_dir)?;
    server.listen(&cfg.listen, 8)?;
    server.join()
}

#[derive(Debug, Deserialize)]
struct Config {
    id: String,
    listen: String,
    log_level: String,
    data_dir: String,
    peers: HashMap<String, String>,
}

impl Config {
    fn new(file: &str) -> Result<Self, config::ConfigError> {
        let mut c = config::Config::new();
        c.set_default("id", "toydb")?;
        c.set_default("listen", "0.0.0.0:9605")?;
        c.set_default("log_level", "info")?;
        c.set_default("data_dir", "/var/lib/toydb")?;

        c.merge(config::File::with_name(file))?;
        c.merge(config::Environment::with_prefix("TOYDB"))?;
        c.try_into()
    }

    fn parse_peers(&self) -> Result<HashMap<String, std::net::SocketAddr>, toydb::Error> {
        let mut peers = HashMap::new();
        for (id, address) in self.peers.iter() {
            peers.insert(
                id.clone(),
                if let Ok(sa) = address.parse::<std::net::SocketAddr>() {
                    sa
                } else {
                    let ip = address.parse::<std::net::IpAddr>()?;
                    std::net::SocketAddr::new(ip, 9605)
                },
            );
        }
        Ok(peers)
    }
}
