mod raft;
mod toydb;

pub use self::toydb::{Request, Response, Status};

use self::toydb::ToyDB;
use crate::error::Error;
use crate::kv;
use crate::raft::Raft;
use crate::service;
use crate::sql;

use std::collections::HashMap;

pub struct Server {
    id: String,
    peers: HashMap<String, std::net::SocketAddr>,
    data_dir: String,
}

impl Server {
    pub fn new(
        id: &str,
        peers: HashMap<String, std::net::SocketAddr>,
        data_dir: &str,
    ) -> Result<Self, Error> {
        Ok(Server { id: id.into(), peers, data_dir: data_dir.into() })
    }

    pub async fn listen(self, sql_addr: String, raft_addr: String) -> Result<(), Error> {
        info!("Starting ToyDB node {} on {} (SQL) and {} (Raft)", self.id, sql_addr, raft_addr);
        let mut server = grpc::ServerBuilder::new_plain();
        server.http.set_addr(raft_addr)?;
        server.http.set_cpu_pool_threads(4);
        let data_path = std::path::Path::new(&self.data_dir);
        std::fs::create_dir_all(data_path)?;

        let raft_transport = raft::GRPC::new(self.peers.clone())?;
        server.add_service(service::RaftServer::new_service_def(raft_transport.build_service()?));
        let raft = Raft::start(
            &self.id,
            self.peers.keys().cloned().collect(),
            sql::engine::Raft::new_state(kv::MVCC::new(kv::storage::File::new(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(data_path.join("state"))?,
            )?)),
            kv::Simple::new(kv::storage::BLog::new(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(data_path.join("raft"))?,
            )?),
            raft_transport,
        )?;

        let _s = server.build()?;

        ToyDB { id: self.id.clone(), engine: sql::engine::Raft::new(raft.clone()) }
            .listen(&sql_addr)
            .await?;

        raft.shutdown()?;

        Ok(())
    }
}
