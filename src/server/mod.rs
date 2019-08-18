mod raft;
mod toydb;

use self::toydb::ToyDB;
use crate::error::Error;
use crate::kv;
use crate::raft::Raft;
use crate::service;
use crate::state;

use std::collections::HashMap;

pub struct Server {
    pub id: String,
    pub addr: String,
    pub threads: usize,
    pub peers: HashMap<String, std::net::SocketAddr>,
    pub data_dir: String,
}

impl Server {
    pub fn listen(&self) -> Result<(), Error> {
        info!("Starting ToyDB node with ID {} on {}", self.id, self.addr);
        let mut server = grpc::ServerBuilder::new_plain();
        server.http.set_addr(&self.addr)?;
        server.http.set_cpu_pool_threads(self.threads);
        let data_path = std::path::Path::new(&self.data_dir);
        std::fs::create_dir_all(data_path)?;

        let raft_transport = raft::GRPC::new(self.peers.clone())?;
        server.add_service(service::RaftServer::new_service_def(raft_transport.build_service()?));
        let raft = Raft::start(
            &self.id,
            self.peers.keys().cloned().collect(),
            state::State::new(kv::File::new(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(data_path.join("state"))?,
            )?),
            kv::File::new(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(data_path.join("raft"))?,
            )?,
            raft_transport,
        )?;

        server.add_service(service::ToyDBServer::new_service_def(ToyDB {
            id: self.id.clone(),
            raft: raft.clone(),
        }));
        let _s = server.build()?;

        raft.join()
    }
}
