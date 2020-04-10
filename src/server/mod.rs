mod raft;
mod toydb;

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
    grpc: Option<grpc::Server>,
    raft: Option<Raft>,
}

impl Server {
    pub fn new(
        id: &str,
        peers: HashMap<String, std::net::SocketAddr>,
        data_dir: &str,
    ) -> Result<Self, Error> {
        Ok(Server { id: id.into(), peers, data_dir: data_dir.into(), grpc: None, raft: None })
    }

    pub fn listen(&mut self, addr: &str, threads: usize) -> Result<(), Error> {
        info!("Starting ToyDB node with ID {} on {}", self.id, addr);
        let mut server = grpc::ServerBuilder::new_plain();
        server.http.set_addr(addr)?;
        server.http.set_cpu_pool_threads(threads);
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
            kv::Simple::new(kv::storage::File::new(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(data_path.join("raft"))?,
            )?),
            raft_transport,
        )?;

        server.add_service(service::ToyDBServer::new_service_def(ToyDB {
            id: self.id.clone(),
            raft: raft.clone(),
            engine: sql::engine::Raft::new(raft.clone()),
        }));

        self.grpc = Some(server.build()?);
        self.raft = Some(raft);
        Ok(())
    }

    pub fn join(self) -> Result<(), Error> {
        self.raft.map(|r| r.join()).unwrap_or(Ok(()))
    }

    pub fn shutdown(self) -> Result<(), Error> {
        self.raft.map(|r| r.shutdown()).unwrap_or(Ok(()))
    }
}
