use crate::error::Error;
use crate::service;

pub struct Server {
    pub id: String,
    pub addr: String,
    pub threads: usize,
}

impl Server {
    pub fn listen(&self) -> Result<(), Error> {
        let mut server = grpc::ServerBuilder::new_plain();
        server.http.set_addr(&self.addr)?;
        server.http.set_cpu_pool_threads(self.threads);
        server.add_service(service::ToyDBServer::new_service_def(ToyDB { id: self.id.clone() }));
        let _s = server.build()?;

        loop {
            std::thread::park();
        }
    }
}

struct ToyDB {
    id: String,
}

impl service::ToyDB for ToyDB {
    fn status(
        &self,
        _: grpc::RequestOptions,
        _: service::StatusRequest,
    ) -> grpc::SingleResponse<service::StatusResponse> {
        grpc::SingleResponse::completed(service::StatusResponse {
            id: self.id.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            time: match std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|t| t.as_secs())
            {
                Ok(t) => t as i64,
                Err(e) => return grpc::SingleResponse::err(grpc::Error::Panic(format!("{}", e))),
            },
            ..Default::default()
        })
    }
}
