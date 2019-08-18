use crate::service;
use crate::raft::Raft;
use crate::state;

pub struct ToyDB {
    pub id: String,
    pub raft: Raft,
}

impl service::ToyDB for ToyDB {
    fn get(
        &self,
        _: grpc::RequestOptions,
        req: service::GetRequest,
    ) -> grpc::SingleResponse<service::GetResponse> {
        let value =
            self.raft.read(state::serialize(state::Read::Get(req.key.clone())).unwrap()).unwrap();
        grpc::SingleResponse::completed(service::GetResponse {
            key: req.key,
            value: state::deserialize(value).unwrap(),
            ..Default::default()
        })
    }

    fn set(
        &self,
        _: grpc::RequestOptions,
        req: service::SetRequest,
    ) -> grpc::SingleResponse<service::SetResponse> {
        self.raft
            .mutate(
                state::serialize(state::Mutation::Set {
                    key: req.key,
                    value: state::serialize(req.value).unwrap(),
                })
                .unwrap(),
            )
            .unwrap();
        grpc::SingleResponse::completed(service::SetResponse { ..Default::default() })
    }

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
