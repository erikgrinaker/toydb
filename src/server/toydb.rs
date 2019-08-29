use crate::raft::Raft;
use crate::service;
use crate::sql::types::{Row, Value};
use crate::sql::{Parser, Planner, Storage};
use crate::Error;

pub struct ToyDB {
    pub id: String,
    pub raft: Raft,
    pub storage: Box<Storage>,
}

impl service::ToyDB for ToyDB {
    fn query(
        &self,
        _: grpc::RequestOptions,
        req: service::QueryRequest,
    ) -> grpc::StreamingResponse<service::Row> {
        let plan = Planner::new(self.storage.clone())
            .build(Parser::new(&req.query).parse().unwrap())
            .unwrap();
        let mut metadata = grpc::Metadata::new();
        metadata.add(
            grpc::MetadataKey::from("columns"),
            Self::serialize(&plan.columns).unwrap().into(),
        );
        // FIXME This needs to handle errors
        grpc::StreamingResponse::iter_with_metadata(
            metadata,
            plan.map(|row| Self::row_to_protobuf(row.unwrap())),
        )
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

impl ToyDB {
    /// Converts a row into a protobuf row
    fn row_to_protobuf(row: Row) -> service::Row {
        service::Row {
            field: row.into_iter().map(Self::value_to_protobuf).collect(),
            ..Default::default()
        }
    }

    /// Converts a value into a protobuf field
    fn value_to_protobuf(value: Value) -> service::Field {
        service::Field {
            value: match value {
                Value::Null => None,
                Value::Boolean(b) => Some(service::Field_oneof_value::boolean(b)),
                Value::Float(f) => Some(service::Field_oneof_value::float(f)),
                Value::Integer(i) => Some(service::Field_oneof_value::integer(i)),
                Value::String(s) => Some(service::Field_oneof_value::string(s)),
            },
            ..Default::default()
        }
    }

    /// Serializes a value into a byte buffer
    fn serialize<V: serde::Serialize>(value: V) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        value.serialize(&mut rmps::Serializer::new(&mut bytes))?;
        Ok(bytes)
    }
}
