use crate::Error;
use crate::raft::Raft;
use crate::service;
use crate::sql::types::{Row, Value};
use crate::sql::{Parser, Plan, Planner, Storage};
use crate::utility::serialize;

pub struct ToyDB {
    pub id: String,
    pub raft: Raft,
    pub storage: Box<Storage>,
}

impl service::ToyDB for ToyDB {
    fn get_table(
        &self,
        _: grpc::RequestOptions,
        req: service::GetTableRequest,
    ) -> grpc::SingleResponse<service::GetTableResponse> {
        let mut resp = service::GetTableResponse::new();
        match self.storage.get_table(&req.name) {
            Ok(schema) => resp.sql = schema.to_query(),
            Err(err) => resp.error = Self::error_to_protobuf(err),
        };
        grpc::SingleResponse::completed(resp)
    }

    fn query(
        &self,
        _: grpc::RequestOptions,
        req: service::QueryRequest,
    ) -> grpc::StreamingResponse<service::Row> {
        let plan = match self.execute(&req.query) {
            Ok(plan) => plan,
            Err(err) => return grpc::StreamingResponse::completed(vec![service::Row{
                error: Self::error_to_protobuf(err),
                ..Default::default()
            }])
        };
        let mut metadata = grpc::Metadata::new();
        metadata.add(grpc::MetadataKey::from("columns"), serialize(&plan.columns).unwrap().into());
        grpc::StreamingResponse::iter_with_metadata(
            metadata,
            plan.map(|result| match result {
                Ok(row) => Self::row_to_protobuf(row),
                Err(err) => service::Row{error: Self::error_to_protobuf(err), ..Default::default()},
            }),
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
            ..Default::default()
        })
    }
}

impl ToyDB {
    /// Executes an SQL statement
    fn execute(&self, query: &str) -> Result<Plan, Error> {
        Planner::new(self.storage.clone()).build(Parser::new(query).parse()?)
    }

    /// Converts an error into a protobuf object
    fn error_to_protobuf(err: Error) -> protobuf::SingularPtrField<service::Error> {
        protobuf::SingularPtrField::from(Some(service::Error { message: err.to_string(), ..Default::default() }))
    }

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
}
