use crate::raft::Raft;
use crate::service;
use crate::sql::types::{Row, Value};
use crate::sql;
use crate::utility::serialize;
use crate::Error;

pub struct ToyDB {
    pub id: String,
    pub raft: Raft,
    pub storage: Box<sql::Storage>,
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

    fn list_tables(
        &self,
        _: grpc::RequestOptions,
        _: service::Empty,
    ) -> grpc::SingleResponse<service::ListTablesResponse> {
        let mut resp = service::ListTablesResponse::new();
        match self.storage.list_tables() {
            Ok(tables) => resp.name = protobuf::RepeatedField::from_vec(tables),
            Err(err) => resp.error = Self::error_to_protobuf(err),
        };
        grpc::SingleResponse::completed(resp)
    }

    fn query(
        &self,
        _: grpc::RequestOptions,
        req: service::QueryRequest,
    ) -> grpc::StreamingResponse<service::Row> {
        let result = match self.execute(&req.query) {
            Ok(result) => result,
            Err(err) => {
                return grpc::StreamingResponse::completed(vec![service::Row {
                    error: Self::error_to_protobuf(err),
                    ..Default::default()
                }])
            }
        };
        let mut metadata = grpc::Metadata::new();
        // FIXME metadata.add(grpc::MetadataKey::from("columns"), serialize(&plan.columns).unwrap().into());
        metadata.add(
            grpc::MetadataKey::from("columns"),
            serialize(Vec::<String>::new()).unwrap().into(),
        );
        grpc::StreamingResponse::iter_with_metadata(
            metadata,
            result.map(|r| match r {
                Ok(row) => Self::row_to_protobuf(row),
                Err(err) => {
                    service::Row { error: Self::error_to_protobuf(err), ..Default::default() }
                }
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
    fn execute(&self, query: &str) -> Result<sql::ResultSet, Error> {
        sql::Plan::build(sql::Parser::new(query).parse()?)?.execute(sql::Context{
            storage: self.storage.clone(),
        })
    }

    /// Converts an error into a protobuf object
    fn error_to_protobuf(err: Error) -> protobuf::SingularPtrField<service::Error> {
        protobuf::SingularPtrField::from(Some(service::Error {
            message: err.to_string(),
            ..Default::default()
        }))
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
