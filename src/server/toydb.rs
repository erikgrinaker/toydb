use crate::raft::Raft;
use crate::service;
use crate::sql;
use crate::sql::types::{Row, Value};
use crate::utility::{deserialize, serialize};
use crate::Error;
use sql::engine::{Engine, Transaction};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    GetTable(String),
    ListTables,
    Status,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
    GetTable(sql::Table),
    ListTables(Vec<String>),
    Status(Status),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub id: String,
    pub version: String,
}

pub struct ToyDB {
    pub id: String,
    pub raft: Raft,
    pub engine: sql::engine::Raft,
}

impl service::ToyDB for ToyDB {
    fn call(
        &self,
        _: grpc::RequestOptions,
        req: service::Request,
    ) -> grpc::SingleResponse<service::Result> {
        let mut resp = service::Result::new();
        match deserialize(&req.body) {
            Ok(req) => match self.call(req) {
                Ok(r) => resp.ok = serialize(&r).unwrap(),
                Err(e) => resp.err = serialize(&e).unwrap(),
            },
            Err(e) => {
                resp.err = serialize(&Error::Value(format!(
                    "Failed to deserialize request request: {}",
                    e
                )))
                .unwrap()
            }
        }
        grpc::SingleResponse::completed(resp)
    }

    fn query(
        &self,
        _: grpc::RequestOptions,
        req: service::QueryRequest,
    ) -> grpc::StreamingResponse<service::Row> {
        let result = match self
            .engine
            .session(Some(req.txn_id).filter(|&id| id > 0))
            .unwrap()
            .execute(&req.query)
        {
            Ok(result) => result,
            Err(err) => {
                return grpc::StreamingResponse::completed(vec![service::Row {
                    error: Self::error_to_protobuf(err),
                    ..Default::default()
                }])
            }
        };
        let mut metadata = grpc::Metadata::new();
        metadata
            .add(grpc::MetadataKey::from("columns"), serialize(&result.columns()).unwrap().into());
        if let Some(effect) = result.effect() {
            metadata.add(grpc::MetadataKey::from("effect"), serialize(&effect).unwrap().into());
        }
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
}

impl ToyDB {
    fn call(&self, req: Request) -> Result<Response, Error> {
        Ok(match req {
            Request::GetTable(table) => Response::GetTable(self.get_table(&table)?),
            Request::ListTables => Response::ListTables(self.list_tables()?),
            Request::Status => Response::Status(Status {
                id: self.id.clone(),
                version: env!("CARGO_PKG_VERSION").into(),
            }),
        })
    }

    fn get_table(&self, name: &str) -> Result<sql::Table, Error> {
        self.engine.with_txn(sql::Mode::ReadOnly, |txn| match txn.read_table(name)? {
            Some(t) => Ok(t),
            None => Err(Error::Value(format!("Table {} does not exist", name))),
        })
    }

    fn list_tables(&self) -> Result<Vec<String>, Error> {
        self.engine
            .with_txn(sql::Mode::ReadOnly, |txn| Ok(txn.scan_tables()?.map(|t| t.name).collect()))
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
