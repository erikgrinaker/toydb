use crate::client;
use crate::raft::Raft;
use crate::service;
use crate::sql;
use crate::sql::types::{Row, Value};
use crate::utility::serialize;
use crate::Error;
use sql::engine::{Engine, Mode, Transaction};

pub struct ToyDB {
    pub id: String,
    pub raft: Raft,
    pub engine: sql::engine::Raft,
}

impl service::ToyDB for ToyDB {
    fn get_table(
        &self,
        _: grpc::RequestOptions,
        req: service::GetTableRequest,
    ) -> grpc::SingleResponse<service::GetTableResponse> {
        let mut resp = service::GetTableResponse::new();
        let txn = self.engine.begin().unwrap();
        match txn.read_table(&req.name) {
            Ok(Some(schema)) => resp.sql = schema.as_sql(),
            Ok(None) => {
                resp.error = Self::error_to_protobuf(Error::Value(format!(
                    "Table {} does not exist",
                    req.name
                )))
            }
            Err(err) => resp.error = Self::error_to_protobuf(err),
        };
        txn.rollback().unwrap();
        grpc::SingleResponse::completed(resp)
    }

    fn list_tables(
        &self,
        _: grpc::RequestOptions,
        _: service::Empty,
    ) -> grpc::SingleResponse<service::ListTablesResponse> {
        let mut resp = service::ListTablesResponse::new();
        let txn = self.engine.begin().unwrap();
        match txn.scan_tables() {
            Ok(iter) => {
                resp.name = protobuf::RepeatedField::from_vec(iter.map(|s| s.name).collect())
            }
            Err(err) => resp.error = Self::error_to_protobuf(err),
        };
        txn.rollback().unwrap();
        grpc::SingleResponse::completed(resp)
    }

    fn query(
        &self,
        _: grpc::RequestOptions,
        req: service::QueryRequest,
    ) -> grpc::StreamingResponse<service::Row> {
        let result = match self.execute(Some(req.txn_id).filter(|&id| id > 0), &req.query) {
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
        if let Some(effect) = &result.effect {
            metadata.add(grpc::MetadataKey::from("effect"), serialize(effect).unwrap().into());
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
    fn execute(&self, txn_id: Option<u64>, query: &str) -> Result<sql::ResultSet, Error> {
        let statement = sql::Parser::new(query).parse()?;

        // FIXME Needs to return effect for DDL statements as well
        if let Some(txn_id) = txn_id {
            let mut txn = self.engine.resume(txn_id)?;
            match statement {
                sql::ast::Statement::Begin { .. } => {
                    Err(Error::Value("Already in a transaction".into()))
                }
                sql::ast::Statement::Commit => {
                    let mut rs = sql::ResultSet::empty();
                    rs.effect = Some(client::Effect::Commit(txn.id()));
                    txn.commit()?;
                    Ok(rs)
                }
                sql::ast::Statement::Rollback => {
                    let mut rs = sql::ResultSet::empty();
                    rs.effect = Some(client::Effect::Rollback(txn.id()));
                    txn.rollback()?;
                    Ok(rs)
                }
                _ => {
                    let rs = sql::Plan::build(statement)?
                        .optimize()?
                        .execute(sql::Context { txn: &mut txn })?;
                    Ok(rs)
                }
            }
        } else {
            match statement {
                sql::ast::Statement::Begin { readonly: false, version } => {
                    if version.is_some() {
                        return Err(Error::Value(
                            "Can't start read-write transaction in a given version".into(),
                        ));
                    }
                    let txn = self.engine.begin()?;
                    let mut rs = sql::ResultSet::empty();
                    rs.effect = Some(client::Effect::Begin { id: txn.id(), mode: txn.mode() });
                    Ok(rs)
                }
                sql::ast::Statement::Begin { readonly: true, version: None } => {
                    let txn = self.engine.begin_with_mode(Mode::ReadOnly)?;
                    let mut rs = sql::ResultSet::empty();
                    rs.effect = Some(client::Effect::Begin { id: txn.id(), mode: txn.mode() });
                    Ok(rs)
                }
                sql::ast::Statement::Begin { readonly: true, version: Some(version) } => {
                    let txn = self.engine.begin_with_mode(Mode::Snapshot { version })?;
                    let mut rs = sql::ResultSet::empty();
                    rs.effect = Some(client::Effect::Begin { id: txn.id(), mode: txn.mode() });
                    Ok(rs)
                }
                sql::ast::Statement::Commit | sql::ast::Statement::Rollback => {
                    Err(Error::Value("Not in a transaction".into()))
                }
                sql::ast::Statement::Select { .. } => {
                    let mut txn = self.engine.begin_with_mode(Mode::ReadOnly)?;
                    let result = sql::Plan::build(statement)?
                        .optimize()?
                        .execute(sql::Context { txn: &mut txn });
                    txn.commit()?;
                    result
                }
                _ => {
                    let mut txn = self.engine.begin()?;
                    let result = sql::Plan::build(statement)?
                        .optimize()?
                        .execute(sql::Context { txn: &mut txn });
                    txn.commit()?;
                    result
                }
            }
        }
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
