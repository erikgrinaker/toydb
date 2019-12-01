use crate::client;
use crate::raft::Raft;
use crate::service;
use crate::sql;
use crate::sql::types::{Row, Value};
use crate::utility::serialize;
use crate::Error;
use sql::engine::Engine;
use sql::engine::Transaction;

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
            Ok(Some(schema)) => resp.sql = schema.to_query(),
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
        match txn.list_tables() {
            Ok(tables) => {
                resp.name =
                    protobuf::RepeatedField::from_vec(tables.into_iter().map(|s| s.name).collect())
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
        let mode = match req.mode {
            Some(service::QueryRequest_oneof_mode::txn_id(id)) => client::Mode::Transaction(id),
            Some(service::QueryRequest_oneof_mode::snapshot_version(version)) => {
                client::Mode::Snapshot(version)
            }
            None => client::Mode::Statement,
        };
        let result = match self.execute(mode, &req.query) {
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
            .add(grpc::MetadataKey::from("columns"), serialize(result.columns()).unwrap().into());
        if let Some(effect) = result.effect.clone() {
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
    fn execute(&self, mode: client::Mode, query: &str) -> Result<sql::types::ResultSet, Error> {
        let statement = sql::Parser::new(query).parse()?;

        // FIXME Needs to return effect for DDL statements as well
        match mode {
            // Ongoing transaction
            client::Mode::Transaction(txn_id) => {
                let mut txn = self.engine.resume(txn_id)?;
                match statement {
                    sql::ast::Statement::Begin { .. } => {
                        Err(Error::Value("Already in a transaction".into()))
                    }
                    sql::ast::Statement::Commit => {
                        let mut rs = sql::types::ResultSet::empty();
                        rs.effect = Some(client::Effect::Commit(txn.id()));
                        txn.commit()?;
                        Ok(rs)
                    }
                    sql::ast::Statement::Rollback => {
                        let mut rs = sql::types::ResultSet::empty();
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
            }
            // Ongoing read-only snapshot transaction
            client::Mode::Snapshot(version) => {
                let mut txn = self.engine.snapshot(Some(version))?;
                match statement {
                    sql::ast::Statement::Begin { .. } => {
                        Err(Error::Value("Already in a transaction".into()))
                    }
                    sql::ast::Statement::Commit => {
                        let mut rs = sql::types::ResultSet::empty();
                        rs.effect = Some(client::Effect::Commit(txn.id()));
                        txn.commit()?;
                        Ok(rs)
                    }
                    sql::ast::Statement::Rollback => {
                        let mut rs = sql::types::ResultSet::empty();
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
            }
            client::Mode::Statement => match statement {
                sql::ast::Statement::Begin { readonly: false } => {
                    let txn = self.engine.begin()?;
                    let mut rs = sql::types::ResultSet::empty();
                    rs.effect = Some(client::Effect::Begin { id: txn.id(), readonly: false });
                    Ok(rs)
                }
                sql::ast::Statement::Begin { readonly: true } => {
                    let txn = self.engine.snapshot(None)?;
                    let mut rs = sql::types::ResultSet::empty();
                    rs.effect = Some(client::Effect::Begin { id: txn.id(), readonly: true });
                    Ok(rs)
                }
                sql::ast::Statement::Commit | sql::ast::Statement::Rollback => {
                    Err(Error::Value("Not in a transaction".into()))
                }
                sql::ast::Statement::Select { .. } => {
                    let mut txn = self.engine.snapshot(None)?;
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
            },
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
