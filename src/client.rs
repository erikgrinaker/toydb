pub use crate::server::Status;
use crate::server::{Request, Response};
use crate::service;
pub use crate::sql::types::schema::Column;
pub use crate::sql::types::DataType;
pub use crate::sql::{Effect, Mode, ResultColumns, ResultSet, Row, Table, Value};
use crate::utility::{deserialize, serialize};
use crate::Error;
use grpc::ClientStubExt;
use service::ToyDB;

/// A ToyDB client
pub struct Client {
    client: service::ToyDBClient,
    txn: Option<(u64, Mode)>,
}

impl Client {
    /// Creates a new client
    pub fn new(host: &str, port: u16) -> Result<Self, Error> {
        Ok(Self {
            client: service::ToyDBClient::new_plain(host, port, grpc::ClientConf::new())?,
            txn: None,
        })
    }

    /// Call a server method
    fn call(&self, req: Request) -> Result<Response, Error> {
        let (_, resp, _) = self
            .client
            .call(
                grpc::RequestOptions::new(),
                service::Request { body: serialize(&req)?, ..Default::default() },
            )
            .wait()?;
        if !resp.err.is_empty() {
            Err(deserialize(&resp.err)?)
        } else {
            Ok(deserialize(&resp.ok)?)
        }
    }

    /// Fetches the table schema as SQL
    pub fn get_table(&self, table: &str) -> Result<Table, Error> {
        match self.call(Request::GetTable(table.into()))? {
            Response::GetTable(t) => Ok(t),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Lists database tables
    pub fn list_tables(&self) -> Result<Vec<String>, Error> {
        match self.call(Request::ListTables)? {
            Response::ListTables(t) => Ok(t),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// Returns the client transaction info, if any
    pub fn txn(&self) -> Option<(u64, Mode)> {
        self.txn.clone()
    }

    /// Runs a query
    pub fn query(&mut self, query: &str) -> Result<ResultSet, Error> {
        let (metadata, iter) = self
            .client
            .query(
                grpc::RequestOptions::new(),
                service::QueryRequest {
                    query: query.to_owned(),
                    txn_id: match self.txn {
                        Some((id, _)) => id,
                        None => 0,
                    },
                    ..Default::default()
                },
            )
            .wait()?;
        let rs = resultset_from_grpc(metadata, iter)?;
        match rs.effect() {
            Some(Effect::Begin { id, mode }) => self.txn = Some((id, mode)),
            Some(Effect::Commit { .. }) => self.txn = None,
            Some(Effect::Rollback { .. }) => self.txn = None,
            _ => {}
        }
        Ok(rs)
    }

    /// Checks server status
    pub fn status(&self) -> Result<Status, Error> {
        match self.call(Request::Status)? {
            Response::Status(s) => Ok(s),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }
}

/// Converts a protobuf error into a toyDB error
fn error_from_protobuf(err: protobuf::SingularPtrField<service::Error>) -> Result<(), Error> {
    match err.into_option() {
        Some(err) => Err(Error::Internal(err.message)),
        _ => Ok(()),
    }
}

/// Converts a GRPC result into a resultset
fn resultset_from_grpc(
    metadata: grpc::Metadata,
    rows: Box<dyn Iterator<Item = Result<service::Row, grpc::Error>> + Send>,
) -> Result<ResultSet, Error> {
    let rows = rows.map(|r| match r {
        Ok(protorow) => {
            if let Err(err) = error_from_protobuf(protorow.error.clone()) {
                Err(err)
            } else {
                Ok(row_from_protobuf(protorow))
            }
        }
        Err(err) => Err(err.into()),
    });
    let columns = match metadata.get("columns") {
        Some(c) => ResultColumns::from(deserialize(c)?),
        None => ResultColumns::new(Vec::new()),
    };
    let effect = metadata.get("effect").map(deserialize).transpose()?;
    Ok(ResultSet::new(effect, columns, Some(Box::new(rows))))
}

/// Converts a protobuf row into a proper row
fn row_from_protobuf(row: service::Row) -> Row {
    row.field.into_iter().map(value_from_protobuf).collect()
}

/// Converts a protobuf field into a proper value
fn value_from_protobuf(field: service::Field) -> Value {
    match field.value {
        None => Value::Null,
        Some(service::Field_oneof_value::boolean(b)) => Value::Boolean(b),
        Some(service::Field_oneof_value::float(f)) => Value::Float(f),
        Some(service::Field_oneof_value::integer(f)) => Value::Integer(f),
        Some(service::Field_oneof_value::string(s)) => Value::String(s),
    }
}
