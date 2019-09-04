use crate::service;
use crate::sql::types::{Row, Value};
use crate::utility::deserialize;
use crate::Error;
use grpc::ClientStubExt;
use service::ToyDB;

/// A ToyDB client
pub struct Client {
    client: service::ToyDBClient,
}

impl Client {
    /// Creates a new client
    pub fn new(host: &str, port: u16) -> Result<Self, Error> {
        Ok(Self { client: service::ToyDBClient::new_plain(host, port, grpc::ClientConf::new())? })
    }

    /// Fetches the table schema as SQL
    pub fn get_table(&self, table: &str) -> Result<String, Error> {
        let (_, resp, _) = self
            .client
            .get_table(
                grpc::RequestOptions::new(),
                service::GetTableRequest { name: table.to_string(), ..Default::default() },
            )
            .wait()?;
        error_from_protobuf(resp.error)?;
        Ok(resp.sql)
    }

    /// Runs a query
    pub fn query(&self, query: &str) -> Result<ResultSet, Error> {
        let (metadata, iter) = self
            .client
            .query(
                grpc::RequestOptions::new(),
                service::QueryRequest { query: query.to_owned(), ..Default::default() },
            )
            .wait()?;
        ResultSet::from_grpc(metadata, iter)
    }

    /// Checks server status
    pub fn status(&self) -> Result<Status, Error> {
        let (_, resp, _) = self
            .client
            .status(grpc::RequestOptions::new(), service::StatusRequest::new())
            .wait()?;
        Ok(Status { id: resp.id, version: resp.version })
    }
}

/// A query result set
pub struct ResultSet {
    columns: Vec<String>,
    rows: Box<dyn Iterator<Item = Result<service::Row, grpc::Error>>>,
}

impl Iterator for ResultSet {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Result<Row, Error>> {
        match self.rows.next()? {
            Ok(row) => {
                if let Err(err) = error_from_protobuf(row.error.clone()) {
                    return Some(Err(err))
                }
                Some(Ok(row_from_protobuf(row)))
            },
            Err(err) => Some(Err(err.into())),
        }
    }
}

impl ResultSet {
    fn from_grpc(
        metadata: grpc::Metadata,
        rows: Box<dyn std::iter::Iterator<Item = Result<service::Row, grpc::Error>>>,
    ) -> Result<Self, Error> {
        let columns =
            deserialize(metadata.get("columns").map(|c| c.to_vec()).unwrap_or_else(Vec::new))
                .unwrap_or_else(|_| Vec::new());
        Ok(Self { columns, rows })
    }

    pub fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }
}

/// Server status
pub struct Status {
    pub id: String,
    pub version: String,
}

/// Converts a protobuf error into a toyDB error
fn error_from_protobuf(err: protobuf::SingularPtrField<service::Error>) -> Result<(), Error> {
    match err.into_option() {
        Some(err) => Err(Error::Internal(err.message)),
        _ => Ok(()),
    }
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
