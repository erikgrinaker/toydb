use crate::service;
use crate::sql::types::{Row, Value};
use crate::Error;
use grpc::ClientStubExt;
use service::ToyDB;

/// A ToyDB client
pub struct Client {
    client: service::ToyDBClient,
}

impl Client {
    /// Creates a new client
    pub fn new(addr: std::net::SocketAddr) -> Result<Self, Error> {
        Ok(Self {
            client: service::ToyDBClient::new_plain(
                &addr.ip().to_string(),
                addr.port(),
                grpc::ClientConf::new(),
            )?,
        })
    }

    /// Runs an echo request
    pub fn echo(&self, value: &str) -> Result<String, Error> {
        let (_, resp, _) = self
            .client
            .echo(
                grpc::RequestOptions::new(),
                service::EchoRequest { value: value.to_owned(), ..Default::default() },
            )
            .wait()?;
        Ok(resp.value)
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
}

/// A query result set
pub struct ResultSet {
    columns: Vec<String>,
    rows: Box<dyn Iterator<Item = Result<service::Row, grpc::Error>>>,
}

impl Iterator for ResultSet {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Result<Row, Error>> {
        Some(self.rows.next()?.map(Self::row_from_protobuf).map_err(|e| e.into()))
    }
}

impl ResultSet {
    fn from_grpc(
        metadata: grpc::Metadata,
        rows: Box<dyn std::iter::Iterator<Item = Result<service::Row, grpc::Error>>>,
    ) -> Result<Self, Error> {
        Ok(Self { columns: Self::deserialize(metadata.get("columns").unwrap().to_vec())?, rows })
    }

    pub fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    /// Deserializes a value from a byte buffer
    fn deserialize<'de, V: serde::Deserialize<'de>>(bytes: Vec<u8>) -> Result<V, Error> {
        Ok(serde::Deserialize::deserialize(&mut rmps::Deserializer::new(&bytes[..]))?)
    }

    /// Converts a protobuf row into a proper row
    fn row_from_protobuf(row: service::Row) -> Row {
        row.field.into_iter().map(Self::value_from_protobuf).collect()
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
}
