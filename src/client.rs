use crate::server::Status;

use crate::server::{Request, Response};
use crate::service;
use crate::sql::engine::Mode;
use crate::sql::execution::ResultSet;
use crate::sql::schema::Table;
use crate::sql::types::Row;
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
        let (_, mut stream) = self
            .client
            .execute(
                grpc::RequestOptions::new(),
                service::Query {
                    txn_id: match self.txn {
                        Some((id, _)) => id,
                        None => 0,
                    },
                    query: query.to_owned(),
                    ..Default::default()
                },
            )
            .wait()?;

        let resp =
            stream.next().ok_or_else(|| Error::Internal("Unexpected end of stream".into()))??;
        if !resp.err.is_empty() {
            return Err(deserialize(&resp.err)?);
        }
        let mut result = deserialize(&resp.ok)?;
        match &mut result {
            ResultSet::Query { relation } => {
                relation.rows = Some(Box::new(stream.map(Self::stream_to_row)))
            }
            ResultSet::Begin { id, mode } => self.txn = Some((*id, mode.clone())),
            ResultSet::Commit { .. } => self.txn = None,
            ResultSet::Rollback { .. } => self.txn = None,
            _ => {}
        };
        Ok(result)
    }

    /// Transform the gRPC result stream into a row iterator
    fn stream_to_row(item: Result<service::Result, grpc::Error>) -> Result<Row, Error> {
        let item = item?;
        if !item.err.is_empty() {
            return Err(deserialize(&item.err)?);
        }
        let row = deserialize(&item.ok)?;
        Ok(row)
    }

    /// Checks server status
    pub fn status(&self) -> Result<Status, Error> {
        match self.call(Request::Status)? {
            Response::Status(s) => Ok(s),
            resp => Err(Error::Value(format!("Unexpected response: {:?}", resp))),
        }
    }
}
