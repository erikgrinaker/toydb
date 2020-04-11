use crate::server::{Request, Response, Status};
use crate::service;
use crate::service::ToyDB;
use crate::sql::engine::Mode;
use crate::sql::execution::ResultSet;
use crate::sql::schema::Table;
use crate::sql::types::Row;
use crate::utility::{deserialize, serialize};
use crate::Error;

use grpc::ClientStubExt;
use rand::Rng as _;

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

    /// Runs a transaction as a closure, automatically handling serialization failures by
    /// retrying the closure with exponential backoff. The returned result is from the final commit.
    pub fn with_txn<F>(&mut self, f: F) -> Result<ResultSet, Error>
    where
        F: Fn(&mut Self) -> Result<(), Error>,
    {
        let mut rng = rand::thread_rng();
        for i in 0..5 {
            if i > 0 {
                std::thread::sleep(std::time::Duration::from_millis(
                    2_u64.pow(i as u32 - 1) * rng.gen_range(75, 125),
                ));
            }
            // Ugly workaround to use ?, while waiting for try_blocks:
            // https://doc.rust-lang.org/unstable-book/language-features/try-blocks.html
            let result = (|| {
                self.query("BEGIN")?;
                f(self)?;
                self.query("COMMIT")
            })();
            if self.txn().is_some() {
                self.query("ROLLBACK")?;
            }
            if let Err(Error::Serialization) = result {
                continue;
            }
            return result;
        }
        Err(Error::Serialization)
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
