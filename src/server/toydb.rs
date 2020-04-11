use crate::raft::Raft;
use crate::service;
use crate::sql::engine::{Engine, Mode, Transaction};
use crate::sql::execution::ResultSet;
use crate::sql::schema::Table;
use crate::sql::types::{Relation, Row};
use crate::utility::{deserialize, serialize};
use crate::Error;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    GetTable(String),
    ListTables,
    Status,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
    GetTable(Table),
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
    pub engine: crate::sql::engine::Raft,
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

    fn execute(
        &self,
        _: grpc::RequestOptions,
        req: service::Query,
    ) -> grpc::StreamingResponse<service::Result> {
        let iter: Box<dyn Iterator<Item = service::Result> + Send> = match self
            .execute(req.txn_id, &req.query)
        {
            Ok(result) => {
                let mut iter: Box<dyn Iterator<Item = service::Result> + Send> = Box::new(
                    vec![service::Result { ok: serialize(&result).unwrap(), ..Default::default() }]
                        .into_iter(),
                );
                if let ResultSet::Query { relation: Relation { rows: Some(rows), .. } } = result {
                    iter = Box::new(iter.chain(rows.map(Self::rows_to_stream)));
                }
                iter
            }
            Err(e) => Box::new(
                vec![service::Result { err: serialize(&e).unwrap(), ..Default::default() }]
                    .into_iter(),
            ),
        };
        grpc::StreamingResponse::iter(iter)
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

    fn execute(&self, txn_id: u64, query: &str) -> Result<ResultSet, Error> {
        error!("txn_id: {}", txn_id);
        let txn_id = if txn_id > 0 { Some(txn_id) } else { None };
        self.engine.session(txn_id)?.execute(query)
    }

    fn rows_to_stream(item: Result<Row, Error>) -> service::Result {
        match item {
            Ok(row) => service::Result { ok: serialize(&row).unwrap(), ..Default::default() },
            Err(err) => service::Result { err: serialize(&err).unwrap(), ..Default::default() },
        }
    }

    fn get_table(&self, name: &str) -> Result<Table, Error> {
        self.engine.with_txn(Mode::ReadOnly, |txn| match txn.read_table(name)? {
            Some(t) => Ok(t),
            None => Err(Error::Value(format!("Table {} does not exist", name))),
        })
    }

    fn list_tables(&self) -> Result<Vec<String>, Error> {
        self.engine.with_txn(Mode::ReadOnly, |txn| Ok(txn.scan_tables()?.map(|t| t.name).collect()))
    }
}
