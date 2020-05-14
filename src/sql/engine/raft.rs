use super::super::schema::{Catalog, Table, Tables};
use super::super::types::{Expression, Row, Value};
use super::{Engine as _, IndexScan, Mode, Scan, Transaction as _};
use crate::raft;
use crate::storage::kv;
use crate::Error;

use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashSet;

/// A Raft state machine mutation
#[derive(Clone, Serialize, Deserialize)]
enum Mutation {
    /// Begins a transaction in the given mode
    Begin(Mode),
    /// Commits the transaction with the given ID
    Commit(u64),
    /// Rolls back the transaction with the given ID
    Rollback(u64),

    /// Creates a new row
    Create { txn_id: u64, table: String, row: Row },
    /// Deletes a row
    Delete { txn_id: u64, table: String, id: Value },
    /// Updates a row
    Update { txn_id: u64, table: String, id: Value, row: Row },

    /// Creates a table
    CreateTable { txn_id: u64, schema: Table },
    /// Deletes a table
    DeleteTable { txn_id: u64, table: String },
}

/// A Raft state machine query
#[derive(Clone, Serialize, Deserialize)]
enum Query {
    /// Fetches engine status
    Status,
    /// Resumes the active transaction with the given ID
    Resume(u64),

    /// Reads a row
    Read { txn_id: u64, table: String, id: Value },
    /// Reads an index entry
    ReadIndex { txn_id: u64, table: String, column: String, value: Value },
    /// Scans a table's rows
    Scan { txn_id: u64, table: String, filter: Option<Expression> },
    /// Scans an index
    ScanIndex { txn_id: u64, table: String, column: String },

    /// Scans the tables
    ScanTables { txn_id: u64 },
    /// Reads a table
    ReadTable { txn_id: u64, table: String },
}

/// Status for the Raft SQL engine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub raft: raft::Status,
    pub mvcc: kv::mvcc::Status,
}

/// An SQL engine that wraps a Raft cluster.
#[derive(Clone)]
pub struct Raft {
    client: raft::Client,
}

impl Raft {
    /// Creates a new Raft SQL engine.
    pub fn new(client: raft::Client) -> Self {
        Self { client }
    }

    /// Creates an underlying state machine for a Raft engine.
    pub fn new_state<S: kv::Store>(kv: kv::MVCC<S>) -> Result<State<S>, Error> {
        State::new(kv)
    }

    /// Returns Raft SQL engine status.
    pub fn status(&self) -> Result<Status, Error> {
        Ok(Status {
            raft: futures::executor::block_on(self.client.status())?,
            mvcc: Raft::deserialize(&futures::executor::block_on(
                self.client.query(Raft::serialize(&Query::Status)?),
            )?)?,
        })
    }

    /// Serializes a command for the Raft SQL state machine.
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>, Error> {
        Ok(bincode::serialize(value)?)
    }

    /// Deserializes a command for the Raft SQL state machine.
    fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V, Error> {
        Ok(bincode::deserialize(bytes)?)
    }
}

impl super::Engine for Raft {
    type Transaction = Transaction;

    fn begin(&self, mode: Mode) -> Result<Self::Transaction, Error> {
        Transaction::begin(self.client.clone(), mode)
    }

    fn resume(&self, id: u64) -> Result<Self::Transaction, Error> {
        Transaction::resume(self.client.clone(), id)
    }
}

/// A Raft-based SQL transaction
#[derive(Clone)]
pub struct Transaction {
    /// The underlying Raft cluster
    client: raft::Client,
    /// The transaction ID
    id: u64,
    /// The transaction mode
    mode: Mode,
}

impl Transaction {
    /// Starts a transaction in the given mode
    fn begin(client: raft::Client, mode: Mode) -> Result<Self, Error> {
        let id = Raft::deserialize(&futures::executor::block_on(
            client.mutate(Raft::serialize(&Mutation::Begin(mode))?),
        )?)?;
        Ok(Self { client, id, mode })
    }

    /// Resumes an active transaction
    fn resume(client: raft::Client, id: u64) -> Result<Self, Error> {
        let (id, mode) = Raft::deserialize(&futures::executor::block_on(
            client.query(Raft::serialize(&Query::Resume(id))?),
        )?)?;
        Ok(Self { client, id, mode })
    }

    /// Executes a mutation
    fn mutate(&self, mutation: Mutation) -> Result<Vec<u8>, Error> {
        futures::executor::block_on(self.client.mutate(Raft::serialize(&mutation)?))
    }

    /// Executes a query
    fn query(&self, query: Query) -> Result<Vec<u8>, Error> {
        futures::executor::block_on(self.client.query(Raft::serialize(&query)?))
    }
}

impl super::Transaction for Transaction {
    fn id(&self) -> u64 {
        self.id
    }

    fn mode(&self) -> Mode {
        self.mode
    }

    fn commit(self) -> Result<(), Error> {
        Raft::deserialize(&self.mutate(Mutation::Commit(self.id))?)
    }

    fn rollback(self) -> Result<(), Error> {
        Raft::deserialize(&self.mutate(Mutation::Rollback(self.id))?)
    }

    fn create(&mut self, table: &str, row: Row) -> Result<(), Error> {
        Raft::deserialize(&self.mutate(Mutation::Create {
            txn_id: self.id,
            table: table.to_string(),
            row,
        })?)
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<(), Error> {
        Raft::deserialize(&self.mutate(Mutation::Delete {
            txn_id: self.id,
            table: table.to_string(),
            id: id.clone(),
        })?)
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>, Error> {
        Raft::deserialize(&self.query(Query::Read {
            txn_id: self.id,
            table: table.to_string(),
            id: id.clone(),
        })?)
    }

    fn read_index(
        &self,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<HashSet<Value>, Error> {
        Raft::deserialize(&self.query(Query::ReadIndex {
            txn_id: self.id,
            table: table.to_string(),
            column: column.to_string(),
            value: value.clone(),
        })?)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Scan, Error> {
        Ok(Box::new(
            Raft::deserialize::<Vec<_>>(&self.query(Query::Scan {
                txn_id: self.id,
                table: table.to_string(),
                filter,
            })?)?
            .into_iter()
            .map(Ok),
        ))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan, Error> {
        Ok(Box::new(
            Raft::deserialize::<Vec<_>>(&self.query(Query::ScanIndex {
                txn_id: self.id,
                table: table.to_string(),
                column: column.to_string(),
            })?)?
            .into_iter()
            .map(Ok),
        ))
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<(), Error> {
        Raft::deserialize(&self.mutate(Mutation::Update {
            txn_id: self.id,
            table: table.to_string(),
            id: id.clone(),
            row,
        })?)
    }
}

impl Catalog for Transaction {
    fn create_table(&mut self, table: &Table) -> Result<(), Error> {
        Raft::deserialize(
            &self.mutate(Mutation::CreateTable { txn_id: self.id, schema: table.clone() })?,
        )
    }

    fn delete_table(&mut self, table: &str) -> Result<(), Error> {
        Raft::deserialize(
            &self.mutate(Mutation::DeleteTable { txn_id: self.id, table: table.to_string() })?,
        )
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>, Error> {
        Raft::deserialize(
            &self.query(Query::ReadTable { txn_id: self.id, table: table.to_string() })?,
        )
    }

    fn scan_tables(&self) -> Result<Tables, Error> {
        Ok(Box::new(
            Raft::deserialize::<Vec<_>>(&self.query(Query::ScanTables { txn_id: self.id })?)?
                .into_iter(),
        ))
    }
}

/// The Raft state machine for the Raft-based SQL engine, using a KV SQL engine
pub struct State<S: kv::Store> {
    /// The underlying KV SQL engine
    engine: super::KV<S>,
    /// The last applied index
    applied_index: u64,
}

impl<S: kv::Store> State<S> {
    /// Creates a new Raft state maching using the given MVCC key/value store
    pub fn new(store: kv::MVCC<S>) -> Result<Self, Error> {
        let engine = super::KV::new(store);
        let applied_index = engine
            .get_metadata(b"applied_index")?
            .map(|b| Raft::deserialize(&b))
            .unwrap_or(Ok(0))?;
        Ok(State { engine, applied_index })
    }

    /// Applies a state machine mutation
    fn apply(&mut self, mutation: Mutation) -> Result<Vec<u8>, Error> {
        match mutation {
            Mutation::Begin(mode) => Raft::serialize(&self.engine.begin(mode)?.id()),
            Mutation::Commit(txn_id) => Raft::serialize(&self.engine.resume(txn_id)?.commit()?),
            Mutation::Rollback(txn_id) => Raft::serialize(&self.engine.resume(txn_id)?.rollback()?),

            Mutation::Create { txn_id, table, row } => {
                Raft::serialize(&self.engine.resume(txn_id)?.create(&table, row)?)
            }
            Mutation::Delete { txn_id, table, id } => {
                Raft::serialize(&self.engine.resume(txn_id)?.delete(&table, &id)?)
            }
            Mutation::Update { txn_id, table, id, row } => {
                Raft::serialize(&self.engine.resume(txn_id)?.update(&table, &id, row)?)
            }

            Mutation::CreateTable { txn_id, schema } => {
                Raft::serialize(&self.engine.resume(txn_id)?.create_table(&schema)?)
            }
            Mutation::DeleteTable { txn_id, table } => {
                Raft::serialize(&self.engine.resume(txn_id)?.delete_table(&table)?)
            }
        }
    }
}

impl<S: kv::Store> raft::State for State<S> {
    fn applied_index(&self) -> u64 {
        self.applied_index
    }

    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        // We don't check that index == applied_index + 1, since the Raft log commits no-op
        // entries during leader election which we need to ignore.
        match self.apply(Raft::deserialize(&command)?) {
            error @ Err(Error::Internal(_)) => error,
            result => {
                self.engine.set_metadata(b"applied_index", Raft::serialize(&(index))?)?;
                self.applied_index = index;
                result
            }
        }
    }

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match Raft::deserialize(&command)? {
            Query::Resume(id) => {
                let txn = self.engine.resume(id)?;
                Raft::serialize(&(txn.id(), txn.mode()))
            }

            Query::Read { txn_id, table, id } => {
                Raft::serialize(&self.engine.resume(txn_id)?.read(&table, &id)?)
            }
            Query::ReadIndex { txn_id, table, column, value } => {
                Raft::serialize(&self.engine.resume(txn_id)?.read_index(&table, &column, &value)?)
            }
            // FIXME These need to stream rows somehow
            Query::Scan { txn_id, table, filter } => Raft::serialize(
                &self
                    .engine
                    .resume(txn_id)?
                    .scan(&table, filter)?
                    .collect::<Result<Vec<_>, Error>>()?,
            ),
            Query::ScanIndex { txn_id, table, column } => Raft::serialize(
                &self
                    .engine
                    .resume(txn_id)?
                    .scan_index(&table, &column)?
                    .collect::<Result<Vec<_>, Error>>()?,
            ),
            Query::Status => Raft::serialize(&self.engine.kv.status()?),

            Query::ReadTable { txn_id, table } => {
                Raft::serialize(&self.engine.resume(txn_id)?.read_table(&table)?)
            }
            Query::ScanTables { txn_id } => {
                Raft::serialize(&self.engine.resume(txn_id)?.scan_tables()?.collect::<Vec<_>>())
            }
        }
    }
}
