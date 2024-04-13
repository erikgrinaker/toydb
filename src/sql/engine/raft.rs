use super::super::schema::{Catalog, Table, Tables};
use super::super::types::{Expression, Row, Value};
use super::{Engine as _, IndexScan, Scan, Transaction as _};
use crate::encoding::bincode;
use crate::error::{Error, Result};
use crate::raft::{self, Entry};
use crate::storage::{self, mvcc::TransactionState};

use crossbeam::channel::Sender;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashSet;

/// A Raft state machine mutation.
///
/// TODO: use Cows for these.
#[derive(Clone, Serialize, Deserialize)]
enum Mutation {
    /// Begins a read-write transaction
    Begin,
    /// Commits the given transaction
    Commit(TransactionState),
    /// Rolls back the given transaction
    Rollback(TransactionState),

    /// Creates a new row
    Create { txn: TransactionState, table: String, row: Row },
    /// Deletes a row
    Delete { txn: TransactionState, table: String, id: Value },
    /// Updates a row
    Update { txn: TransactionState, table: String, id: Value, row: Row },

    /// Creates a table
    CreateTable { txn: TransactionState, schema: Table },
    /// Deletes a table
    DeleteTable { txn: TransactionState, table: String },
}

/// A Raft state machine query.
///
/// TODO: use Cows for these.
#[derive(Clone, Serialize, Deserialize)]
enum Query {
    /// Begins a read-only transaction
    BeginReadOnly { as_of: Option<u64> },
    /// Fetches engine status
    Status,

    /// Reads a row
    Read { txn: TransactionState, table: String, id: Value },
    /// Reads an index entry
    ReadIndex { txn: TransactionState, table: String, column: String, value: Value },
    /// Scans a table's rows
    Scan { txn: TransactionState, table: String, filter: Option<Expression> },
    /// Scans an index
    ScanIndex { txn: TransactionState, table: String, column: String },

    /// Scans the tables
    ScanTables { txn: TransactionState },
    /// Reads a table
    ReadTable { txn: TransactionState, table: String },
}

/// Status for the Raft SQL engine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub raft: raft::Status,
    pub mvcc: storage::mvcc::Status,
}

/// A client for the local Raft node.
#[derive(Clone)]
struct Client {
    tx: Sender<(raft::Request, Sender<Result<raft::Response>>)>,
}

impl Client {
    /// Creates a new Raft client.
    fn new(tx: Sender<(raft::Request, Sender<Result<raft::Response>>)>) -> Self {
        Self { tx }
    }

    /// Executes a request against the Raft cluster.
    fn execute(&self, request: raft::Request) -> Result<raft::Response> {
        let (response_tx, response_rx) = crossbeam::channel::bounded(1);
        self.tx.send((request, response_tx))?;
        response_rx.recv()?
    }

    /// Mutates the Raft state machine, deserializing the response into the
    /// return type.
    fn mutate<V: DeserializeOwned>(&self, mutation: Mutation) -> Result<V> {
        match self.execute(raft::Request::Write(bincode::serialize(&mutation)?))? {
            raft::Response::Write(response) => Ok(bincode::deserialize(&response)?),
            resp => Err(Error::Internal(format!("Unexpected Raft mutation response {:?}", resp))),
        }
    }

    /// Queries the Raft state machine, deserializing the response into the
    /// return type.
    fn query<V: DeserializeOwned>(&self, query: Query) -> Result<V> {
        match self.execute(raft::Request::Read(bincode::serialize(&query)?))? {
            raft::Response::Read(response) => Ok(bincode::deserialize(&response)?),
            resp => Err(Error::Internal(format!("Unexpected Raft query response {:?}", resp))),
        }
    }

    /// Fetches Raft node status.
    fn status(&self) -> Result<raft::Status> {
        match self.execute(raft::Request::Status)? {
            raft::Response::Status(status) => Ok(status),
            resp => Err(Error::Internal(format!("Unexpected Raft status response {:?}", resp))),
        }
    }
}

/// A SQL engine using a Raft state machine.
#[derive(Clone)]
pub struct Raft {
    client: Client,
}

impl Raft {
    /// Creates a new Raft-based SQL engine.
    pub fn new(tx: Sender<(raft::Request, Sender<Result<raft::Response>>)>) -> Self {
        Self { client: Client::new(tx) }
    }

    /// Creates an underlying state machine for a Raft engine.
    pub fn new_state<E: storage::Engine>(engine: E) -> Result<State<E>> {
        State::new(engine)
    }

    /// Returns Raft SQL engine status.
    pub fn status(&self) -> Result<Status> {
        Ok(Status { raft: self.client.status()?, mvcc: self.client.query(Query::Status)? })
    }
}

impl super::Engine for Raft {
    type Transaction = Transaction;

    fn begin(&self) -> Result<Self::Transaction> {
        Transaction::begin(self.client.clone(), false, None)
    }

    fn begin_read_only(&self) -> Result<Self::Transaction> {
        Transaction::begin(self.client.clone(), true, None)
    }

    fn begin_as_of(&self, version: u64) -> Result<Self::Transaction> {
        Transaction::begin(self.client.clone(), true, Some(version))
    }
}

/// A Raft-based SQL transaction.
#[derive(Clone)]
pub struct Transaction {
    client: Client,
    state: TransactionState,
}

impl Transaction {
    /// Starts a transaction in the given mode.
    fn begin(client: Client, read_only: bool, as_of: Option<u64>) -> Result<Self> {
        let state = if read_only || as_of.is_some() {
            client.query(Query::BeginReadOnly { as_of })?
        } else {
            client.mutate(Mutation::Begin)?
        };
        Ok(Self { client, state })
    }
}

impl super::Transaction for Transaction {
    fn version(&self) -> u64 {
        self.state.version
    }

    fn read_only(&self) -> bool {
        self.state.read_only
    }

    fn commit(self) -> Result<()> {
        if !self.read_only() {
            self.client.mutate(Mutation::Commit(self.state.clone()))?
        }
        Ok(())
    }

    fn rollback(self) -> Result<()> {
        if !self.read_only() {
            self.client.mutate(Mutation::Rollback(self.state.clone()))?;
        }
        Ok(())
    }

    fn create(&mut self, table: &str, row: Row) -> Result<()> {
        self.client.mutate(Mutation::Create {
            txn: self.state.clone(),
            table: table.to_string(),
            row,
        })
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<()> {
        self.client.mutate(Mutation::Delete {
            txn: self.state.clone(),
            table: table.to_string(),
            id: id.clone(),
        })
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        self.client.query(Query::Read {
            txn: self.state.clone(),
            table: table.to_string(),
            id: id.clone(),
        })
    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        self.client.query(Query::ReadIndex {
            txn: self.state.clone(),
            table: table.to_string(),
            column: column.to_string(),
            value: value.clone(),
        })
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Scan> {
        Ok(Box::new(
            self.client
                .query::<Vec<_>>(Query::Scan {
                    txn: self.state.clone(),
                    table: table.to_string(),
                    filter,
                })?
                .into_iter()
                .map(Ok),
        ))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan> {
        Ok(Box::new(
            self.client
                .query::<Vec<_>>(Query::ScanIndex {
                    txn: self.state.clone(),
                    table: table.to_string(),
                    column: column.to_string(),
                })?
                .into_iter()
                .map(Ok),
        ))
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()> {
        self.client.mutate(Mutation::Update {
            txn: self.state.clone(),
            table: table.to_string(),
            id: id.clone(),
            row,
        })
    }
}

impl Catalog for Transaction {
    fn create_table(&mut self, table: Table) -> Result<()> {
        self.client.mutate(Mutation::CreateTable { txn: self.state.clone(), schema: table })
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        self.client
            .mutate(Mutation::DeleteTable { txn: self.state.clone(), table: table.to_string() })
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        self.client.query(Query::ReadTable { txn: self.state.clone(), table: table.to_string() })
    }

    fn scan_tables(&self) -> Result<Tables> {
        Ok(Box::new(
            self.client.query::<Vec<_>>(Query::ScanTables { txn: self.state.clone() })?.into_iter(),
        ))
    }
}

/// The Raft state machine for the Raft-based SQL engine, using a KV SQL engine
pub struct State<E: storage::Engine> {
    /// The underlying KV SQL engine
    engine: super::KV<E>,
    /// The last applied index
    applied_index: u64,
}

impl<E: storage::Engine> State<E> {
    /// Creates a new Raft state maching using the given storage engine.
    pub fn new(engine: E) -> Result<Self> {
        let engine = super::KV::new(engine);
        let applied_index = engine
            .get_metadata(b"applied_index")?
            .map(|b| bincode::deserialize(&b))
            .unwrap_or(Ok(0))?;
        Ok(State { engine, applied_index })
    }

    /// Mutates the state machine.
    fn mutate(&mut self, mutation: Mutation) -> Result<Vec<u8>> {
        match mutation {
            Mutation::Begin => bincode::serialize(&self.engine.begin()?.state()),
            Mutation::Commit(txn) => bincode::serialize(&self.engine.resume(txn)?.commit()?),
            Mutation::Rollback(txn) => bincode::serialize(&self.engine.resume(txn)?.rollback()?),

            Mutation::Create { txn, table, row } => {
                bincode::serialize(&self.engine.resume(txn)?.create(&table, row)?)
            }
            Mutation::Delete { txn, table, id } => {
                bincode::serialize(&self.engine.resume(txn)?.delete(&table, &id)?)
            }
            Mutation::Update { txn, table, id, row } => {
                bincode::serialize(&self.engine.resume(txn)?.update(&table, &id, row)?)
            }

            Mutation::CreateTable { txn, schema } => {
                bincode::serialize(&self.engine.resume(txn)?.create_table(schema)?)
            }
            Mutation::DeleteTable { txn, table } => {
                bincode::serialize(&self.engine.resume(txn)?.delete_table(&table)?)
            }
        }
    }
}

impl<E: storage::Engine> raft::State for State<E> {
    fn get_applied_index(&self) -> u64 {
        self.applied_index
    }

    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        assert_eq!(entry.index, self.applied_index + 1, "entry index not after applied index");

        let result = match &entry.command {
            Some(command) => match self.mutate(bincode::deserialize(command)?) {
                error @ Err(Error::Internal(_)) => return error, // don't record as applied
                result => result,
            },
            None => Ok(Vec::new()),
        };
        self.applied_index = entry.index;
        self.engine.set_metadata(b"applied_index", bincode::serialize(&entry.index)?)?;
        result
    }

    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match bincode::deserialize(&command)? {
            Query::BeginReadOnly { as_of } => {
                let txn = if let Some(version) = as_of {
                    self.engine.begin_as_of(version)?
                } else {
                    self.engine.begin_read_only()?
                };
                bincode::serialize(&txn.state())
            }
            Query::Read { txn, table, id } => {
                bincode::serialize(&self.engine.resume(txn)?.read(&table, &id)?)
            }
            Query::ReadIndex { txn, table, column, value } => {
                bincode::serialize(&self.engine.resume(txn)?.read_index(&table, &column, &value)?)
            }
            // FIXME These need to stream rows somehow
            Query::Scan { txn, table, filter } => bincode::serialize(
                &self.engine.resume(txn)?.scan(&table, filter)?.collect::<Result<Vec<_>>>()?,
            ),
            Query::ScanIndex { txn, table, column } => bincode::serialize(
                &self
                    .engine
                    .resume(txn)?
                    .scan_index(&table, &column)?
                    .collect::<Result<Vec<_>>>()?,
            ),
            Query::Status => bincode::serialize(&self.engine.kv.status()?),

            Query::ReadTable { txn, table } => {
                bincode::serialize(&self.engine.resume(txn)?.read_table(&table)?)
            }
            Query::ScanTables { txn } => {
                bincode::serialize(&self.engine.resume(txn)?.scan_tables()?.collect::<Vec<_>>())
            }
        }
    }
}
