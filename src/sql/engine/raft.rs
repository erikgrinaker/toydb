use super::super::types::schema::Table;
use super::super::types::{Expression, Row, Rows, Value};
use super::{Catalog, Engine as _, IndexScan, Transaction as _};
use crate::encoding::{self, bincode, Value as _};
use crate::errdata;
use crate::error::Result;
use crate::raft::{self, Entry};
use crate::storage::{self, mvcc, mvcc::TransactionState};

use crossbeam::channel::Sender;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
    Create { txn: TransactionState, table: String, rows: Vec<Row> },
    /// Deletes a row
    Delete { txn: TransactionState, table: String, ids: Vec<Value> },
    /// Updates a row
    Update { txn: TransactionState, table: String, rows: HashMap<Value, Row> },

    /// Creates a table
    CreateTable { txn: TransactionState, schema: Table },
    /// Deletes a table
    DeleteTable { txn: TransactionState, table: String },
}

impl encoding::Value for Mutation {}

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
    Read { txn: TransactionState, table: String, ids: Vec<Value> },
    /// Reads an index entry
    ReadIndex { txn: TransactionState, table: String, column: String, values: Vec<Value> },
    /// Scans a table's rows
    Scan { txn: TransactionState, table: String, filter: Option<Expression> },
    /// Scans an index
    ScanIndex { txn: TransactionState, table: String, column: String },

    /// Scans the tables
    ScanTables { txn: TransactionState },
    /// Reads a table
    ReadTable { txn: TransactionState, table: String },
}

impl encoding::Value for Query {}

/// Status for the Raft SQL engine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub raft: raft::Status,
    pub mvcc: mvcc::Status,
}

/// A client for the local Raft node.
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
        match self.execute(raft::Request::Write(mutation.encode()))? {
            raft::Response::Write(response) => Ok(bincode::deserialize(&response)?),
            resp => errdata!("unexpected Raft mutation response {resp:?}"),
        }
    }

    /// Queries the Raft state machine, deserializing the response into the
    /// return type.
    fn query<V: DeserializeOwned>(&self, query: Query) -> Result<V> {
        match self.execute(raft::Request::Read(query.encode()))? {
            raft::Response::Read(response) => Ok(bincode::deserialize(&response)?),
            resp => errdata!("unexpected Raft query response {resp:?}"),
        }
    }

    /// Fetches Raft node status.
    fn status(&self) -> Result<raft::Status> {
        match self.execute(raft::Request::Status)? {
            raft::Response::Status(status) => Ok(status),
            resp => errdata!("unexpected Raft status response {resp:?}"),
        }
    }
}

/// A SQL engine using a Raft state machine.
pub struct Raft {
    tx: Sender<(raft::Request, Sender<Result<raft::Response>>)>,
}

impl Raft {
    /// Creates a new Raft-based SQL engine.
    pub fn new(tx: Sender<(raft::Request, Sender<Result<raft::Response>>)>) -> Self {
        Self { tx }
    }

    /// Creates an underlying state machine for a Raft engine.
    pub fn new_state<E: storage::Engine>(engine: E) -> Result<State<E>> {
        State::new(engine)
    }

    /// Returns Raft SQL engine status.
    pub fn status(&self) -> Result<Status> {
        let client = Client::new(self.tx.clone());
        Ok(Status { raft: client.status()?, mvcc: client.query(Query::Status)? })
    }
}

impl<'a> super::Engine<'a> for Raft {
    type Transaction = Transaction;

    fn begin(&self) -> Result<Self::Transaction> {
        Transaction::begin(Client::new(self.tx.clone()), false, None)
    }

    fn begin_read_only(&self) -> Result<Self::Transaction> {
        Transaction::begin(Client::new(self.tx.clone()), true, None)
    }

    fn begin_as_of(&self, version: u64) -> Result<Self::Transaction> {
        Transaction::begin(Client::new(self.tx.clone()), true, Some(version))
    }
}

/// A Raft-based SQL transaction.
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
    fn version(&self) -> mvcc::Version {
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

    fn insert(&self, table: &str, rows: Vec<Row>) -> Result<()> {
        self.client.mutate(Mutation::Create {
            txn: self.state.clone(),
            table: table.to_string(),
            rows,
        })
    }

    fn delete(&self, table: &str, ids: &[Value]) -> Result<()> {
        self.client.mutate(Mutation::Delete {
            txn: self.state.clone(),
            table: table.to_string(),
            ids: ids.to_vec(),
        })
    }

    fn get(&self, table: &str, ids: &[Value]) -> Result<Vec<Row>> {
        self.client.query(Query::Read {
            txn: self.state.clone(),
            table: table.to_string(),
            ids: ids.to_vec(),
        })
    }

    fn lookup_index(&self, table: &str, column: &str, values: &[Value]) -> Result<HashSet<Value>> {
        self.client.query(Query::ReadIndex {
            txn: self.state.clone(),
            table: table.to_string(),
            column: column.to_string(),
            values: values.to_vec(),
        })
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Rows> {
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

    fn update(&self, table: &str, rows: HashMap<Value, Row>) -> Result<()> {
        self.client.mutate(Mutation::Update {
            txn: self.state.clone(),
            table: table.to_string(),
            rows,
        })
    }
}

impl Catalog for Transaction {
    fn create_table(&self, table: Table) -> Result<()> {
        self.client.mutate(Mutation::CreateTable { txn: self.state.clone(), schema: table })
    }

    fn drop_table(&self, table: &str) -> Result<()> {
        self.client
            .mutate(Mutation::DeleteTable { txn: self.state.clone(), table: table.to_string() })
    }

    fn get_table(&self, table: &str) -> Result<Option<Table>> {
        self.client.query(Query::ReadTable { txn: self.state.clone(), table: table.to_string() })
    }

    fn list_tables(&self) -> Result<Vec<Table>> {
        self.client.query(Query::ScanTables { txn: self.state.clone() })
    }
}

/// The Raft state machine for the Raft-based SQL engine, using a KV SQL engine
pub struct State<E: storage::Engine> {
    /// The underlying local SQL engine.
    engine: super::Local<E>,
    /// The last applied index
    applied_index: u64,
}

impl<E: storage::Engine> State<E> {
    /// Creates a new Raft state maching using the given storage engine.
    pub fn new(engine: E) -> Result<Self> {
        let engine = super::Local::new(engine);
        let applied_index = engine
            .get_metadata(b"applied_index")?
            .map(|b| bincode::deserialize(&b))
            .unwrap_or(Ok(0))?;
        Ok(State { engine, applied_index })
    }

    /// Mutates the state machine.
    fn mutate(&mut self, mutation: Mutation) -> Result<Vec<u8>> {
        let response = match mutation {
            Mutation::Begin => self.engine.begin()?.state().encode(),
            Mutation::Commit(txn) => bincode::serialize(&self.engine.resume(txn)?.commit()?),
            Mutation::Rollback(txn) => bincode::serialize(&self.engine.resume(txn)?.rollback()?),

            Mutation::Create { txn, table, rows } => {
                bincode::serialize(&self.engine.resume(txn)?.insert(&table, rows)?)
            }
            Mutation::Delete { txn, table, ids } => {
                bincode::serialize(&self.engine.resume(txn)?.delete(&table, &ids)?)
            }
            Mutation::Update { txn, table, rows } => {
                bincode::serialize(&self.engine.resume(txn)?.update(&table, rows)?)
            }

            Mutation::CreateTable { txn, schema } => {
                bincode::serialize(&self.engine.resume(txn)?.create_table(schema)?)
            }
            Mutation::DeleteTable { txn, table } => {
                bincode::serialize(&self.engine.resume(txn)?.drop_table(&table)?)
            }
        };
        Ok(response)
    }
}

impl<E: storage::Engine> raft::State for State<E> {
    fn get_applied_index(&self) -> u64 {
        self.applied_index
    }

    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        assert_eq!(entry.index, self.applied_index + 1, "entry index not after applied index");

        let result = match &entry.command {
            Some(command) => match self.mutate(Mutation::decode(command)?) {
                // We must panic on non-deterministic apply failures to prevent
                // replica divergence. See [`raft::State`] documentation for
                // details.
                Err(e) if !e.is_deterministic() => panic!("non-deterministic apply failure: {e}"),
                result => result,
            },
            None => Ok(Vec::new()),
        };
        self.applied_index = entry.index;
        self.engine.set_metadata(b"applied_index", bincode::serialize(&entry.index))?;
        result
    }

    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        let response = match Query::decode(&command)? {
            Query::BeginReadOnly { as_of } => {
                let txn = if let Some(version) = as_of {
                    self.engine.begin_as_of(version)?
                } else {
                    self.engine.begin_read_only()?
                };
                txn.state().encode()
            }
            Query::Read { txn, table, ids } => self.engine.resume(txn)?.get(&table, &ids)?.encode(),
            Query::ReadIndex { txn, table, column, values } => {
                self.engine.resume(txn)?.lookup_index(&table, &column, &values)?.encode()
            }
            // FIXME These need to stream rows somehow
            Query::Scan { txn, table, filter } => {
                self.engine.resume(txn)?.scan(&table, filter)?.collect::<Result<Vec<_>>>()?.encode()
            }
            Query::ScanIndex { txn, table, column } => self
                .engine
                .resume(txn)?
                .scan_index(&table, &column)?
                .collect::<Result<Vec<_>>>()?
                .encode(),
            Query::Status => self.engine.kv.status()?.encode(),

            Query::ReadTable { txn, table } => self.engine.resume(txn)?.get_table(&table)?.encode(),
            Query::ScanTables { txn } => self.engine.resume(txn)?.list_tables()?.encode(),
        };
        Ok(response)
    }
}
