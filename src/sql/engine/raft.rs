use super::super::schema::{Catalog, Table, Tables};
use super::super::types::{Expression, Row, Value};
use super::{Engine as _, IndexScan, Scan, Transaction as _};
use crate::error::{Error, Result};
use crate::raft;
use crate::storage::{self, mvcc::TransactionState};

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A Raft state machine mutation
///
/// TODO: use Cows for these.
#[derive(Clone, Serialize, Deserialize)]
enum Mutation {
    /// Begins a transaction
    Begin { read_only: bool, as_of: Option<u64> },
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

/// A Raft state machine query
///
/// TODO: use Cows for these.
#[derive(Clone, Serialize, Deserialize)]
enum Query {
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
    pub fn new_state<E: storage::engine::Engine>(engine: E) -> Result<State<E>> {
        State::new(engine)
    }

    /// Returns Raft SQL engine status.
    pub fn status(&self) -> Result<Status> {
        Ok(Status {
            raft: futures::executor::block_on(self.client.status())?,
            mvcc: Raft::deserialize(&futures::executor::block_on(
                self.client.query(Raft::serialize(&Query::Status)?),
            )?)?,
        })
    }

    /// Serializes a command for the Raft SQL state machine.
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    /// Deserializes a command for the Raft SQL state machine.
    fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
        Ok(bincode::deserialize(bytes)?)
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

/// A Raft-based SQL transaction
#[derive(Clone)]
pub struct Transaction {
    /// The underlying Raft cluster
    client: raft::Client,
    // Transaction state
    state: TransactionState,
}

impl Transaction {
    /// Starts a transaction in the given mode
    fn begin(client: raft::Client, read_only: bool, as_of: Option<u64>) -> Result<Self> {
        let state = Raft::deserialize(&futures::executor::block_on(
            client.mutate(Raft::serialize(&Mutation::Begin { read_only, as_of })?),
        )?)?;
        Ok(Self { client, state })
    }

    /// Executes a mutation
    fn mutate(&self, mutation: Mutation) -> Result<Vec<u8>> {
        futures::executor::block_on(self.client.mutate(Raft::serialize(&mutation)?))
    }

    /// Executes a query
    fn query(&self, query: Query) -> Result<Vec<u8>> {
        futures::executor::block_on(self.client.query(Raft::serialize(&query)?))
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
        Raft::deserialize(&self.mutate(Mutation::Commit(self.state.clone()))?)
    }

    fn rollback(self) -> Result<()> {
        Raft::deserialize(&self.mutate(Mutation::Rollback(self.state.clone()))?)
    }

    fn create(&mut self, table: &str, row: Row) -> Result<()> {
        Raft::deserialize(&self.mutate(Mutation::Create {
            txn: self.state.clone(),
            table: table.to_string(),
            row,
        })?)
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<()> {
        Raft::deserialize(&self.mutate(Mutation::Delete {
            txn: self.state.clone(),
            table: table.to_string(),
            id: id.clone(),
        })?)
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        Raft::deserialize(&self.query(Query::Read {
            txn: self.state.clone(),
            table: table.to_string(),
            id: id.clone(),
        })?)
    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        Raft::deserialize(&self.query(Query::ReadIndex {
            txn: self.state.clone(),
            table: table.to_string(),
            column: column.to_string(),
            value: value.clone(),
        })?)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Scan> {
        Ok(Box::new(
            Raft::deserialize::<Vec<_>>(&self.query(Query::Scan {
                txn: self.state.clone(),
                table: table.to_string(),
                filter,
            })?)?
            .into_iter()
            .map(Ok),
        ))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan> {
        Ok(Box::new(
            Raft::deserialize::<Vec<_>>(&self.query(Query::ScanIndex {
                txn: self.state.clone(),
                table: table.to_string(),
                column: column.to_string(),
            })?)?
            .into_iter()
            .map(Ok),
        ))
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()> {
        Raft::deserialize(&self.mutate(Mutation::Update {
            txn: self.state.clone(),
            table: table.to_string(),
            id: id.clone(),
            row,
        })?)
    }
}

impl Catalog for Transaction {
    fn create_table(&mut self, table: Table) -> Result<()> {
        Raft::deserialize(
            &self.mutate(Mutation::CreateTable { txn: self.state.clone(), schema: table })?,
        )
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        Raft::deserialize(
            &self.mutate(Mutation::DeleteTable {
                txn: self.state.clone(),
                table: table.to_string(),
            })?,
        )
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        Raft::deserialize(
            &self.query(Query::ReadTable { txn: self.state.clone(), table: table.to_string() })?,
        )
    }

    fn scan_tables(&self) -> Result<Tables> {
        Ok(Box::new(
            Raft::deserialize::<Vec<_>>(
                &self.query(Query::ScanTables { txn: self.state.clone() })?,
            )?
            .into_iter(),
        ))
    }
}

/// The Raft state machine for the Raft-based SQL engine, using a KV SQL engine
pub struct State<E: storage::engine::Engine> {
    /// The underlying KV SQL engine
    engine: super::KV<E>,
    /// The last applied index
    applied_index: u64,
}

impl<E: storage::engine::Engine> State<E> {
    /// Creates a new Raft state maching using the given storage engine.
    pub fn new(engine: E) -> Result<Self> {
        let engine = super::KV::new(engine);
        let applied_index = engine
            .get_metadata(b"applied_index")?
            .map(|b| Raft::deserialize(&b))
            .unwrap_or(Ok(0))?;
        Ok(State { engine, applied_index })
    }

    /// Applies a state machine mutation
    fn apply(&mut self, mutation: Mutation) -> Result<Vec<u8>> {
        match mutation {
            Mutation::Begin { read_only, as_of } => {
                let txn = if !read_only {
                    self.engine.begin()?
                } else if let Some(version) = as_of {
                    self.engine.begin_as_of(version)?
                } else {
                    self.engine.begin_read_only()?
                };
                Raft::serialize(&txn.state())
            }
            Mutation::Commit(txn) => Raft::serialize(&self.engine.resume(txn)?.commit()?),
            Mutation::Rollback(txn) => Raft::serialize(&self.engine.resume(txn)?.rollback()?),

            Mutation::Create { txn, table, row } => {
                Raft::serialize(&self.engine.resume(txn)?.create(&table, row)?)
            }
            Mutation::Delete { txn, table, id } => {
                Raft::serialize(&self.engine.resume(txn)?.delete(&table, &id)?)
            }
            Mutation::Update { txn, table, id, row } => {
                Raft::serialize(&self.engine.resume(txn)?.update(&table, &id, row)?)
            }

            Mutation::CreateTable { txn, schema } => {
                Raft::serialize(&self.engine.resume(txn)?.create_table(schema)?)
            }
            Mutation::DeleteTable { txn, table } => {
                Raft::serialize(&self.engine.resume(txn)?.delete_table(&table)?)
            }
        }
    }
}

impl<E: storage::engine::Engine> raft::State for State<E> {
    fn applied_index(&self) -> u64 {
        self.applied_index
    }

    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>> {
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

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match Raft::deserialize(&command)? {
            Query::Read { txn, table, id } => {
                Raft::serialize(&self.engine.resume(txn)?.read(&table, &id)?)
            }
            Query::ReadIndex { txn, table, column, value } => {
                Raft::serialize(&self.engine.resume(txn)?.read_index(&table, &column, &value)?)
            }
            // FIXME These need to stream rows somehow
            Query::Scan { txn, table, filter } => Raft::serialize(
                &self.engine.resume(txn)?.scan(&table, filter)?.collect::<Result<Vec<_>>>()?,
            ),
            Query::ScanIndex { txn, table, column } => Raft::serialize(
                &self
                    .engine
                    .resume(txn)?
                    .scan_index(&table, &column)?
                    .collect::<Result<Vec<_>>>()?,
            ),
            Query::Status => Raft::serialize(&self.engine.kv.status()?),

            Query::ReadTable { txn, table } => {
                Raft::serialize(&self.engine.resume(txn)?.read_table(&table)?)
            }
            Query::ScanTables { txn } => {
                Raft::serialize(&self.engine.resume(txn)?.scan_tables()?.collect::<Vec<_>>())
            }
        }
    }
}
