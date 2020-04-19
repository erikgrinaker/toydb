use super::super::schema::{Catalog, Table, Tables};
use super::super::types::{Expression, Row, Value};
use super::{Engine as _, Transaction as _};
use crate::kv;
use crate::raft;
use crate::utility::{deserialize, serialize};
use crate::Error;

use std::collections::HashSet;

/// A Raft state machine mutation
#[derive(Clone, Serialize, Deserialize)]
enum Mutation {
    /// Begins a transaction in the given mode
    Begin(super::Mode),
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

/// An SQL engine that wraps a Raft cluster.
#[derive(Clone)]
pub struct Raft {
    /// The underlying Raft cluster.
    raft: raft::Raft,
}

impl Raft {
    /// Creates a new Raft SQL engine.
    pub fn new(raft: raft::Raft) -> Self {
        Self { raft }
    }

    /// Creates an underlying state machine for a Raft engine.
    pub fn new_state<S: kv::storage::Storage>(kv: kv::MVCC<S>) -> State<S> {
        State::new(kv)
    }
}

impl super::Engine for Raft {
    type Transaction = Transaction;

    fn begin(&self, mode: super::Mode) -> Result<Self::Transaction, Error> {
        Transaction::begin(self.raft.clone(), mode)
    }

    fn resume(&self, id: u64) -> Result<Self::Transaction, Error> {
        Transaction::resume(self.raft.clone(), id)
    }
}

/// A Raft-based SQL transaction
#[derive(Clone)]
pub struct Transaction {
    /// The underlying Raft cluster
    raft: raft::Raft,
    /// The transaction ID
    id: u64,
    /// The transaction mode
    mode: super::Mode,
}

impl Transaction {
    /// Starts a transaction in the given mode
    fn begin(raft: raft::Raft, mode: super::Mode) -> Result<Self, Error> {
        let id = deserialize(&raft.mutate(serialize(&Mutation::Begin(mode.clone()))?)?)?;
        Ok(Self { raft, id, mode })
    }

    /// Resumes an active transaction
    fn resume(raft: raft::Raft, id: u64) -> Result<Self, Error> {
        let (id, mode) = deserialize(&raft.query(serialize(&Query::Resume(id))?)?)?;
        Ok(Self { raft, id, mode })
    }
}

impl super::Transaction for Transaction {
    fn id(&self) -> u64 {
        self.id
    }

    fn mode(&self) -> super::Mode {
        self.mode.clone()
    }

    fn commit(self) -> Result<(), Error> {
        deserialize(&self.raft.mutate(serialize(&Mutation::Commit(self.id))?)?)
    }

    fn rollback(self) -> Result<(), Error> {
        deserialize(&self.raft.mutate(serialize(&Mutation::Rollback(self.id))?)?)
    }

    fn create(&mut self, table: &str, row: Row) -> Result<(), Error> {
        deserialize(&self.raft.mutate(serialize(&Mutation::Create {
            txn_id: self.id,
            table: table.to_string(),
            row,
        })?)?)
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<(), Error> {
        deserialize(&self.raft.mutate(serialize(&Mutation::Delete {
            txn_id: self.id,
            table: table.to_string(),
            id: id.clone(),
        })?)?)
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>, Error> {
        deserialize(&self.raft.query(serialize(&Query::Read {
            txn_id: self.id,
            table: table.to_string(),
            id: id.clone(),
        })?)?)
    }

    fn read_index(
        &self,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<HashSet<Value>, Error> {
        deserialize(&self.raft.query(serialize(&Query::ReadIndex {
            txn_id: self.id,
            table: table.to_string(),
            column: column.to_string(),
            value: value.clone(),
        })?)?)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<super::Scan, Error> {
        Ok(Box::new(
            deserialize::<Vec<_>>(&self.raft.query(serialize(&Query::Scan {
                txn_id: self.id,
                table: table.to_string(),
                filter,
            })?)?)?
            .into_iter()
            .map(Ok),
        ))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<super::IndexScan, Error> {
        Ok(Box::new(
            deserialize::<Vec<_>>(&self.raft.query(serialize(&Query::ScanIndex {
                txn_id: self.id,
                table: table.to_string(),
                column: column.to_string(),
            })?)?)?
            .into_iter()
            .map(Ok),
        ))
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<(), Error> {
        deserialize(&self.raft.mutate(serialize(&Mutation::Update {
            txn_id: self.id,
            table: table.to_string(),
            id: id.clone(),
            row,
        })?)?)
    }
}

impl Catalog for Transaction {
    fn create_table(&mut self, table: &Table) -> Result<(), Error> {
        deserialize(&self.raft.mutate(serialize(&Mutation::CreateTable {
            txn_id: self.id,
            schema: table.clone(),
        })?)?)
    }

    fn delete_table(&mut self, table: &str) -> Result<(), Error> {
        deserialize(&self.raft.mutate(serialize(&Mutation::DeleteTable {
            txn_id: self.id,
            table: table.to_string(),
        })?)?)
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>, Error> {
        deserialize(
            &self.raft.query(serialize(&Query::ReadTable {
                txn_id: self.id,
                table: table.to_string(),
            })?)?,
        )
    }

    fn scan_tables(&self) -> Result<Tables, Error> {
        Ok(Box::new(
            deserialize::<Vec<_>>(
                &self.raft.query(serialize(&Query::ScanTables { txn_id: self.id })?)?,
            )?
            .into_iter(),
        ))
    }
}

/// The Raft state machine for the Raft-based SQL engine, using a KV SQL engine
pub struct State<S: kv::storage::Storage> {
    /// The underlying KV SQL engine
    engine: super::KV<S>,
}

impl<S: kv::storage::Storage> State<S> {
    /// Creates a new Raft state maching using the given MVCC key/value store
    pub fn new(store: kv::MVCC<S>) -> Self {
        State { engine: super::KV::new(store) }
    }
}

impl<S: kv::storage::Storage> raft::State for State<S> {
    fn mutate(&mut self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match deserialize(&command)? {
            Mutation::Begin(mode) => serialize(&self.engine.begin(mode)?.id()),
            Mutation::Commit(txn_id) => serialize(&self.engine.resume(txn_id)?.commit()?),
            Mutation::Rollback(txn_id) => serialize(&self.engine.resume(txn_id)?.rollback()?),

            Mutation::Create { txn_id, table, row } => {
                serialize(&self.engine.resume(txn_id)?.create(&table, row)?)
            }
            Mutation::Delete { txn_id, table, id } => {
                serialize(&self.engine.resume(txn_id)?.delete(&table, &id)?)
            }
            Mutation::Update { txn_id, table, id, row } => {
                serialize(&self.engine.resume(txn_id)?.update(&table, &id, row)?)
            }

            Mutation::CreateTable { txn_id, schema } => {
                serialize(&self.engine.resume(txn_id)?.create_table(&schema)?)
            }
            Mutation::DeleteTable { txn_id, table } => {
                serialize(&self.engine.resume(txn_id)?.delete_table(&table)?)
            }
        }
    }

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match deserialize(&command)? {
            Query::Resume(id) => {
                let txn = self.engine.resume(id)?;
                serialize(&(txn.id(), txn.mode()))
            }

            Query::Read { txn_id, table, id } => {
                serialize(&self.engine.resume(txn_id)?.read(&table, &id)?)
            }
            Query::ReadIndex { txn_id, table, column, value } => {
                serialize(&self.engine.resume(txn_id)?.read_index(&table, &column, &value)?)
            }
            // FIXME These need to stream rows somehow
            Query::Scan { txn_id, table, filter } => serialize(
                &self
                    .engine
                    .resume(txn_id)?
                    .scan(&table, filter)?
                    .collect::<Result<Vec<_>, Error>>()?,
            ),
            Query::ScanIndex { txn_id, table, column } => serialize(
                &self
                    .engine
                    .resume(txn_id)?
                    .scan_index(&table, &column)?
                    .collect::<Result<Vec<_>, Error>>()?,
            ),

            Query::ReadTable { txn_id, table } => {
                serialize(&self.engine.resume(txn_id)?.read_table(&table)?)
            }
            Query::ScanTables { txn_id } => {
                serialize(&self.engine.resume(txn_id)?.scan_tables()?.collect::<Vec<_>>())
            }
        }
    }
}
