use super::super::schema;
use super::super::types;
use super::Engine as _;
use super::Transaction as _;
use crate::kv;
use crate::raft;
use crate::utility::{deserialize, serialize};
use crate::Error;
use std::collections::HashMap;

/// A state machine mutation
#[derive(Clone, Debug, Serialize, Deserialize)]
enum Mutation {
    Begin,
    Commit(u64),
    Rollback(u64),

    Create { txn_id: u64, table: String, row: types::Row },
    Delete { txn_id: u64, table: String, id: types::Value },
    Update { txn_id: u64, table: String, id: types::Value, row: types::Row },

    CreateTable { txn_id: u64, schema: schema::Table },
    DeleteTable { txn_id: u64, table: String },
}

/// A state machine query
#[derive(Clone, Debug, Serialize, Deserialize)]
enum Query {
    Read { txn_id: u64, table: String, id: types::Value },
    Scan { txn_id: u64, table: String },

    ListTables { txn_id: u64 },
    ReadTable { txn_id: u64, table: String },
}

pub struct Raft {
    raft: raft::Raft,
}

impl Raft {
    /// Creates a new SQL engine around a Raft cluster.
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

    fn begin(&self) -> Result<Self::Transaction, Error> {
        Transaction::new(self.raft.clone())
    }
}

#[derive(Clone)]
pub struct Transaction {
    id: u64,
    raft: raft::Raft,
}

impl Transaction {
    fn new(raft: raft::Raft) -> Result<Self, Error> {
        let id = deserialize(raft.mutate(serialize(Mutation::Begin)?)?)?;
        Ok(Self { raft, id })
    }
}

impl super::Transaction for Transaction {
    fn id(&self) -> u64 {
        self.id
    }

    fn commit(self) -> Result<(), Error> {
        deserialize(self.raft.mutate(serialize(Mutation::Commit(self.id))?)?)
    }

    fn rollback(self) -> Result<(), Error> {
        deserialize(self.raft.mutate(serialize(Mutation::Rollback(self.id))?)?)
    }

    fn create(&mut self, table: &str, row: types::Row) -> Result<(), Error> {
        deserialize(self.raft.mutate(serialize(Mutation::Create {
            txn_id: self.id,
            table: table.into(),
            row,
        })?)?)
    }

    fn delete(&mut self, table: &str, id: &types::Value) -> Result<(), Error> {
        deserialize(self.raft.mutate(serialize(Mutation::Delete {
            txn_id: self.id,
            table: table.into(),
            id: id.clone(),
        })?)?)
    }

    fn read(&self, table: &str, id: &types::Value) -> Result<Option<types::Row>, Error> {
        deserialize(self.raft.read(serialize(Query::Read {
            txn_id: self.id,
            table: table.into(),
            id: id.clone(),
        })?)?)
    }

    fn scan(&self, table: &str) -> Result<super::Scan, Error> {
        Ok(Box::new(
            deserialize::<Vec<types::Row>>(
                self.raft.read(serialize(Query::Scan { txn_id: self.id, table: table.into() })?)?,
            )?
            .into_iter()
            .map(Ok),
        ))
    }

    fn update(&mut self, table: &str, id: &types::Value, row: types::Row) -> Result<(), Error> {
        deserialize(self.raft.mutate(serialize(Mutation::Update {
            txn_id: self.id,
            table: table.into(),
            id: id.clone(),
            row,
        })?)?)
    }

    fn create_table(&mut self, table: &schema::Table) -> Result<(), Error> {
        deserialize(
            self.raft.mutate(serialize(Mutation::CreateTable {
                txn_id: self.id,
                schema: table.clone(),
            })?)?,
        )
    }

    fn delete_table(&mut self, table: &str) -> Result<(), Error> {
        deserialize(
            self.raft.mutate(serialize(Mutation::DeleteTable {
                txn_id: self.id,
                table: table.into(),
            })?)?,
        )
    }

    fn list_tables(&self) -> Result<Vec<schema::Table>, Error> {
        deserialize(self.raft.read(serialize(Query::ListTables { txn_id: self.id })?)?)
    }

    fn read_table(&self, table: &str) -> Result<Option<schema::Table>, Error> {
        deserialize(
            self.raft
                .read(serialize(Query::ReadTable { txn_id: self.id, table: table.into() })?)?,
        )
    }
}

/// The underlying state machine for the Raft-based engine
pub struct State<S: kv::storage::Storage> {
    engine: super::KV<S>,
    txns: HashMap<u64, <super::KV<S> as super::Engine>::Transaction>,
}

impl<S: kv::storage::Storage> std::fmt::Debug for State<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "State")
    }
}

impl<S: kv::storage::Storage> State<S> {
    pub fn new(store: kv::MVCC<S>) -> Self {
        State { engine: super::KV::new(store), txns: HashMap::new() }
    }
}

impl<S: kv::storage::Storage> raft::State for State<S> {
    fn mutate(&mut self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match deserialize(command)? {
            Mutation::Begin => {
                let txn = self.engine.begin()?;
                let txn_id = txn.id();
                self.txns.insert(txn_id, txn);
                serialize(txn_id)
            }
            Mutation::Commit(txn_id) => serialize(
                self.txns
                    .remove(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .commit()?,
            ),
            Mutation::Rollback(txn_id) => serialize(
                self.txns
                    .remove(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .rollback()?,
            ),

            Mutation::Create { txn_id, table, row } => serialize(
                self.txns
                    .get_mut(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .create(&table, row)?,
            ),
            Mutation::Delete { txn_id, table, id } => serialize(
                self.txns
                    .get_mut(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .delete(&table, &id)?,
            ),
            Mutation::Update { txn_id, table, id, row } => serialize(
                self.txns
                    .get_mut(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .update(&table, &id, row)?,
            ),

            Mutation::CreateTable { txn_id, schema } => serialize(
                self.txns
                    .get_mut(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .create_table(&schema)?,
            ),
            Mutation::DeleteTable { txn_id, table } => serialize(
                self.txns
                    .get_mut(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .delete_table(&table)?,
            ),
        }
    }

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match deserialize(command)? {
            Query::Read { txn_id, table, id } => serialize(
                self.txns
                    .get(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .read(&table, &id)?,
            ),
            Query::Scan { txn_id, table } => serialize(
                // FIXME This needs to stream rows
                self.txns
                    .get(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .scan(&table)?
                    .collect::<Result<Vec<types::Row>, Error>>()?,
            ),

            Query::ListTables { txn_id } => serialize(
                self.txns
                    .get(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .list_tables()?,
            ),
            Query::ReadTable { txn_id, table } => serialize(
                self.txns
                    .get(&txn_id)
                    .ok_or_else(|| Error::Internal("Unknown transaction".into()))?
                    .read_table(&table)?,
            ),
        }
    }
}
