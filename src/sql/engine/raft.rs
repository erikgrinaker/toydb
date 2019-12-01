use super::super::schema;
use super::super::types;
use super::Engine as _;
use super::Transaction as _;
use crate::kv;
use crate::raft;
use crate::utility::{deserialize, serialize};
use crate::Error;

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
    BeginReadOnly(Option<u64>),
    Resume(u64),

    Read { txn_id: u64, table: String, id: types::Value },
    Scan { txn_id: u64, table: String },

    ListTables { txn_id: u64 },
    ReadTable { txn_id: u64, table: String },

    // FIXME Snapshot queries shouldn't be separate variants
    ReadSnapshot { version: u64, table: String, id: types::Value },
    ScanSnapshot { version: u64, table: String },

    ListTablesSnapshot { version: u64 },
    ReadTableSnapshot { version: u64, table: String },
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
    type Snapshot = Snapshot;

    fn begin(&self) -> Result<Self::Transaction, Error> {
        Transaction::new(self.raft.clone())
    }

    fn resume(&self, id: u64) -> Result<Self::Transaction, Error> {
        Transaction::resume(self.raft.clone(), id)
    }

    fn snapshot(&self, version: Option<u64>) -> Result<Self::Snapshot, Error> {
        Snapshot::new(self.raft.clone(), version)
    }
}

#[derive(Clone)]
pub struct Transaction {
    raft: raft::Raft,
    id: u64,
}

impl Transaction {
    fn new(raft: raft::Raft) -> Result<Self, Error> {
        let id = deserialize(raft.mutate(serialize(Mutation::Begin)?)?)?;
        Ok(Self { raft, id })
    }

    fn resume(raft: raft::Raft, id: u64) -> Result<Self, Error> {
        let id = deserialize(raft.query(serialize(Query::Resume(id))?)?)?;
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
        deserialize(self.raft.query(serialize(Query::Read {
            txn_id: self.id,
            table: table.into(),
            id: id.clone(),
        })?)?)
    }

    fn scan(&self, table: &str) -> Result<super::Scan, Error> {
        Ok(Box::new(
            deserialize::<Vec<types::Row>>(
                self.raft
                    .query(serialize(Query::Scan { txn_id: self.id, table: table.into() })?)?,
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
        deserialize(self.raft.query(serialize(Query::ListTables { txn_id: self.id })?)?)
    }

    fn read_table(&self, table: &str) -> Result<Option<schema::Table>, Error> {
        deserialize(
            self.raft
                .query(serialize(Query::ReadTable { txn_id: self.id, table: table.into() })?)?,
        )
    }
}

#[derive(Clone)]
pub struct Snapshot {
    raft: raft::Raft,
    version: u64,
}

impl Snapshot {
    fn new(raft: raft::Raft, version: Option<u64>) -> Result<Self, Error> {
        let version = deserialize(raft.query(serialize(Query::BeginReadOnly(version))?)?)?;
        Ok(Self { raft, version })
    }
}

impl super::Transaction for Snapshot {
    fn id(&self) -> u64 {
        self.version
    }

    fn commit(self) -> Result<(), Error> {
        Ok(())
    }

    fn rollback(self) -> Result<(), Error> {
        Ok(())
    }

    fn create(&mut self, _table: &str, _row: types::Row) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn delete(&mut self, _table: &str, _id: &types::Value) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn read(&self, table: &str, id: &types::Value) -> Result<Option<types::Row>, Error> {
        deserialize(self.raft.query(serialize(Query::ReadSnapshot {
            version: self.version,
            table: table.into(),
            id: id.clone(),
        })?)?)
    }

    fn scan(&self, table: &str) -> Result<super::Scan, Error> {
        Ok(Box::new(
            deserialize::<Vec<types::Row>>(self.raft.query(serialize(Query::ScanSnapshot {
                version: self.version,
                table: table.into(),
            })?)?)?
            .into_iter()
            .map(Ok),
        ))
    }

    fn update(&mut self, _table: &str, _id: &types::Value, _row: types::Row) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn create_table(&mut self, _table: &schema::Table) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn delete_table(&mut self, _table: &str) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn list_tables(&self) -> Result<Vec<schema::Table>, Error> {
        deserialize(
            self.raft.query(serialize(Query::ListTablesSnapshot { version: self.version })?)?,
        )
    }

    fn read_table(&self, table: &str) -> Result<Option<schema::Table>, Error> {
        deserialize(self.raft.query(serialize(Query::ReadTableSnapshot {
            version: self.version,
            table: table.into(),
        })?)?)
    }
}

/// The underlying state machine for the Raft-based engine
pub struct State<S: kv::storage::Storage> {
    engine: super::KV<S>,
}

impl<S: kv::storage::Storage> std::fmt::Debug for State<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "State")
    }
}

impl<S: kv::storage::Storage> State<S> {
    pub fn new(store: kv::MVCC<S>) -> Self {
        State { engine: super::KV::new(store) }
    }
}

impl<S: kv::storage::Storage> raft::State for State<S> {
    fn mutate(&mut self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match deserialize(command)? {
            Mutation::Begin => serialize(self.engine.begin()?.id()),
            Mutation::Commit(txn_id) => serialize(self.engine.resume(txn_id)?.commit()?),
            Mutation::Rollback(txn_id) => serialize(self.engine.resume(txn_id)?.rollback()?),

            Mutation::Create { txn_id, table, row } => {
                serialize(self.engine.resume(txn_id)?.create(&table, row)?)
            }
            Mutation::Delete { txn_id, table, id } => {
                serialize(self.engine.resume(txn_id)?.delete(&table, &id)?)
            }
            Mutation::Update { txn_id, table, id, row } => {
                serialize(self.engine.resume(txn_id)?.update(&table, &id, row)?)
            }

            Mutation::CreateTable { txn_id, schema } => {
                serialize(self.engine.resume(txn_id)?.create_table(&schema)?)
            }
            Mutation::DeleteTable { txn_id, table } => {
                serialize(self.engine.resume(txn_id)?.delete_table(&table)?)
            }
        }
    }

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match deserialize(command)? {
            Query::BeginReadOnly(version) => serialize(self.engine.snapshot(version)?.id()),
            Query::Resume(id) => serialize(self.engine.resume(id)?.id()),

            Query::Read { txn_id, table, id } => {
                serialize(self.engine.resume(txn_id)?.read(&table, &id)?)
            }
            // FIXME This needs to stream rows
            Query::Scan { txn_id, table } => serialize(
                self.engine
                    .resume(txn_id)?
                    .scan(&table)?
                    .collect::<Result<Vec<types::Row>, Error>>()?,
            ),

            Query::ListTables { txn_id } => serialize(self.engine.resume(txn_id)?.list_tables()?),
            Query::ReadTable { txn_id, table } => {
                serialize(self.engine.resume(txn_id)?.read_table(&table)?)
            }

            Query::ReadSnapshot { version, table, id } => {
                serialize(self.engine.snapshot(Some(version))?.read(&table, &id)?)
            }
            // FIXME This needs to stream rows
            Query::ScanSnapshot { version, table } => serialize(
                self.engine
                    .snapshot(Some(version))?
                    .scan(&table)?
                    .collect::<Result<Vec<types::Row>, Error>>()?,
            ),

            Query::ListTablesSnapshot { version } => {
                serialize(self.engine.snapshot(Some(version))?.list_tables()?)
            }
            Query::ReadTableSnapshot { version, table } => {
                serialize(self.engine.snapshot(Some(version))?.read_table(&table)?)
            }
        }
    }
}
