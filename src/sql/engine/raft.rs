use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use crossbeam::channel::Sender;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{Catalog, Engine as _, Transaction as _};
use crate::encoding::{self, bincode, Value as _};
use crate::errdata;
use crate::error::Result;
use crate::raft;
use crate::sql::types::{Expression, Row, Rows, Table, Value};
use crate::storage::{self, mvcc};

/// A Raft-based SQL engine. This dispatches to the `Local` engine for local
/// storage and processing on each node, but plumbs read/write commands through
/// Raft for distributed consensus.
///
/// The `Raft` engine itself is simply a Raft client which sends `raft::Request`
/// requests to the local Raft node for processing. These requests are received
/// and processed by the Raft engine's `State` state machine running below Raft
/// on each node, which forwards the commands to a `Local` SQL engine which
/// actually executes them using a `storage::Engine` for local storage.
///
/// For more details on how SQL statements flow through the engine, see the
/// `sql` module documentation.
pub struct Raft {
    /// Sends requests to the local Raft node, along with a response channel.
    tx: Sender<(raft::Request, Sender<Result<raft::Response>>)>,
}

impl Raft {
    /// The unversioned key used to store the applied index. Just uses a string
    /// for simplicity.
    pub const APPLIED_INDEX_KEY: &'static [u8] = b"applied_index";

    /// Creates a new Raft-based SQL engine, given a Raft request channel to the
    /// local Raft node.
    pub fn new(tx: Sender<(raft::Request, Sender<Result<raft::Response>>)>) -> Self {
        Self { tx }
    }

    /// Creates the Raft-managed state machine for the Raft engine. Receives
    /// commands from the Raft engine and executes them on a `Local` engine.
    pub fn new_state<E: storage::Engine + 'static>(engine: E) -> Result<State<E>> {
        State::new(engine)
    }

    /// Executes a request against the Raft cluster, waiting for the response.
    fn execute(&self, request: raft::Request) -> Result<raft::Response> {
        let (response_tx, response_rx) = crossbeam::channel::bounded(1);
        self.tx.send((request, response_tx))?;
        response_rx.recv()?
    }

    /// Writes through Raft, deserializing the response into the return type.
    fn write<V: DeserializeOwned>(&self, write: Write) -> Result<V> {
        match self.execute(raft::Request::Write(write.encode()))? {
            raft::Response::Write(response) => bincode::deserialize(&response),
            response => errdata!("unexpected Raft write response {response:?}"),
        }
    }

    /// Reads from Raft, deserializing the response into the return type.
    fn read<V: DeserializeOwned>(&self, read: Read) -> Result<V> {
        match self.execute(raft::Request::Read(read.encode()))? {
            raft::Response::Read(response) => bincode::deserialize(&response),
            response => errdata!("unexpected Raft read response {response:?}"),
        }
    }

    /// Raft SQL engine status.
    pub fn status(&self) -> Result<Status> {
        let raft = match self.execute(raft::Request::Status)? {
            raft::Response::Status(status) => status,
            response => return errdata!("unexpected Raft status response {response:?}"),
        };
        let mvcc = self.read(Read::Status)?;
        Ok(Status { raft, mvcc })
    }
}

impl<'a> super::Engine<'a> for Raft {
    type Transaction = Transaction<'a>;

    fn begin(&'a self) -> Result<Self::Transaction> {
        Transaction::begin(self, false, None)
    }

    fn begin_read_only(&'a self) -> Result<Self::Transaction> {
        Transaction::begin(self, true, None)
    }

    fn begin_as_of(&'a self, version: mvcc::Version) -> Result<Self::Transaction> {
        Transaction::begin(self, true, Some(version))
    }
}

/// A Raft SQL engine transaction.
///
/// This keeps track of the transaction state in memory, for the benefit of
/// read-only transactions. An `mvcc::Transaction` normally encapsulates this
/// and manages it in memory, but since `mvcc::Transaction` runs below Raft, it
/// can't maintain this state between individual requests (which could execute
/// on different leaders). Instead, we use `mvcc::Transaction::resume` to resume
/// the transaction using the provided transaction state for each request.
pub struct Transaction<'a> {
    /// The Raft SQL engine, used to communicate with Raft.
    engine: &'a Raft,
    /// The MVCC transaction state.
    state: mvcc::TransactionState,
}

impl<'a> Transaction<'a> {
    /// Starts a transaction in the given mode.
    fn begin(engine: &'a Raft, read_only: bool, as_of: Option<mvcc::Version>) -> Result<Self> {
        assert!(as_of.is_none() || read_only, "can't use as_of without read_only");
        // Read-only transactions don't need to persist anything, they just need
        // to grab the current transaction state, so submit them as reads to
        // avoid a replication roundtrip.
        let state = if read_only || as_of.is_some() {
            engine.read(Read::BeginReadOnly { as_of })?
        } else {
            engine.write(Write::Begin)?
        };
        Ok(Self { engine, state })
    }
}

impl super::Transaction for Transaction<'_> {
    fn state(&self) -> &mvcc::TransactionState {
        &self.state
    }

    fn commit(self) -> Result<()> {
        if self.state.read_only {
            return Ok(()); // noop
        }
        self.engine.write(Write::Commit(self.state.into()))
    }

    fn rollback(self) -> Result<()> {
        if self.state.read_only {
            return Ok(()); // noop
        }
        self.engine.write(Write::Rollback(self.state.into()))
    }

    fn delete(&self, table: &str, ids: &[Value]) -> Result<()> {
        self.engine.write(Write::Delete {
            txn: (&self.state).into(),
            table: table.into(),
            ids: ids.into(),
        })
    }

    fn get(&self, table: &str, ids: &[Value]) -> Result<Vec<Row>> {
        self.engine.read(Read::Get {
            txn: (&self.state).into(),
            table: table.into(),
            ids: ids.into(),
        })
    }

    fn insert(&self, table: &str, rows: Vec<Row>) -> Result<()> {
        self.engine.write(Write::Insert { txn: (&self.state).into(), table: table.into(), rows })
    }

    fn lookup_index(&self, table: &str, column: &str, values: &[Value]) -> Result<BTreeSet<Value>> {
        self.engine.read(Read::LookupIndex {
            txn: (&self.state).into(),
            table: table.into(),
            column: column.into(),
            values: values.into(),
        })
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Rows> {
        let scan: Vec<Row> = self.engine.read(Read::Scan {
            txn: (&self.state).into(),
            table: table.into(),
            filter,
        })?;
        Ok(Box::new(scan.into_iter().map(Ok)))
    }

    fn update(&self, table: &str, rows: BTreeMap<Value, Row>) -> Result<()> {
        self.engine.write(Write::Update { txn: (&self.state).into(), table: table.into(), rows })
    }
}

impl Catalog for Transaction<'_> {
    fn create_table(&self, schema: Table) -> Result<()> {
        self.engine.write(Write::CreateTable { txn: (&self.state).into(), schema })
    }

    fn drop_table(&self, table: &str, if_exists: bool) -> Result<bool> {
        self.engine.write(Write::DropTable {
            txn: (&self.state).into(),
            table: table.into(),
            if_exists,
        })
    }

    fn get_table(&self, table: &str) -> Result<Option<Table>> {
        self.engine.read(Read::GetTable { txn: (&self.state).into(), table: table.into() })
    }

    fn list_tables(&self) -> Result<Vec<Table>> {
        self.engine.read(Read::ListTables { txn: (&self.state).into() })
    }
}

/// The state machine for the Raft SQL engine. Receives commands from the Raft
/// SQL engine and dispatches to a `Local` SQL engine which does the actual
/// work, using a `storage::Engine` for storage.
///
/// For simplicity, we don't attempt to stream large requests or responses,
/// instead simply delivering them as one large chunk. This means that e.g. a
/// full table scan will pull the entire table into memory, serialize it, and
/// send it across the network as one message. The simplest way to address this
/// would likely be to send row batches, but this is fine for our purposes.
pub struct State<E: storage::Engine + 'static> {
    /// The local SQL engine.
    local: super::Local<E>,
    /// The last applied index. This tells Raft which command to apply next.
    applied_index: raft::Index,
}

impl<E: storage::Engine> State<E> {
    /// Creates a new Raft state maching using the given storage engine for
    /// local storage.
    pub fn new(engine: E) -> Result<Self> {
        let local = super::Local::new(engine);
        let applied_index = local
            .get_unversioned(Raft::APPLIED_INDEX_KEY)?
            .map(|b| bincode::deserialize(&b))
            .transpose()?
            .unwrap_or(0);
        Ok(State { local, applied_index })
    }

    /// Executes a write command.
    fn write(&self, command: Write) -> Result<Vec<u8>> {
        Ok(match command {
            Write::Begin => self.local.begin()?.state().encode(),
            Write::Commit(txn) => {
                bincode::serialize(&self.local.resume(txn.into_owned())?.commit()?)
            }
            Write::Rollback(txn) => {
                bincode::serialize(&self.local.resume(txn.into_owned())?.rollback()?)
            }

            Write::Delete { txn, table, ids } => {
                bincode::serialize(&self.local.resume(txn.into_owned())?.delete(&table, &ids)?)
            }
            Write::Insert { txn, table, rows } => {
                bincode::serialize(&self.local.resume(txn.into_owned())?.insert(&table, rows)?)
            }
            Write::Update { txn, table, rows } => {
                bincode::serialize(&self.local.resume(txn.into_owned())?.update(&table, rows)?)
            }

            Write::CreateTable { txn, schema } => {
                bincode::serialize(&self.local.resume(txn.into_owned())?.create_table(schema)?)
            }
            Write::DropTable { txn, table, if_exists } => bincode::serialize(
                &self.local.resume(txn.into_owned())?.drop_table(&table, if_exists)?,
            ),
        })
    }
}

impl<E: storage::Engine> raft::State for State<E> {
    fn get_applied_index(&self) -> raft::Index {
        self.applied_index
    }

    fn apply(&mut self, entry: raft::Entry) -> Result<Vec<u8>> {
        assert_eq!(entry.index, self.applied_index + 1, "entry index not after applied index");

        let result = match &entry.command {
            Some(command) => match self.write(Write::decode(command)?) {
                // Panic on non-deterministic apply failures, to prevent replica
                // divergence. See [`raft::State`] docs for details.
                Err(e) if !e.is_deterministic() => panic!("non-deterministic apply failure: {e}"),
                result => result,
            },
            // Raft submits noop commands on leader changes. Ignore them, but
            // record the applied index below.
            None => Ok(Vec::new()),
        };

        // Persist the applied index. We don't have to flush, because it's ok to
        // lose a tail of the state machine writes (e.g. if the machine
        // crashes). Raft will replay the log from the last known applied index.
        self.applied_index = entry.index;
        self.local.set_unversioned(Raft::APPLIED_INDEX_KEY, bincode::serialize(&entry.index))?;
        result
    }

    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        Ok(match Read::decode(&command)? {
            Read::BeginReadOnly { as_of } => {
                let txn = match as_of {
                    Some(version) => self.local.begin_as_of(version)?,
                    None => self.local.begin_read_only()?,
                };
                txn.state().encode()
            }
            Read::Status => self.local.mvcc.status()?.encode(),

            Read::Get { txn, table, ids } => {
                self.local.resume(txn.into_owned())?.get(&table, &ids)?.encode()
            }
            Read::LookupIndex { txn, table, column, values } => self
                .local
                .resume(txn.into_owned())?
                .lookup_index(&table, &column, &values)?
                .encode(),
            Read::Scan { txn, table, filter } => {
                // For simplicity, buffer the entire scan. See `State` comment.
                self.local
                    .resume(txn.into_owned())?
                    .scan(&table, filter)?
                    .collect::<Result<Vec<Row>>>()?
                    .encode()
            }

            Read::GetTable { txn, table } => {
                self.local.resume(txn.into_owned())?.get_table(&table)?.encode()
            }
            Read::ListTables { txn } => {
                self.local.resume(txn.into_owned())?.list_tables()?.encode()
            }
        })
    }
}

/// A Raft engine read. Values correspond to engine method parameters. Uses
/// Cows to allow borrowed encoding and owned decoding.
#[derive(Debug, Serialize, Deserialize)]
pub enum Read<'a> {
    BeginReadOnly {
        as_of: Option<mvcc::Version>,
    },
    Status,

    Get {
        txn: Cow<'a, mvcc::TransactionState>,
        table: Cow<'a, str>,
        ids: Cow<'a, [Value]>,
    },
    LookupIndex {
        txn: Cow<'a, mvcc::TransactionState>,
        table: Cow<'a, str>,
        column: Cow<'a, str>,
        values: Cow<'a, [Value]>,
    },
    Scan {
        txn: Cow<'a, mvcc::TransactionState>,
        table: Cow<'a, str>,
        filter: Option<Expression>,
    },

    GetTable {
        txn: Cow<'a, mvcc::TransactionState>,
        table: Cow<'a, str>,
    },
    ListTables {
        txn: Cow<'a, mvcc::TransactionState>,
    },
}

impl encoding::Value for Read<'_> {}

/// A Raft engine write. Values correspond to engine method parameters. Uses
/// Cows to allow borrowed encoding (for borrowed params) and owned decoding.
#[derive(Debug, Serialize, Deserialize)]
pub enum Write<'a> {
    Begin,
    Commit(Cow<'a, mvcc::TransactionState>),
    Rollback(Cow<'a, mvcc::TransactionState>),

    Delete { txn: Cow<'a, mvcc::TransactionState>, table: Cow<'a, str>, ids: Cow<'a, [Value]> },
    Insert { txn: Cow<'a, mvcc::TransactionState>, table: Cow<'a, str>, rows: Vec<Row> },
    Update { txn: Cow<'a, mvcc::TransactionState>, table: Cow<'a, str>, rows: BTreeMap<Value, Row> },

    CreateTable { txn: Cow<'a, mvcc::TransactionState>, schema: Table },
    DropTable { txn: Cow<'a, mvcc::TransactionState>, table: Cow<'a, str>, if_exists: bool },
}

impl encoding::Value for Write<'_> {}

/// Raft SQL engine status.
#[derive(Serialize, Deserialize)]
pub struct Status {
    pub raft: raft::Status,
    pub mvcc: mvcc::Status,
}
