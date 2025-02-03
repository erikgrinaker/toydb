use super::{Entry, Index};
use crate::error::Result;

/// A Raft-managed state machine. Raft itself does not care what the state
/// machine is, nor what the commands and results do -- it will simply apply
/// arbitrary binary commands sequentially from the Raft log, returning an
/// arbitrary binary result to the client.
///
/// Since commands are applied identically across all replicas, they must be
/// deterministic and yield the same state and result across all replicas too.
/// Otherwise, the replicas will diverge, and different replicas will produce
/// different results.
///
/// Write commands (`Request::Write`) are replicated and applied on all replicas
/// via `State::apply`. The state machine must keep track of the last applied
/// index and return it via `State::get_applied_index`. Read commands
/// (`Request::Read`) are only executed on a single replica via `State::read`
/// and must not make any state changes.
pub trait State: Send {
    /// Returns the last applied index from the state machine.
    ///
    /// This must correspond to the current state of the state machine, since it
    /// determines which command to apply next. In particular, a node crash may
    /// result in partial command application or data loss, which must be
    /// handled appropriately.
    fn get_applied_index(&self) -> Index;

    /// Applies a log entry to the state machine, returning a client result.
    /// Errors are considered applied and propagated back to the client.
    ///
    /// This is executed on all replicas, so the result must be deterministic:
    /// it must yield the same state and result on all replicas, even if the
    /// command is reapplied following a node crash.
    ///
    /// Any non-deterministic apply error (e.g. an IO error) must panic and
    /// crash the node -- if it instead returns an error to the client, the
    /// command is considered applied and replica states will diverge. The state
    /// machine is responsible for panicing when appropriate.
    ///
    /// The entry may contain a noop command, which is committed by Raft during
    /// leader changes. This still needs to be applied to the state machine to
    /// properly update the applied index, and should return an empty result.
    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>>;

    /// Executes a read command in the state machine, returning a client result.
    /// Errors are also propagated back to the client.
    ///
    /// This is only executed on a single replica/node, so it must not result in
    /// any state changes (i.e. it must not write).
    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}

/// Test helper state machines.
#[cfg(test)]
pub mod test {
    use std::{collections::BTreeMap, fmt::Display};

    use crossbeam::channel::Sender;
    use itertools::Itertools as _;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::encoding::{self, Value as _};

    /// Wraps a state machine and emits applied entries to the provided channel.
    pub struct Emit {
        inner: Box<dyn State>,
        tx: Sender<Entry>,
    }

    impl Emit {
        pub fn new(inner: Box<dyn State>, tx: Sender<Entry>) -> Box<Self> {
            Box::new(Self { inner, tx })
        }
    }

    impl State for Emit {
        fn get_applied_index(&self) -> Index {
            self.inner.get_applied_index()
        }

        fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
            let response = self.inner.apply(entry.clone())?;
            self.tx.send(entry)?;
            Ok(response)
        }

        fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
            self.inner.read(command)
        }
    }

    /// A simple string key/value store. Takes KVCommands.
    pub struct KV {
        applied_index: Index,
        data: BTreeMap<String, String>,
    }

    impl KV {
        pub fn new() -> Box<Self> {
            Box::new(Self { applied_index: 0, data: BTreeMap::new() })
        }
    }

    impl State for KV {
        fn get_applied_index(&self) -> Index {
            self.applied_index
        }

        fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
            let command = entry.command.as_deref().map(KVCommand::decode).transpose()?;
            let response = match command {
                Some(KVCommand::Put { key, value }) => {
                    self.data.insert(key, value);
                    KVResponse::Put(entry.index).encode()
                }
                Some(c @ (KVCommand::Get { .. } | KVCommand::Scan)) => {
                    panic!("{c} submitted as write command")
                }
                None => Vec::new(),
            };
            self.applied_index = entry.index;
            Ok(response)
        }

        fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
            match KVCommand::decode(&command)? {
                KVCommand::Get { key } => {
                    Ok(KVResponse::Get(self.data.get(&key).cloned()).encode())
                }
                KVCommand::Scan => Ok(KVResponse::Scan(self.data.clone()).encode()),
                c @ KVCommand::Put { .. } => panic!("{c} submitted as read command"),
            }
        }
    }

    /// A KV command. Returns the corresponding KVResponse.
    #[derive(Serialize, Deserialize)]
    pub enum KVCommand {
        /// Fetches the value of the given key.
        Get { key: String },
        /// Stores the given key/value pair, returning the applied index.
        Put { key: String, value: String },
        /// Returns all key/value pairs.
        Scan,
    }

    impl encoding::Value for KVCommand {}

    impl Display for KVCommand {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Get { key } => write!(f, "get {key}"),
                Self::Put { key, value } => write!(f, "put {key}={value}"),
                Self::Scan => write!(f, "scan"),
            }
        }
    }

    /// A KVCommand response.
    #[derive(Serialize, Deserialize)]
    pub enum KVResponse {
        /// Get returns the key's value, or None if it does not exist.
        Get(Option<String>),
        /// Put returns the applied index of the command.
        Put(Index),
        /// Scan returns the key/value pairs.
        Scan(BTreeMap<String, String>),
    }

    impl encoding::Value for KVResponse {}

    impl Display for KVResponse {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Get(Some(value)) => write!(f, "{value}"),
                Self::Get(None) => write!(f, "None"),
                Self::Put(applied_index) => write!(f, "{applied_index}"),
                Self::Scan(kvs) => {
                    write!(f, "{}", kvs.iter().map(|(k, v)| format!("{k}={v}")).join(","))
                }
            }
        }
    }

    /// A state machine which does nothing. All commands are ignored.
    pub struct Noop {
        applied_index: Index,
    }

    impl Noop {
        pub fn new() -> Box<Self> {
            Box::new(Self { applied_index: 0 })
        }
    }

    impl State for Noop {
        fn get_applied_index(&self) -> Index {
            self.applied_index
        }

        fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
            self.applied_index = entry.index;
            Ok(Vec::new())
        }

        fn read(&self, _: Vec<u8>) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
    }
}
