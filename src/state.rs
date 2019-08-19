use crate::kv;
use crate::raft;
use crate::Error;
use serde_derive::{Deserialize, Serialize};

/// A state machine mutation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Mutation {
    /// Sets a key to a value.
    Set { key: String, value: Vec<u8> },
}

/// A state machine read.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Read {
    /// Fethes a value.
    Get(String),
}

/// A basic key/value state machine.
#[derive(Debug)]
pub struct State {
    // The backing store.
    kv: Box<dyn kv::Store>,
}

impl State {
    /// Creates a new kv state machine.
    pub fn new<K: kv::Store>(kv: K) -> Self {
        State { kv: Box::new(kv) }
    }
}

impl raft::State for State {
    fn mutate(&mut self, bytes: Vec<u8>) -> Result<Vec<u8>, Error> {
        let mutation: Mutation = deserialize(bytes)?;
        match mutation {
            Mutation::Set { key, value } => {
                info!("Setting {} to {:?}", key, value);
                self.kv.set(&key, value)?;
                Ok(vec![])
            }
        }
    }

    fn read(&self, bytes: Vec<u8>) -> Result<Vec<u8>, Error> {
        let read: Read = deserialize(bytes)?;
        match read {
            Read::Get(key) => {
                info!("Getting {}", key);
                Ok(self.kv.get(&key)?.unwrap_or(serialize("")?))
            }
        }
    }
}

/// Deserializes a value from a byte buffer
pub fn deserialize<'de, V: serde::Deserialize<'de>>(bytes: Vec<u8>) -> Result<V, Error> {
    Ok(serde::Deserialize::deserialize(&mut rmps::Deserializer::new(&bytes[..]))?)
}

/// Serializes a value into a byte buffer
pub fn serialize<V: serde::Serialize>(value: V) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    value.serialize(&mut rmps::Serializer::new(&mut bytes))?;
    Ok(bytes)
}
