//! Decodes and formats raw keys and values, recursively as needed. Handles both
//! both Raft, MVCC, SQL, and raw binary data.

use super::{bincode, Key as _};
use crate::raft;
use crate::storage::mvcc;

use itertools::Itertools as _;
use std::collections::BTreeSet;

/// Formats raw key/value pairs.
pub trait Formatter {
    /// Formats a key.
    fn key(key: &[u8]) -> String;

    /// Formats a value.
    fn value(key: &[u8], value: &[u8]) -> String;

    /// Formats a key/value pair.
    fn key_value(key: &[u8], value: &[u8]) -> String {
        let fkey = Self::key(key);
        let fvalue = Self::value(key, value);
        format!("{fkey} → {fvalue}")
    }

    /// Formats a key/value pair, where the value may not exist.
    fn key_maybe_value(key: &[u8], value: Option<&[u8]>) -> String {
        let fkey = Self::key(key);
        let fvalue = value.map(|v| Self::value(key, v)).unwrap_or("None".to_string());
        format!("{fkey} → {fvalue}")
    }
}

/// Formats raw byte slices.
pub struct Raw;

impl Raw {
    pub fn bytes(bytes: &[u8]) -> String {
        let escaped = bytes.iter().copied().flat_map(std::ascii::escape_default).collect_vec();
        let string = String::from_utf8_lossy(&escaped);
        format!("\"{string}\"")
    }
}

impl Formatter for Raw {
    fn key(key: &[u8]) -> String {
        Self::bytes(key)
    }

    fn value(_: &[u8], value: &[u8]) -> String {
        Self::bytes(value)
    }
}

/// Formats Raft log entries.
pub struct Raft;

impl Raft {
    pub fn entry(entry: &raft::Entry) -> String {
        format!(
            "{}@{} {}",
            entry.index,
            entry.term,
            entry.command.as_deref().map(Raw::bytes).unwrap_or("None".to_string())
        )
    }
}

impl Formatter for Raft {
    fn key(key: &[u8]) -> String {
        let Ok(key) = raft::Key::decode(key) else {
            return Raw::key(key);
        };
        format!("raft:{key:?}")
    }

    fn value(key: &[u8], value: &[u8]) -> String {
        let Ok(key) = raft::Key::decode(key) else {
            return Raw::value(key, value);
        };
        match key {
            raft::Key::CommitIndex => {
                match bincode::deserialize::<(raft::Index, raft::Term)>(value) {
                    Ok((index, term)) => format!("{index}@{term}"),
                    Err(_) => Raw::bytes(value),
                }
            }
            raft::Key::TermVote => {
                match bincode::deserialize::<(raft::Term, Option<raft::NodeID>)>(value) {
                    Ok((term, vote)) => format!(
                        "term={term} vote={}",
                        vote.map(|v| v.to_string()).unwrap_or("None".to_string())
                    ),
                    Err(_) => Raw::bytes(value),
                }
            }
            raft::Key::Entry(_) => match bincode::deserialize::<raft::Entry>(value) {
                Ok(entry) => Self::entry(&entry),
                Err(_) => Raw::bytes(value),
            },
        }
    }
}

/// Formats MVCC keys/values. Dispatches to I for inner key/value formatting.
pub struct MVCC<I: Formatter>(std::marker::PhantomData<I>);

impl<I: Formatter> Formatter for MVCC<I> {
    fn key(key: &[u8]) -> String {
        let Ok(key) = mvcc::Key::decode(key) else { return Raw::key(key) };
        match key {
            mvcc::Key::TxnWrite(version, innerkey) => {
                format!("mvcc:TxnWrite({version}, {})", I::key(&innerkey))
            }
            mvcc::Key::Version(innerkey, version) => {
                format!("mvcc:Version({}, {version})", I::key(&innerkey))
            }
            mvcc::Key::Unversioned(innerkey) => {
                format!("mvcc:Unversioned({})", I::key(&innerkey))
            }
            mvcc::Key::NextVersion | mvcc::Key::TxnActive(_) | mvcc::Key::TxnActiveSnapshot(_) => {
                format!("mvcc:{key:?}")
            }
        }
    }

    fn value(key: &[u8], value: &[u8]) -> String {
        let Ok(key) = mvcc::Key::decode(key) else { return Raw::bytes(value) };
        match key {
            mvcc::Key::NextVersion => {
                let Ok(version) = bincode::deserialize::<mvcc::Version>(value) else {
                    return Raw::bytes(value);
                };
                version.to_string()
            }
            mvcc::Key::TxnActiveSnapshot(_) => {
                let Ok(active) = bincode::deserialize::<BTreeSet<u64>>(value) else {
                    return Raw::bytes(value);
                };
                format!("{{{}}}", active.iter().map(|v| v.to_string()).join(","))
            }
            mvcc::Key::TxnActive(_) | mvcc::Key::TxnWrite(_, _) => Raw::bytes(value),
            mvcc::Key::Version(userkey, _) => match bincode::deserialize(value) {
                Ok(Some(value)) => I::value(&userkey, value),
                Ok(None) => "None".to_string(),
                Err(_) => Raw::bytes(value),
            },
            mvcc::Key::Unversioned(userkey) => I::value(&userkey, value),
        }
    }
}
