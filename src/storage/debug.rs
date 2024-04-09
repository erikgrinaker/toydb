//! Storage debug helpers, primarily formatting of raw engine data.

use std::collections::HashSet;

use super::engine::{self, ScanIterator, Status};
use super::mvcc::{self, TransactionState};
use crate::encoding::bincode;
use crate::error::Result;

/// Formats a raw byte string, either as a UTF-8 string (if valid and
/// printable), otherwise hex-encoded.
pub fn format_raw(v: &[u8]) -> String {
    if v.is_empty() {
        return String::from("[]");
    }
    if let Ok(s) = String::from_utf8(v.to_vec()) {
        if s.chars().all(|c| !c.is_control()) {
            return format!(r#""{}""#, s);
        }
    }
    format!("0x{}", hex::encode(v))
}

/// Formats a transaction state.
pub fn format_txn(state: &TransactionState) -> String {
    format!(
        "v{} {} active={}",
        state.version,
        if state.read_only { "read-only" } else { "read-write" },
        format_hashset(&state.active)
    )
}

/// Formats a HashSet with sorted elements.
pub fn format_hashset<T: Copy + Ord + std::fmt::Display>(set: &HashSet<T>) -> String {
    let mut elements: Vec<T> = set.iter().copied().collect();
    elements.sort();
    let elements: Vec<String> = elements.into_iter().map(|v| v.to_string()).collect();
    format!("{{{}}}", elements.join(","))
}

/// Formats a raw engine key/value pair, or just the key if the value is None.
/// Attempts to decode known MVCC key formats and values.
///
/// TODO: decode Raft and SQL keys/values too.
pub fn format_key_value(key: &[u8], value: &Option<Vec<u8>>) -> (String, Option<String>) {
    // Default to string/hex formatting of the raw key and value.
    let mut fkey = format_raw(key);
    let mut fvalue = value.as_ref().map(|v| format_raw(v.as_slice()));

    // Try to decode MVCC keys and values.
    if let Ok(key) = mvcc::Key::decode(key) {
        // Use the debug formatting of the key, unless we need more.
        fkey = format!("{:?}", key);

        match key {
            mvcc::Key::NextVersion => {
                if let Some(ref v) = value {
                    if let Ok(v) = bincode::deserialize::<u64>(v) {
                        fvalue = Some(format!("{}", v))
                    }
                }
            }
            mvcc::Key::TxnActive(_) => {}
            mvcc::Key::TxnActiveSnapshot(_) => {
                if let Some(ref v) = value {
                    if let Ok(active) = bincode::deserialize::<HashSet<u64>>(v) {
                        fvalue = Some(format_hashset(&active));
                    }
                }
            }
            mvcc::Key::TxnWrite(version, userkey) => {
                fkey = format!("TxnWrite({}, {})", version, format_raw(&userkey))
            }
            mvcc::Key::Version(userkey, version) => {
                fkey = format!("Version({}, {})", format_raw(&userkey), version);
                if let Some(ref v) = value {
                    match bincode::deserialize(v) {
                        Ok(Some(v)) => fvalue = Some(format_raw(v)),
                        Ok(None) => fvalue = Some(String::from("None")),
                        Err(_) => {}
                    }
                }
            }
            mvcc::Key::Unversioned(userkey) => {
                fkey = format!("Unversioned({})", format_raw(&userkey));
            }
        }
    }

    (fkey, fvalue)
}

/// A debug storage engine, which wraps another engine and logs mutations.
pub struct Engine<E: engine::Engine> {
    /// The wrapped engine.
    inner: E,
    /// Write log as key/value tuples. Value is None for deletes.
    write_log: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl<E: engine::Engine> std::fmt::Display for Engine<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "debug:{}", self.inner)
    }
}

impl<E: engine::Engine> Engine<E> {
    pub fn new(inner: E) -> Self {
        Self { inner, write_log: Vec::new() }
    }

    /// Returns and resets the write log. The next call only returns new writes.
    pub fn take_write_log(&mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let mut write_log = Vec::new();
        std::mem::swap(&mut write_log, &mut self.write_log);
        write_log
    }
}

impl<E: engine::Engine> engine::Engine for Engine<E> {
    type ScanIterator<'a> = E::ScanIterator<'a> where E: 'a;

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)?;
        self.write_log.push((key.to_vec(), None));
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_> {
        self.inner.scan(range)
    }

    fn scan_dyn(
        &mut self,
        range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
    ) -> Box<dyn ScanIterator + '_> {
        Box::new(self.scan(range))
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.inner.set(key, value.clone())?;
        self.write_log.push((key.to_vec(), Some(value)));
        Ok(())
    }

    fn status(&mut self) -> Result<Status> {
        self.inner.status()
    }
}
