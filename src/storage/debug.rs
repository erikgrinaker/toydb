//! Storage debug helpers, primarily formatting of raw engine data.

// TODO: consider moving these elsewhere.

use super::mvcc::{self, TransactionState};
use crate::encoding::{bincode, Key as _};

use itertools::Itertools as _;
use std::collections::BTreeSet;

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
        format_set(&state.active)
    )
}

/// Formats a BTreeSet.
pub fn format_set<T: Copy + Ord + std::fmt::Display>(set: &BTreeSet<T>) -> String {
    let elements = set.iter().map(|v| v.to_string()).join(",");
    format!("{{{elements}}}")
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
                    if let Ok(active) = bincode::deserialize::<BTreeSet<mvcc::Version>>(v) {
                        fvalue = Some(format_set(&active));
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
