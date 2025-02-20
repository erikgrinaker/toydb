//! Formats raw keys and values, recursively where necessary. Handles both both
//! Raft, MVCC, SQL, and raw binary data.

use std::collections::BTreeSet;
use std::marker::PhantomData;

use itertools::Itertools as _;
use regex::Regex;

use super::{Key as _, Value as _, bincode};
use crate::raft;
use crate::sql;
use crate::storage::mvcc;

/// Formats encoded keys and values.
pub trait Formatter {
    /// Formats a key.
    fn key(key: &[u8]) -> String;

    /// Formats a value. Also takes the key to determine the kind of value.
    fn value(key: &[u8], value: &[u8]) -> String;

    /// Formats a key/value pair.
    fn key_value(key: &[u8], value: &[u8]) -> String {
        Self::key_maybe_value(key, Some(value))
    }

    /// Formats a key/value pair, where the value may not exist.
    fn key_maybe_value(key: &[u8], value: Option<&[u8]>) -> String {
        let fmtkey = Self::key(key);
        let fmtvalue = value.map_or("None".to_string(), |v| Self::value(key, v));
        format!("{fmtkey} → {fmtvalue}")
    }
}

/// Formats raw byte slices without any decoding.
pub struct Raw;

impl Raw {
    /// Formats raw bytes as escaped ASCII strings.
    pub fn bytes(bytes: &[u8]) -> String {
        let escaped = bytes.iter().copied().flat_map(std::ascii::escape_default).collect_vec();
        format!("\"{}\"", String::from_utf8_lossy(&escaped))
    }
}

impl Formatter for Raw {
    fn key(key: &[u8]) -> String {
        Self::bytes(key)
    }

    fn value(_key: &[u8], value: &[u8]) -> String {
        Self::bytes(value)
    }
}

/// Formats Raft log entries. Dispatches to F to format each Raft command.
pub struct Raft<F: Formatter>(PhantomData<F>);

impl<F: Formatter> Raft<F> {
    /// Formats a Raft entry.
    pub fn entry(entry: &raft::Entry) -> String {
        let fmtcommand = entry.command.as_deref().map_or("None".to_string(), |c| F::value(&[], c));
        format!("{}@{} {fmtcommand}", entry.index, entry.term)
    }
}

impl<F: Formatter> Formatter for Raft<F> {
    fn key(key: &[u8]) -> String {
        let Ok(key) = raft::Key::decode(key) else {
            return Raw::key(key); // invalid key
        };
        format!("raft:{key:?}")
    }

    fn value(key: &[u8], value: &[u8]) -> String {
        let Ok(key) = raft::Key::decode(key) else {
            return Raw::value(key, value); // invalid key
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
                        vote.map_or("None".to_string(), |v| v.to_string()),
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

/// Formats MVCC keys/values. Dispatches to F to format the inner key/value.
pub struct MVCC<F: Formatter>(PhantomData<F>);

impl<F: Formatter> Formatter for MVCC<F> {
    fn key(key: &[u8]) -> String {
        let Ok(key) = mvcc::Key::decode(key) else {
            return Raw::key(key); // invalid key
        };
        match key {
            mvcc::Key::TxnWrite(version, innerkey) => {
                format!("mvcc:TxnWrite({version}, {})", F::key(&innerkey))
            }
            mvcc::Key::Version(innerkey, version) => {
                format!("mvcc:Version({}, {version})", F::key(&innerkey))
            }
            mvcc::Key::Unversioned(innerkey) => {
                format!("mvcc:Unversioned({})", F::key(&innerkey))
            }
            mvcc::Key::NextVersion | mvcc::Key::TxnActive(_) | mvcc::Key::TxnActiveSnapshot(_) => {
                format!("mvcc:{key:?}")
            }
        }
    }

    fn value(key: &[u8], value: &[u8]) -> String {
        let Ok(key) = mvcc::Key::decode(key) else {
            return Raw::bytes(value); // invalid key
        };
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
                format!("{{{}}}", active.iter().join(","))
            }
            mvcc::Key::TxnActive(_) | mvcc::Key::TxnWrite(_, _) => Raw::bytes(value),
            mvcc::Key::Version(userkey, _) => match bincode::deserialize(value) {
                Ok(Some(value)) => F::value(&userkey, value),
                Ok(None) => "None".to_string(),
                Err(_) => Raw::bytes(value),
            },
            mvcc::Key::Unversioned(userkey) => F::value(&userkey, value),
        }
    }
}

/// Formats SQL keys/values.
pub struct SQL;

impl SQL {
    /// Formats a list of SQL values.
    fn values(values: impl IntoIterator<Item = sql::types::Value>) -> String {
        values.into_iter().join(",")
    }

    /// Formats a table schema.
    fn schema(table: sql::types::Table) -> String {
        // Put it all on a single line.
        let re = Regex::new(r#"\n\s*"#).expect("invalid regex");
        re.replace_all(&table.to_string(), " ").into_owned()
    }
}

impl Formatter for SQL {
    fn key(key: &[u8]) -> String {
        // Special-case the Raft applied index key.
        if key == sql::engine::Raft::APPLIED_INDEX_KEY {
            return String::from_utf8_lossy(key).into_owned();
        }
        let Ok(key) = sql::engine::Key::decode(key) else {
            return Raw::key(key); // invalid key
        };
        match key {
            sql::engine::Key::Table(name) => format!("sql:Table({name})"),
            sql::engine::Key::Index(table, column, value) => {
                format!("sql:Index({table}.{column}, {value})")
            }
            sql::engine::Key::Row(table, id) => {
                format!("sql:Row({table}, {id})")
            }
        }
    }

    fn value(key: &[u8], value: &[u8]) -> String {
        // Special-case the applied_index key.
        if key == sql::engine::Raft::APPLIED_INDEX_KEY {
            if let Ok(applied_index) = bincode::deserialize::<raft::Index>(value) {
                return applied_index.to_string();
            }
        }

        let Ok(key) = sql::engine::Key::decode(key) else {
            return Raw::key(value);
        };
        match key {
            sql::engine::Key::Table(_) => {
                let Ok(table) = bincode::deserialize(value) else {
                    return Raw::bytes(value);
                };
                Self::schema(table)
            }
            sql::engine::Key::Row(_, _) => {
                let Ok(row) = bincode::deserialize::<sql::types::Row>(value) else {
                    return Raw::bytes(value);
                };
                Self::values(row)
            }
            sql::engine::Key::Index(_, _, _) => {
                let Ok(index) = bincode::deserialize::<BTreeSet<sql::types::Value>>(value) else {
                    return Raw::bytes(value);
                };
                Self::values(index)
            }
        }
    }
}

/// Formats SQL Raft write commands, from the Raft log.
pub struct SQLCommand;

impl Formatter for SQLCommand {
    fn key(_key: &[u8]) -> String {
        // There is no key, since these are wrapped in a Raft log entry.
        panic!("SQL commands don't have a key");
    }

    fn value(_key: &[u8], value: &[u8]) -> String {
        let Ok(write) = sql::engine::Write::decode(value) else {
            return Raw::bytes(value);
        };

        let txn = match &write {
            sql::engine::Write::Begin => None,
            sql::engine::Write::Commit(txn)
            | sql::engine::Write::Rollback(txn)
            | sql::engine::Write::Delete { txn, .. }
            | sql::engine::Write::Insert { txn, .. }
            | sql::engine::Write::Update { txn, .. }
            | sql::engine::Write::CreateTable { txn, .. }
            | sql::engine::Write::DropTable { txn, .. } => Some(txn),
        };
        let fmttxn =
            txn.filter(|t| !t.read_only).map_or("".to_string(), |t| format!("t{} ", t.version));

        let fmtcommand = match write {
            sql::engine::Write::Begin => "BEGIN".to_string(),
            sql::engine::Write::Commit(_) => "COMMIT".to_string(),
            sql::engine::Write::Rollback(_) => "ROLLBACK".to_string(),
            sql::engine::Write::Delete { table, ids, .. } => {
                format!("DELETE {table} {}", ids.iter().map(|id| id.to_string()).join(","))
            }
            sql::engine::Write::Insert { table, rows, .. } => {
                format!(
                    "INSERT {table} {}",
                    rows.into_iter().map(|row| format!("({})", SQL::values(row))).join(" ")
                )
            }
            sql::engine::Write::Update { table, rows, .. } => format!(
                "UPDATE {table} {}",
                rows.into_iter().map(|(id, row)| format!("{id}→({})", SQL::values(row))).join(" ")
            ),
            sql::engine::Write::CreateTable { schema, .. } => SQL::schema(schema),
            sql::engine::Write::DropTable { table, .. } => format!("DROP TABLE {table}"),
        };
        format!("{fmttxn}{fmtcommand}")
    }
}
