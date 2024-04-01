//! Bincode is used to encode values, both in key/value stores and the toyDB
//! network protocol. It is a Rust-specific encoding that depends on the
//! internal data structures being stable, but it's sufficient for toyDB. See:
//! https://github.com/bincode-org/bincode
//!
//! This module wraps the standard bincode crate to change the default options,
//! in particular to use variable-length rather than fixed-length integers.
//! Confusingly, upstream bincode::(de)serialize uses different options (fixed)
//! than DefaultOptions (variable) -- this module always uses DefaultOptions.

use crate::error::{Error, Result};

use bincode::Options;

/// Returns the default Bincode options, initialized on first use.
fn bincode() -> &'static bincode::DefaultOptions {
    static BINCODE: std::sync::OnceLock<bincode::DefaultOptions> = std::sync::OnceLock::new();
    BINCODE.get_or_init(bincode::DefaultOptions::new)
}

/// Deserializes a value using Bincode.
pub fn deserialize<'de, T: serde::Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    Ok(bincode().deserialize(bytes)?)
}

/// Deserializes a value from a reader using Bincode.
pub fn deserialize_from<R: std::io::Read, T: serde::de::DeserializeOwned>(reader: R) -> Result<T> {
    Ok(bincode().deserialize_from(reader)?)
}

/// Deserializes a value from a reader using Bincode, or returns None if the
/// reader is closed.
pub fn maybe_deserialize_from<R: std::io::Read, T: serde::de::DeserializeOwned>(
    reader: R,
) -> Result<Option<T>> {
    match bincode().deserialize_from(reader) {
        Ok(v) => Ok(Some(v)),
        Err(e) => match *e {
            bincode::ErrorKind::Io(e) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => Ok(None),
                std::io::ErrorKind::ConnectionReset => Ok(None),
                _ => Err(Error::from(e)),
            },
            _ => Err(Error::from(e)),
        },
    }
}

/// Serializes a value using Bincode.
pub fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(bincode().serialize(value)?)
}

/// Serializes a value to a writer using Bincode.
pub fn serialize_into<W: std::io::Write, T: serde::Serialize>(writer: W, value: &T) -> Result<()> {
    Ok(bincode().serialize_into(writer, value)?)
}
