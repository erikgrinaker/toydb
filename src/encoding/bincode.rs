//! Bincode is used to encode values, both in key/value stores and the toyDB
//! network protocol. It is a Rust-specific encoding that depends on the
//! internal data structures being stable, but it's sufficient for toyDB. See:
//! <https://github.com/bincode-org/bincode>
//!
//! This module wraps the standard [`bincode`] crate to change the default
//! options. In particular, this uses variable-length rather than fixed-length
//! integer encoding. Confusingly, upstream [`bincode::serialize`] uses
//! different options than [`bincode::DefaultOptions`].

use std::io::{Read, Write};
use std::sync::LazyLock;

use bincode::Options as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Use default Bincode options, unlike [`bincode::serialize`] (weirdly).
static BINCODE: LazyLock<bincode::DefaultOptions> = LazyLock::new(bincode::DefaultOptions::new);

/// Serializes a value using Bincode.
pub fn serialize<T: Serialize>(value: &T) -> Vec<u8> {
    // Panic on failure, as this is a problem with the data structure.
    BINCODE.serialize(value).expect("value must be serializable")
}

/// Deserializes a value using Bincode.
pub fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    Ok(BINCODE.deserialize(bytes)?)
}

/// Serializes a value to a writer using Bincode.
pub fn serialize_into<W: Write, T: Serialize>(writer: W, value: &T) -> Result<()> {
    Ok(BINCODE.serialize_into(writer, value)?)
}

/// Deserializes a value from a reader using Bincode.
pub fn deserialize_from<R: Read, T: DeserializeOwned>(reader: R) -> Result<T> {
    Ok(BINCODE.deserialize_from(reader)?)
}

/// Deserializes a value from a reader using Bincode, or returns None if the
/// reader is closed.
pub fn maybe_deserialize_from<R: Read, T: DeserializeOwned>(reader: R) -> Result<Option<T>> {
    match BINCODE.deserialize_from(reader) {
        Ok(t) => Ok(Some(t)),
        Err(err) => match *err {
            bincode::ErrorKind::Io(err) => match err.kind() {
                std::io::ErrorKind::UnexpectedEof => Ok(None),
                std::io::ErrorKind::ConnectionReset => Ok(None),
                _ => Err(Error::from(err)),
            },
            _ => Err(Error::from(err)),
        },
    }
}
