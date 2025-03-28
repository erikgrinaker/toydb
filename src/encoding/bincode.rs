//! Bincode is used to encode values, both in key/value stores and the toyDB
//! network protocol. It is a Rust-specific encoding that depends on the
//! internal data structures being stable, but it's sufficient for toyDB. See:
//! <https://github.com/bincode-org/bincode>
//!
//! This module wraps the [`bincode`] crate and uses the standard config.

use std::io::{Read, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Use the standard Bincode configuration.
const CONFIG: bincode::config::Configuration = bincode::config::standard();

/// Serializes a value using Bincode.
pub fn serialize<T: Serialize>(value: &T) -> Vec<u8> {
    // Panic on failure, as this is a problem with the data structure.
    bincode::serde::encode_to_vec(value, CONFIG).expect("value must be serializable")
}

/// Deserializes a value using Bincode.
pub fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    Ok(bincode::serde::borrow_decode_from_slice(bytes, CONFIG)?.0)
}

/// Serializes a value to a writer using Bincode.
pub fn serialize_into<W: Write, T: Serialize>(mut writer: W, value: &T) -> Result<()> {
    bincode::serde::encode_into_std_write(value, &mut writer, CONFIG)?;
    Ok(())
}

/// Deserializes a value from a reader using Bincode.
pub fn deserialize_from<R: Read, T: DeserializeOwned>(mut reader: R) -> Result<T> {
    Ok(bincode::serde::decode_from_std_read(&mut reader, CONFIG)?)
}

/// Deserializes a value from a reader using Bincode, or returns None if the
/// reader is closed.
pub fn maybe_deserialize_from<R: Read, T: DeserializeOwned>(mut reader: R) -> Result<Option<T>> {
    match bincode::serde::decode_from_std_read(&mut reader, CONFIG) {
        Ok(t) => Ok(Some(t)),
        Err(bincode::error::DecodeError::Io { inner, .. })
            if inner.kind() == std::io::ErrorKind::UnexpectedEof
                || inner.kind() == std::io::ErrorKind::ConnectionReset =>
        {
            Ok(None)
        }
        Err(err) => Err(Error::from(err)),
    }
}
