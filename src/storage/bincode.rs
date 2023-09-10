//! Bincode is used to encode values. For details, see:
//! https://github.com/bincode-org/bincode
//!
//! By default, the bincode::(de)serialize functions use fixed-length integer
//! encoding, despite DefaultOptions using variable-length encoding. This module
//! provides simple wrappers for these functions that use variable-length
//! encoding and the other defaults.

use crate::error::Result;

use bincode::Options;
use lazy_static::lazy_static;

lazy_static! {
    /// Create a static binding for the default Bincode options.
    static ref BINCODE: bincode::DefaultOptions = bincode::DefaultOptions::new();
}

/// Deserializes a value using Bincode.
pub fn deserialize<'de, T: serde::Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    Ok(BINCODE.deserialize(bytes)?)
}

/// Serializes a value using Bincode.
pub fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(BINCODE.serialize(value)?)
}
