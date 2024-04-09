//! Bincode is used to encode values, both in key/value stores and the toyDB
//! network protocol. It is a Rust-specific encoding that depends on the
//! internal data structures being stable, but it's sufficient for toyDB. See:
//! https://github.com/bincode-org/bincode
//!
//! This module wraps the standard bincode crate to change the default settings.
//! In particular, to use variable-length integers rather than fixed-length.

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
