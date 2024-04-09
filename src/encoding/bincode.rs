//! Bincode is used to encode values, both in key/value stores and the toyDB
//! network protocol. It is a Rust-specific encoding that depends on the
//! internal data structures being stable, but it's sufficient for toyDB. See:
//! https://github.com/bincode-org/bincode
//!
//! This module wraps the standard bincode crate to change the default options,
//! in particular to use variable-length rather than fixed-length integers.
//! Confusingly, upstream bincode::(de)serialize uses different options (fixed)
//! than DefaultOptions (variable) -- this module always uses DefaultOptions.

use crate::error::Result;

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

/// Serializes a value using Bincode.
pub fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(bincode().serialize(value)?)
}
