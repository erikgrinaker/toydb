use crate::Error;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::Read;

/// Deserializes a value from a byte buffer, using MessagePack.
/// Returns `Error::Internal` on error.
pub fn deserialize<'de, V: Deserialize<'de>>(bytes: &[u8]) -> Result<V, Error> {
    Ok(Deserialize::deserialize(&mut rmp_serde::Deserializer::new(bytes))?)
}

/// Deserializes the next value from a reader, using MessagePack.
pub fn deserialize_read<R: Read, V: DeserializeOwned>(reader: R) -> Result<Option<V>, Error> {
    match rmp_serde::decode::from_read(reader) {
        Ok(value) => Ok(Some(value)),
        Err(rmp_serde::decode::Error::InvalidMarkerRead(e))
            if e.kind() == std::io::ErrorKind::UnexpectedEof =>
        {
            Ok(None)
        }
        Err(err) => Err(err.into()),
    }
}

/// Serializes a value into a byte buffer, using MessagePack.
/// Returns `Error::Internal` on error.
pub fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    value.serialize(&mut rmp_serde::Serializer::new(&mut bytes))?;
    Ok(bytes)
}
