use crate::Error;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::Read;

/// Deserializes a value from a byte buffer, using MessagePack.
/// Returns `Error::Internal` on error.
pub fn deserialize<'de, V: Deserialize<'de>>(bytes: &[u8]) -> Result<V, Error> {
    Ok(Deserialize::deserialize(&mut rmps::Deserializer::new(bytes))?)
}

/// Deserializes the next value from a reader, using MessagePack.
pub fn deserialize_read<R: Read, V: DeserializeOwned>(reader: R) -> Result<Option<V>, Error> {
    match rmps::decode::from_read(reader) {
        Ok(value) => Ok(Some(value)),
        Err(rmps::decode::Error::InvalidMarkerRead(e))
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
    value.serialize(&mut rmps::Serializer::new(&mut bytes))?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() -> Result<(), Error> {
        assert_eq!(7, deserialize::<u64>(&[0x07])?);
        Ok(())
    }

    #[test]
    fn test_deserialize_error() -> Result<(), Error> {
        assert_matches!(deserialize::<u64>(&[]), Err(Error::Internal(_)));
        Ok(())
    }

    #[test]
    fn test_deserialize_serialize() -> Result<(), Error> {
        assert_eq!(7, deserialize::<u64>(&serialize(&7)?)?);
        Ok(())
    }

    #[test]
    fn test_serialize() -> Result<(), Error> {
        assert_eq!([0x07].to_vec(), serialize::<u64>(&7)?);
        Ok(())
    }
}
