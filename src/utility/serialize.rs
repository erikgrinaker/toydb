use crate::Error;

/// Deserializes a value from a byte buffer, using MessagePack.
/// Returns `Error::IO` on error.
pub fn deserialize<'de, V: serde::Deserialize<'de>>(bytes: &[u8]) -> Result<V, Error> {
    Ok(serde::Deserialize::deserialize(&mut rmps::Deserializer::new(bytes))?)
}

/// Serializes a value into a byte buffer, using MessagePack.
/// Returns `Error::IO` on error.
pub fn serialize<V: serde::Serialize>(value: &V) -> Result<Vec<u8>, Error> {
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
        assert_matches!(deserialize::<u64>(&[]), Err(Error::IO(_)));
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
