use crate::Error;

/// Deserializes a value from a byte buffer, using MessagePack.
pub fn deserialize<'de, V: serde::Deserialize<'de>>(bytes: &[u8]) -> Result<V, Error> {
    Ok(serde::Deserialize::deserialize(&mut rmps::Deserializer::new(bytes))?)
}

/// Serializes a value into a byte buffer, using MessagePack.
pub fn serialize<V: serde::Serialize>(value: &V) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    value.serialize(&mut rmps::Serializer::new(&mut bytes))?;
    Ok(bytes)
}
