//! KeyCode is a lexicographical order-preserving binary encoding for use with
//! keys. It is designed for simplicity, not efficiency (i.e. it does not use
//! varints or other compression methods).
//!
//! Ordering is important because it allows limited scans across specific parts
//! of the keyspace, e.g. scanning an individual table or using an index range
//! predicate like WHERE id < 100. It also avoids sorting in some cases where
//! the keys are already in the desired order, e.g. in the Raft log.
//!
//! The encoding is not self-describing: the caller must provide a concrete type
//! to decode into, and the binary key must conform to its structure.
//!
//! KeyCode supports a subset of primitive data types, encoded as follows:
//!
//! bool:    0x00 for false, 0x01 for true.
//! u64:     Big-endian binary representation.
//! i64:     Big-endian binary representation, with sign bit flipped.
//! f64:     Big-endian binary representation, with sign bit flipped, and rest if negative.
//! Vec<u8>: 0x00 is escaped as 0x00ff, terminated with 0x0000.
//! String:  Like Vec<u8>.
//!
//! Additionally, several container types are supported:
//!
//! Tuple:  Concatenation of elements, with no surrounding structure.
//! Array:  Like tuple.
//! Vec:    Like tuple.
//! Enum:   The variant's enum index as a single u8 byte.
//!
//! SQL Value enums are encoded according to the above scheme, i.e. a single
//! byte identifying the enum variant by index, then the primitive value.
//!
//! The canonical key reprentation is an enum -- for example:
//!
//! ```
//! #[derive(Debug, Deserialize, Serialize)]
//! enum Key {
//!     Foo,
//!     Bar(String),
//!     Baz(bool, u64, #[serde(with = "serde_bytes")] Vec<u8>),
//! }
//! ```
//!
//! Unfortunately, byte vectors and slices such as Vec<u8> must be wrapped with
//! serde_bytes::ByteBuf or use the #[serde(with="serde_bytes")] attribute. See
//! https://github.com/serde-rs/bytes

use de::IntoDeserializer;
use serde::{de, ser};

use crate::error::{Error, Result};

// Serializes a key to a binary KeyCode representation.
pub fn serialize<T: serde::Serialize>(key: &T) -> Result<Vec<u8>> {
    let mut serializer = Serializer { output: Vec::new() };
    key.serialize(&mut serializer)?;
    Ok(serializer.output)
}

// Deserializes a key from a binary KeyCode representation.
pub fn deserialize<'a, T: serde::Deserialize<'a>>(input: &'a [u8]) -> Result<T> {
    let mut deserializer = Deserializer::from_bytes(input);
    let t = T::deserialize(&mut deserializer)?;
    if !deserializer.input.is_empty() {
        return Err(Error::Internal(format!(
            "Unexpected trailing bytes {:x?} at end of key {:x?}",
            deserializer.input, input
        )));
    }
    Ok(t)
}

// Serializes keys as binary byte vectors.
struct Serializer {
    output: Vec<u8>,
}

impl<'a> serde::Serializer for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleVariant = Self;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    /// bool simply uses 1 for true and 0 for false.
    fn serialize_bool(self, v: bool) -> Result<()> {
        self.output.push(if v { 1 } else { 0 });
        Ok(())
    }

    fn serialize_i8(self, _: i8) -> Result<()> {
        unimplemented!()
    }

    fn serialize_i16(self, _: i16) -> Result<()> {
        unimplemented!()
    }

    fn serialize_i32(self, _: i32) -> Result<()> {
        unimplemented!()
    }

    /// i64 uses the big-endian two's completement encoding, but flips the
    /// left-most sign bit such that negative numbers are ordered before
    /// positive numbers.
    ///
    /// The relative ordering of the remaining bits is already correct: -1, the
    /// largest negative integer, is encoded as 01111111...11111111, ordered
    /// after all other negative integers but before positive integers.
    fn serialize_i64(self, v: i64) -> Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_u8(self, _: u8) -> Result<()> {
        unimplemented!()
    }

    fn serialize_u16(self, _: u16) -> Result<()> {
        unimplemented!()
    }

    fn serialize_u32(self, _: u32) -> Result<()> {
        unimplemented!()
    }

    /// u64 simply uses the big-endian encoding.
    fn serialize_u64(self, v: u64) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, _: f32) -> Result<()> {
        unimplemented!()
    }

    /// f64 is encoded in big-endian form, but it flips the sign bit to order
    /// positive numbers after negative numbers, and also flips all other bits
    /// for negative numbers to order them from smallest to greatest. NaN is
    /// ordered at the end.
    fn serialize_f64(self, v: f64) -> Result<()> {
        let mut bytes = v.to_be_bytes();
        if bytes[0] & 1 << 7 == 0 {
            bytes[0] ^= 1 << 7; // positive, flip sign bit
        } else {
            bytes.iter_mut().for_each(|b| *b = !*b); // negative, flip all bits
        }
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_char(self, _: char) -> Result<()> {
        unimplemented!()
    }

    // Strings are encoded like bytes.
    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_bytes(v.as_bytes())
    }

    // Byte slices are terminated by 0x0000, escaping 0x00 as 0x00ff.
    // Prefix-length encoding can't be used, since it violates ordering.
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.output.extend(
            v.iter()
                .flat_map(|b| match b {
                    0x00 => vec![0x00, 0xff],
                    b => vec![*b],
                })
                .chain([0x00, 0x00]),
        );
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        unimplemented!()
    }

    fn serialize_some<T: serde::Serialize + ?Sized>(self, _: &T) -> Result<()> {
        unimplemented!()
    }

    fn serialize_unit(self) -> Result<()> {
        unimplemented!()
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<()> {
        unimplemented!()
    }

    // Enum variants are serialized using their index, as a single byte.
    fn serialize_unit_variant(self, _: &'static str, index: u32, _: &'static str) -> Result<()> {
        self.output.push(u8::try_from(index)?);
        Ok(())
    }

    fn serialize_newtype_struct<T: serde::Serialize + ?Sized>(
        self,
        _: &'static str,
        _: &T,
    ) -> Result<()> {
        unimplemented!()
    }

    // Newtype variants are serialized using the variant index and inner type.
    fn serialize_newtype_variant<T: serde::Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()> {
        self.serialize_unit_variant(name, index, variant)?;
        value.serialize(self)
    }

    // Sequences are serialized as the concatenation of the serialized elements.
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }

    // Tuples are serialized as the concatenation of the serialized elements.
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        unimplemented!()
    }

    // Tuple variants are serialized using the variant index and the
    // concatenation of the serialized elements.
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        unimplemented!()
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        unimplemented!()
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        unimplemented!()
    }
}

// Sequences simply concatenate the serialized elements, with no external structure.
impl<'a> ser::SerializeSeq for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: serde::Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// Tuples, like sequences, simply concatenate the serialized elements.
impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: serde::Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// Tuples, like sequences, simply concatenate the serialized elements.
impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: serde::Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// Deserializes keys from byte slices into a given type. The format is not
// self-describing, so the caller must provide a concrete type to deserialize
// into.
pub struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    // Creates a deserializer for a byte slice.
    pub fn from_bytes(input: &'de [u8]) -> Self {
        Deserializer { input }
    }

    // Chops off and returns the next len bytes of the byte slice, or errors if
    // there aren't enough bytes left.
    fn take_bytes(&mut self, len: usize) -> Result<&[u8]> {
        if self.input.len() < len {
            return Err(Error::Internal(format!(
                "Insufficient bytes, expected {} bytes for {:x?}",
                len, self.input
            )));
        }
        let bytes = &self.input[..len];
        self.input = &self.input[len..];
        Ok(bytes)
    }

    // Decodes and chops off the next encoded byte slice.
    fn decode_next_bytes(&mut self) -> Result<Vec<u8>> {
        // We can't easily share state between Iterator.scan() and
        // Iterator.filter() when processing escape sequences, so use a
        // straightforward loop.
        let mut decoded = Vec::new();
        let mut iter = self.input.iter().enumerate();
        let taken = loop {
            match iter.next() {
                Some((_, 0x00)) => match iter.next() {
                    Some((i, 0x00)) => break i + 1,        // terminator
                    Some((_, 0xff)) => decoded.push(0x00), // escaped 0x00
                    _ => return Err(Error::Value("Invalid escape sequence".to_string())),
                },
                Some((_, b)) => decoded.push(*b),
                None => return Err(Error::Value("Unexpected end of input".to_string())),
            }
        };
        self.input = &self.input[taken..];
        Ok(decoded)
    }
}

// For details on serialization formats, see Serializer.
impl<'de, 'a> serde::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        Err(Error::Internal("Must provide type, KeyCode is not self-describing".to_string()))
    }

    fn deserialize_bool<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(match self.take_bytes(1)?[0] {
            0x00 => false,
            0x01 => true,
            b => return Err(Error::Internal(format!("Invalid boolean value {:?}", b))),
        })
    }

    fn deserialize_i8<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_i16<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_i32<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_i64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let mut bytes = self.take_bytes(8)?.to_vec();
        bytes[0] ^= 1 << 7; // flip sign bit
        visitor.visit_i64(i64::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_u8<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u16<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u32<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(u64::from_be_bytes(self.take_bytes(8)?.try_into()?))
    }

    fn deserialize_f32<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_f64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let mut bytes = self.take_bytes(8)?.to_vec();
        if bytes[0] >> 7 & 1 == 1 {
            bytes[0] ^= 1 << 7; // positive, flip sign bit
        } else {
            bytes.iter_mut().for_each(|b| *b = !*b); // negative, flip all bits
        }
        visitor.visit_f64(f64::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_char<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_str<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_str(&String::from_utf8(bytes)?)
    }

    fn deserialize_string<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_string(String::from_utf8(bytes)?)
    }

    fn deserialize_bytes<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_bytes(&bytes)
    }

    fn deserialize_byte_buf<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_byte_buf(bytes)
    }

    fn deserialize_option<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_unit<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_unit_struct<V: de::Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V: de::Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_seq<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V: de::Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V: de::Visitor<'de>>(
        self,
        _: &'static str,
        _: usize,
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_map<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_struct<V: de::Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_enum<V: de::Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_ignored_any<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }
}

// Sequences are simply deserialized until the byte slice is exhausted.
impl<'de> de::SeqAccess<'de> for Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T: de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        if self.input.is_empty() {
            return Ok(None);
        }
        seed.deserialize(self).map(Some)
    }
}

// Enum variants are deserialized by their index.
impl<'de> de::EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V: de::DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant)> {
        let index = self.take_bytes(1)?[0] as u32;
        let value: Result<_> = seed.deserialize(index.into_deserializer());
        Ok((value?, self))
    }
}

// Enum variant contents are deserialized as sequences.
impl<'de> de::VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: de::DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(&mut *self)
    }

    fn tuple_variant<V: de::Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn struct_variant<V: de::Visitor<'de>>(
        self,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::Value;
    use hex;
    use paste::paste;
    use serde::{Deserialize, Serialize};
    use serde_bytes::ByteBuf;
    use std::borrow::Cow;
    use std::f64::consts::PI;

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    enum Key<'a> {
        Unit,
        NewType(String),
        Tuple(bool, #[serde(with = "serde_bytes")] Vec<u8>, u64),
        Cow(
            #[serde(with = "serde_bytes")]
            #[serde(borrow)]
            Cow<'a, [u8]>,
            bool,
            #[serde(borrow)] Cow<'a, str>,
        ),
    }

    /// Assert that serializing a value yields the expected byte sequence (as a
    /// hex-encoded string), and that deserializing it yields the original value.
    macro_rules! test_serialize_deserialize {
        ( $( $name:ident: $input:expr => $expect:literal, )* ) => {
        $(
            #[test]
            fn $name() -> Result<()> {
                let mut input = $input;
                let expect = $expect;
                let output = serialize(&input)?;
                assert_eq!(hex::encode(&output), expect, "encode failed");

                let expect = input;
                input = deserialize(&output)?; // reuse input variable for proper type
                assert_eq!(input, expect, "decode failed");
                Ok(())
            }
        )*
        };
    }

    /// Assert that deserializing invalid inputs results in errors. Takes byte
    /// slices (as hex-encoded strings) and the type to deserialize into.
    macro_rules! test_deserialize_error {
        ( $( $name:ident: $input:literal as $type:ty, )* ) => {
        paste! {
        $(
            #[test]
            #[should_panic]
            fn [< $name _deserialize_error >]() {
                let bytes = hex::decode($input).unwrap();
                deserialize::<$type>(&bytes).unwrap();
            }
        )*
        }
        };
    }

    // Assert that serializing a value results in an error.
    macro_rules! test_serialize_error {
        ( $( $name:ident: $input:expr, )* ) => {
        paste! {
        $(
            #[test]
            #[should_panic]
            fn [< $name _serialize_error >]() {
                let input = $input;
                serialize(&input).unwrap();
            }
        )*
        }
        };
    }

    test_serialize_deserialize! {
        bool_false: false => "00",
        bool_true: true => "01",

        f64_min: f64::MIN => "0010000000000000",
        f64_neg_inf: f64::NEG_INFINITY => "000fffffffffffff",
        f64_neg_pi: -PI => "3ff6de04abbbd2e7",
        f64_neg_zero: -0f64 => "7fffffffffffffff",
        f64_zero: 0f64 => "8000000000000000",
        f64_pi: PI => "c00921fb54442d18",
        f64_max: f64::MAX => "ffefffffffffffff",
        f64_inf: f64::INFINITY => "fff0000000000000",
        // We don't test NAN here, since NAN != NAN.

        i64_min: i64::MIN => "0000000000000000",
        i64_neg_65535: -65535i64 => "7fffffffffff0001",
        i64_neg_1: -1i64 => "7fffffffffffffff",
        i64_0: 0i64 => "8000000000000000",
        i64_1: 1i64 => "8000000000000001",
        i64_65535: 65535i64 => "800000000000ffff",
        i64_max: i64::MAX => "ffffffffffffffff",

        u64_min: u64::MIN => "0000000000000000",
        u64_1: 1_u64 => "0000000000000001",
        u64_65535: 65535_u64 => "000000000000ffff",
        u64_max: u64::MAX => "ffffffffffffffff",

        bytes: ByteBuf::from(vec![0x01, 0xff]) => "01ff0000",
        bytes_empty: ByteBuf::new() => "0000",
        bytes_escape: ByteBuf::from(vec![0x00, 0x01, 0x02]) => "00ff01020000",

        string: "foo".to_string() => "666f6f0000",
        string_empty: "".to_string() => "0000",
        string_escape: "foo\x00bar".to_string() => "666f6f00ff6261720000",
        string_utf8: "👋".to_string() => "f09f918b0000",

        tuple: (true, u64::MAX, ByteBuf::from(vec![0x00, 0x01])) => "01ffffffffffffffff00ff010000",
        array_bool: [false, true, false] => "000100",
        vec_bool: vec![false, true, false] => "000100",
        vec_u64: vec![u64::MIN, u64::MAX, 65535_u64] => "0000000000000000ffffffffffffffff000000000000ffff",

        enum_unit: Key::Unit => "00",
        enum_newtype: Key::NewType("foo".to_string()) => "01666f6f0000",
        enum_tuple: Key::Tuple(false, vec![0x00, 0x01], u64::MAX) => "020000ff010000ffffffffffffffff",
        enum_cow: Key::Cow(vec![0x00, 0x01].into(), false, String::from("foo").into()) => "0300ff01000000666f6f0000",
        enum_cow_borrow: Key::Cow([0x00, 0x01].as_slice().into(), false, "foo".into()) => "0300ff01000000666f6f0000",

        value_null: Value::Null => "00",
        value_bool: Value::Boolean(true) => "0101",
        value_int: Value::Integer(-1) => "027fffffffffffffff",
        value_float: Value::Float(PI) => "03c00921fb54442d18",
        value_string: Value::String("foo".to_string()) => "04666f6f0000",
    }

    test_serialize_error! {
        char: 'a',
        f32: 0f32,
        i8: 0i8,
        i16: 0i16,
        i32: 0i32,
        i128: 0i128,
        u8: 0u8,
        u16: 0u16,
        u32: 0u32,
        u128: 0u128,
        some: Some(true),
        none: Option::<bool>::None,
        vec_u8: vec![0u8],
    }

    test_deserialize_error! {
        bool_empty: "" as bool,
        bool_2: "02" as bool,
        char: "61" as char,
        f32: "00000000" as f32,
        i8: "00" as i8,
        i16: "0000" as i16,
        i32: "00000000" as i32,
        i128: "00000000000000000000000000000000" as i128,
        u16: "0000" as u16,
        u32: "00000000" as u32,
        u64_partial: "0000" as u64,
        u128: "00000000000000000000000000000000" as u128,
        option: "00" as Option::<bool>,
        string_utf8_invalid: "c0" as String,
        tuple_partial: "0001" as (bool, bool, bool),
        vec_u8: "0000" as Vec<u8>,
    }
}
