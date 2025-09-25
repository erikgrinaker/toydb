# Key/Value Encoding

The key/value store uses binary `Vec<u8>` keys and values, so we need an encoding scheme to 
translate between in-memory Rust data structures and the on-disk binary data. This is provided by
the [`encoding`](https://github.com/erikgrinaker/toydb/tree/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/encoding)
module, with separate schemes for key and value encoding.

## `Bincode` Value Encoding

Values are encoded using [Bincode](https://github.com/bincode-org/bincode), a third-party binary
encoding scheme for Rust. Bincode is convenient because it can easily encode any arbitrary Rust
data type. But we could also have chosen e.g. [JSON](https://en.wikipedia.org/wiki/JSON),
[Protobuf](https://protobuf.dev), [MessagePack](https://msgpack.org/), or any other encoding.

We won't dwell on the actual binary format here, see the [Bincode specification](https://git.sr.ht/~stygianentity/bincode/tree/trunk/item/docs/spec.md)
for details.

To use a consistent configuration for all encoding and decoding, we provide helper functions in
the [`encoding::bincode`](https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/encoding/bincode.rs)
module which use `bincode::config::standard()`.

https://github.com/erikgrinaker/toydb/blob/0ce1fb34349fda043cb9905135f103bceb4395b4/src/encoding/bincode.rs#L15-L27

Bincode uses the very common [Serde](https://serde.rs) framework for its API. toyDB also provides an
`encoding::Value` helper trait for value types which adds automatic `encode()` and `decode()`
methods:

https://github.com/erikgrinaker/toydb/blob/b57ae6502e93ea06df00d94946a7304b7d60b977/src/encoding/mod.rs#L39-L68

Here's an example of how this can be used to encode and decode an arbitrary `Dog` data type:

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct Dog {
    name: String,
    age: u8,
    good_boy: bool,
}

impl encoding::Value for Dog {}

let pluto = Dog { name: "Pluto".into(), age: 4, good_boy: true };
let bytes = pluto.encode();
println!("{bytes:02x?}");

// Outputs [05, 50, 6c, 75, 74, 6f, 04, 01]:
//
// * Length of string "Pluto": 05.
// * String "Pluto": 50 6c 75 74 6f.
// * Age 4: 04.
// * Good boy: 01 (true).

let pluto = Dog::decode(&bytes)?; // gives us back Pluto
```

## `Keycode` Key Encoding

Unlike values, keys can't just use any binary encoding like Bincode. As mentioned in the storage
section, the storage engine sorts data by key to enable range scans. The key encoding must therefore
preserve the [lexicographical order](https://en.wikipedia.org/wiki/Lexicographic_order) of the
encoded values: the binary byte slices must sort in the same order as the original values.

As an example of why we can't just use Bincode, consider the strings "house" and "key". These should
be sorted in alphabetical order: "house" before "key". However, Bincode encodes strings prefixed by
their length, so "key" would be sorted before "house" in binary form:

```
03 6b 65 79        ← 3 bytes: key
05 68 6f 75 73 65  ← 5 bytes: house
```

For similar reasons, we can't just encode numbers in their native binary form: the
[little-endian](https://en.wikipedia.org/wiki/Endianness) representation will order very large
numbers before small numbers, and the [sign bit](https://en.wikipedia.org/wiki/Sign_bit) will order
positive numbers before negative numbers. This would violate the ordering of natural numbers.

We also have to be careful with value sequences, which should be ordered element-wise. For example,
the pair ("a", "xyz") should be ordered before ("ab", "cd"), so we can't just encode the strings
one after the other like "axyz" and "abcd" since that would sort ("ab", "cd") first.

toyDB provides an order-preserving encoding called "Keycode" in the [`encoding::keycode`](https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/encoding/keycode.rs)
module. Like Bincode, the Keycode encoding is not self-describing: the binary data does not say what
the data type is, the caller must provide a type to decode into. It only supports a handful of
primitive data types, and only needs to order values of the same type.

Keycode is implemented as a [Serde](https://serde.rs) (de)serializer, which requires a lot of
boilerplate code to satisfy the trait, but we'll just focus on the actual encoding. The encoding
scheme is as follows:

* `bool`: `00` for `false` and `01` for `true`.

    https://github.com/erikgrinaker/toydb/blob/2027641004989355c2162bbd9eeefcc991d6b29b/src/encoding/keycode.rs#L113-L117

* `u64`: the [big-endian](https://en.wikipedia.org/wiki/Endianness) binary encoding.

    https://github.com/erikgrinaker/toydb/blob/2027641004989355c2162bbd9eeefcc991d6b29b/src/encoding/keycode.rs#L157-L161

* `i64`: the [big-endian](https://en.wikipedia.org/wiki/Endianness) binary encoding, but with the
   sign bit flipped to order negative numbers before positive ones.

    https://github.com/erikgrinaker/toydb/blob/2027641004989355c2162bbd9eeefcc991d6b29b/src/encoding/keycode.rs#L131-L143

* `f64`: the [big-endian IEEE 754](https://en.wikipedia.org/wiki/Double-precision_floating-point_format)
  binary encoding, but with the sign bit flipped, and all bits flipped for negative numbers, to
  order negative numbers correctly.

    https://github.com/erikgrinaker/toydb/blob/2027641004989355c2162bbd9eeefcc991d6b29b/src/encoding/keycode.rs#L167-L179

* `Vec<u8>`: terminated by `00 00`, with `00` escaped as `00 ff` to disambiguate it.

    https://github.com/erikgrinaker/toydb/blob/2027641004989355c2162bbd9eeefcc991d6b29b/src/encoding/keycode.rs#L190-L205

* `String`: like `Vec<u8>`.

    https://github.com/erikgrinaker/toydb/blob/2027641004989355c2162bbd9eeefcc991d6b29b/src/encoding/keycode.rs#L185-L188

* `Vec<T>`, `[T]`, `(T,)`: the concatenation of the inner values.

    https://github.com/erikgrinaker/toydb/blob/2027641004989355c2162bbd9eeefcc991d6b29b/src/encoding/keycode.rs#L295-L307

* `enum`: the variant's numerical index as a `u8`, then the inner values (if any).

    https://github.com/erikgrinaker/toydb/blob/2027641004989355c2162bbd9eeefcc991d6b29b/src/encoding/keycode.rs#L223-L227

Like `encoding::Value`, there is also an `encoding::Key` helper trait:

https://github.com/erikgrinaker/toydb/blob/b57ae6502e93ea06df00d94946a7304b7d60b977/src/encoding/mod.rs#L20-L37

Different kinds of keys are usually represented as enums. For example, if we wanted to store cars
and video games, we could use:

```rust
#[derive(serde::Serialize, serde::Deserialize)]
enum Key {
    Car(String, String, u64),    // make, model, year
    Game(String, u64, Platform), // name, year, platform
}

#[derive(serde::Serialize, serde::Deserialize)]
enum Platform {
    PC,
    PS5,
    Switch,
    Xbox,
}

impl encoding::Key for Key {}

let returnal = Key::Game("Returnal".into(), 2021, Platform::PS5);
let bytes = returnal.encode();
println!("{bytes:02x?}");

// Outputs [01, 52, 65, 74, 75, 72, 6e, 61, 6c, 00, 00, 00, 00, 00, 00, 00, 00, 07, e5, 01].
//
// * Key::Game: 01
// * Returnal: 52 65 74 75 72 6e 61 6c 00 00
// * 2021: 00 00 00 00 00 00 07 e5
// * Platform::PS5: 01

let returnal = Key::decode(&bytes)?;
```

Because the keys are sorted in element-wise order, this would allow us to e.g. perform a prefix
scan to fetch all platforms which Returnal (2021) was released on, or perform a range scan to fetch 
all models of Nissan Altima released between 2010 and 2015.

---

<p align="center">
← <a href="storage.md">Storage Engine</a> &nbsp; | &nbsp; <a href="mvcc.md">MVCC Transactions</a> →
</p>