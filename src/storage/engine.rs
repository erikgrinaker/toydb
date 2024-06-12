use crate::error::Result;

use serde::{Deserialize, Serialize};

/// A key/value storage engine, where both keys and values are arbitrary byte
/// strings between 0 B and 2 GB, stored in lexicographical key order. Writes
/// are only guaranteed durable after calling flush().
///
/// Only supports single-threaded use since all methods (including reads) take a
/// mutable reference -- serialized access can't be avoided anyway, since both
/// Raft execution and file access is serial.
pub trait Engine: Send {
    /// The iterator returned by scan().
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: Sized + 'a; // omit in trait objects, for object safety

    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Flushes any buffered data to the underlying storage medium.
    fn flush(&mut self) -> Result<()>;

    /// Gets a value for a key, if it exists.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
    where
        Self: Sized; // omit in trait objects, for object safety

    /// Like scan, but can be used from trait objects. The iterator will use
    /// dynamic dispatch, which has a minor performance penalty.
    fn scan_dyn(
        &mut self,
        range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
    ) -> Box<dyn ScanIterator + '_>;

    /// Iterates over all key/value pairs starting with prefix.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_>
    where
        Self: Sized, // omit in trait objects, for object safety
    {
        let start = std::ops::Bound::Included(prefix.to_vec());
        let end = match prefix.iter().rposition(|b| *b != 0xff) {
            Some(i) => std::ops::Bound::Excluded(
                prefix.iter().take(i).copied().chain(std::iter::once(prefix[i] + 1)).collect(),
            ),
            None => std::ops::Bound::Unbounded,
        };
        self.scan((start, end))
    }

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Returns engine status.
    fn status(&mut self) -> Result<Status>;
}

/// A scan iterator, with a blanket implementation (in lieu of trait aliases).
pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

impl<I: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>>> ScanIterator for I {}

/// Engine status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// The name of the storage engine.
    pub name: String,
    /// The number of live keys in the engine.
    pub keys: u64,
    /// The logical size of live key/value pairs.
    pub size: u64,
    /// The on-disk size of all data, live and garbage.
    pub total_disk_size: u64,
    /// The on-disk size of live data.
    pub live_disk_size: u64,
    /// The on-disk size of garbage data.
    pub garbage_disk_size: u64,
}

/// Test helpers for engines.
#[cfg(test)]
pub mod test {
    use super::*;
    use crossbeam::channel::Sender;
    use regex::Regex;
    use std::error::Error as StdError;
    use std::fmt::Write as _;
    use std::result::Result as StdResult;

    /// Goldenscript runner for engines. All engines use a common set of
    /// goldenscripts in src/storage/testscripts/engine, as well as their own
    /// engine-specific tests.
    pub struct Runner<E: Engine> {
        pub engine: E,
    }

    impl<E: Engine> goldenscript::Runner for Runner<E> {
        fn run(&mut self, command: &goldenscript::Command) -> StdResult<String, Box<dyn StdError>> {
            let mut output = String::new();
            match command.name.as_str() {
                // delete KEY
                "delete" => {
                    let mut args = command.consume_args();
                    let key = Self::decode_binary(&args.next_pos().ok_or("key not given")?.value);
                    args.reject_rest()?;
                    self.engine.delete(&key)?;
                }

                // get KEY
                "get" => {
                    let mut args = command.consume_args();
                    let key = Self::decode_binary(&args.next_pos().ok_or("key not given")?.value);
                    args.reject_rest()?;
                    let value = self.engine.get(&key)?;
                    output.push_str(&Self::format_key_value(&key, value.as_deref()))
                }

                // scan [reverse=BOOL] RANGE
                "scan" => {
                    let mut args = command.consume_args();
                    let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                    let range = Self::parse_key_range(
                        args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."),
                    )?;
                    args.reject_rest()?;
                    let items: Vec<_> = if reverse {
                        self.engine.scan(range).rev().collect::<Result<_>>()?
                    } else {
                        self.engine.scan(range).collect::<Result<_>>()?
                    };
                    for (key, value) in items {
                        writeln!(output, "{}", Self::format_key_value(&key, Some(&value)))?;
                    }
                }

                // scan_prefix PREFIX
                "scan_prefix" => {
                    let mut args = command.consume_args();
                    let prefix =
                        Self::decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
                    args.reject_rest()?;
                    let mut scan = self.engine.scan_prefix(&prefix);
                    while let Some((key, value)) = scan.next().transpose()? {
                        writeln!(output, "{}", Self::format_key_value(&key, Some(&value)))?;
                    }
                }

                // set KEY=VALUE
                "set" => {
                    let mut args = command.consume_args();
                    let kv = args.next_key().ok_or("key=value not given")?.clone();
                    let key = Self::decode_binary(&kv.key.unwrap());
                    let value = Self::decode_binary(&kv.value);
                    args.reject_rest()?;
                    self.engine.set(&key, value)?;
                }

                // status
                "status" => {
                    command.consume_args().reject_rest()?;
                    writeln!(output, "{:#?}", self.engine.status()?)?;
                }

                name => return Err(format!("invalid command {name}").into()),
            }
            Ok(output)
        }
    }

    impl<E: Engine> Runner<E> {
        pub fn new(engine: E) -> Self {
            Self { engine }
        }

        /// Decodes a raw byte vector from a Unicode string. Code points in the
        /// range U+0080 to U+00FF are converted back to bytes 0x80 to 0xff.
        /// This allows using e.g. \xff in the input string literal, and getting
        /// back a 0xff byte in the byte vector. Otherwise, char(0xff) yields
        /// the UTF-8 bytes 0xc3bf, which is the U+00FF code point as UTF-8.
        /// These characters are effectively represented as ISO-8859-1 rather
        /// than UTF-8, but it allows precise use of the entire u8 value range.
        pub fn decode_binary(s: &str) -> Vec<u8> {
            let mut buf = [0; 4];
            let mut bytes = Vec::new();
            for c in s.chars() {
                // u32 is the Unicode code point, not the UTF-8 encoding.
                match c as u32 {
                    b @ 0x80..=0xff => bytes.push(b as u8),
                    _ => bytes.extend(c.encode_utf8(&mut buf).as_bytes()),
                }
            }
            bytes
        }

        /// Formats a raw binary byte vector, escaping special characters.
        /// TODO: find a better way to manage and share formatting functions.
        pub fn format_bytes(bytes: &[u8]) -> String {
            let b: Vec<u8> = bytes.iter().copied().flat_map(std::ascii::escape_default).collect();
            String::from_utf8_lossy(&b).to_string()
        }

        /// Formats a key/value pair, or None if the value does not exist.
        pub fn format_key_value(key: &[u8], value: Option<&[u8]>) -> String {
            format!(
                "{} → {}",
                Self::format_bytes(key),
                value.map(Self::format_bytes).unwrap_or("None".to_string())
            )
        }

        /// Parses an binary key range, using Rust range syntax.
        fn parse_key_range(
            s: &str,
        ) -> StdResult<impl std::ops::RangeBounds<Vec<u8>>, Box<dyn StdError>> {
            use std::ops::Bound;
            let mut bound = (Bound::<Vec<u8>>::Unbounded, Bound::<Vec<u8>>::Unbounded);
            let re = Regex::new(r"^(\S+)?\.\.(=)?(\S+)?").expect("invalid regex");
            let groups = re.captures(s).ok_or_else(|| format!("invalid range {s}"))?;
            if let Some(start) = groups.get(1) {
                bound.0 = Bound::Included(Self::decode_binary(start.as_str()));
            }
            if let Some(end) = groups.get(3) {
                let end = Self::decode_binary(end.as_str());
                if groups.get(2).is_some() {
                    bound.1 = Bound::Included(end)
                } else {
                    bound.1 = Bound::Excluded(end)
                }
            }
            Ok(bound)
        }
    }

    /// Wraps another engine and emits write events to the given channel.
    pub struct Emit<E: Engine> {
        /// The wrapped engine.
        inner: E,
        /// Sends operation events.
        tx: Sender<Operation>,
    }

    /// An engine operation emitted by the Emit engine.
    pub enum Operation {
        Delete { key: Vec<u8> },
        Flush,
        Set { key: Vec<u8>, value: Vec<u8> },
    }

    impl<E: Engine> Emit<E> {
        pub fn new(inner: E, tx: Sender<Operation>) -> Self {
            Self { inner, tx }
        }
    }

    impl<E: Engine> Engine for Emit<E> {
        type ScanIterator<'a> = E::ScanIterator<'a> where E: 'a;

        fn flush(&mut self) -> Result<()> {
            self.inner.flush()?;
            self.tx.send(Operation::Flush)?;
            Ok(())
        }

        fn delete(&mut self, key: &[u8]) -> Result<()> {
            self.inner.delete(key)?;
            self.tx.send(Operation::Delete { key: key.to_vec() })?;
            Ok(())
        }

        fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.inner.get(key)
        }

        fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_> {
            self.inner.scan(range)
        }

        fn scan_dyn(
            &mut self,
            range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
        ) -> Box<dyn ScanIterator + '_> {
            Box::new(self.scan(range))
        }

        fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
            self.inner.set(key, value.clone())?;
            self.tx.send(Operation::Set { key: key.to_vec(), value })?;
            Ok(())
        }

        fn status(&mut self) -> Result<Status> {
            self.inner.status()
        }
    }

    /// An engine that wraps two others and mirrors operations across them,
    /// panicking if they produce different results. Engine implementations
    /// should not have any observable differences in behavior.
    ///
    /// TODO: use this in more tests, e.g. SQL tests.
    pub struct Mirror<A: Engine, B: Engine> {
        pub a: A,
        pub b: B,
    }

    impl<A: Engine, B: Engine> Mirror<A, B> {
        pub fn new(a: A, b: B) -> Self {
            Self { a, b }
        }
    }

    impl<A: Engine, B: Engine> Engine for Mirror<A, B> {
        type ScanIterator<'a> = MirrorIterator<'a, A, B>
        where
            Self: Sized,
            A: 'a,
            B: 'a;

        fn delete(&mut self, key: &[u8]) -> Result<()> {
            self.a.delete(key)?;
            self.b.delete(key)
        }

        fn flush(&mut self) -> Result<()> {
            self.a.flush()?;
            self.b.flush()
        }

        fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            let a = self.a.get(key)?;
            let b = self.b.get(key)?;
            assert_eq!(a, b);
            Ok(a)
        }

        fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
        where
            Self: Sized,
        {
            let a = self.a.scan((range.start_bound().cloned(), range.end_bound().cloned()));
            let b = self.b.scan(range);
            MirrorIterator { a, b }
        }

        fn scan_dyn(
            &mut self,
            range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
        ) -> Box<dyn ScanIterator + '_> {
            let a = self.a.scan(range.clone());
            let b = self.b.scan(range);
            Box::new(MirrorIterator::<A, B> { a, b })
        }

        fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
            self.a.set(key, value.clone())?;
            self.b.set(key, value)
        }

        fn status(&mut self) -> Result<Status> {
            let a = self.a.status()?;
            let b = self.b.status()?;
            // Only some items are comparable.
            assert_eq!(a.keys, b.keys);
            assert_eq!(a.size, b.size);
            Ok(a)
        }
    }

    pub struct MirrorIterator<'a, A: Engine + 'a, B: Engine + 'a> {
        a: A::ScanIterator<'a>,
        b: B::ScanIterator<'a>,
    }

    impl<'a, A: Engine, B: Engine> Iterator for MirrorIterator<'a, A, B> {
        type Item = Result<(Vec<u8>, Vec<u8>)>;

        fn next(&mut self) -> Option<Self::Item> {
            let a = self.a.next();
            let b = self.b.next();
            assert_eq!(a, b);
            a
        }
    }

    impl<'a, A: Engine, B: Engine> DoubleEndedIterator for MirrorIterator<'a, A, B> {
        fn next_back(&mut self) -> Option<Self::Item> {
            let a = self.a.next_back();
            let b = self.b.next_back();
            assert_eq!(a, b);
            a
        }
    }
}
