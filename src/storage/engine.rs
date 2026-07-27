use std::ops::{Bound, RangeBounds};

use serde::{Deserialize, Serialize};

use crate::encoding::keycode;
use crate::error::Result;

/// A key/value storage engine, which stores arbitrary byte strings. Keys are
/// maintained in lexicographical order, which allows for range scans. This is
/// needed e.g. to scan all rows in a specific SQL table (where all table rows
/// have a common key prefix), or to scan the tail of the Raft log (after a
/// given log entry index).
///
/// Keys should use the Keycode order-preserving encoding, see
/// [`crate::encoding::keycode`].
///
/// Writes are only guaranteed durable after calling [`Engine::flush()`].
///
/// For simplicity, this only supports a single user at a time, so all methods
/// (including reads) take a mutable reference. This isn't that big of a deal
/// since Raft execution is serial anyway.
pub trait Engine: Send {
    /// The iterator returned by [`Engine::scan`].
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: Sized + 'a; // omit in trait objects, for dyn compatibility

    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Flushes any buffered data to disk.
    fn flush(&mut self) -> Result<()>;

    /// Gets a value for a key, if it exists.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
    where
        Self: Sized; // omit in trait objects, for dyn compatibility

    /// Like scan, but can be used from trait objects (with dynamic dispatch).
    fn scan_dyn(&mut self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator + '_>;

    /// Iterates over all key/value pairs starting with the given prefix.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_>
    where
        Self: Sized, // omit in trait objects, for dyn compatibility
    {
        self.scan(keycode::prefix_range(prefix))
    }

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Returns the engine status.
    fn status(&mut self) -> Result<Status>;
}

/// A scan iterator over key/value pairs, returned by [`Engine::scan()`].
pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

/// Blanket implementation for all iterators that can act as a scan iterator.
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
    pub disk_size: u64,
    /// The on-disk size of live data, excluding garbage.
    pub live_disk_size: u64,
}

impl Status {
    /// The on-disk size of garbage data.
    pub fn garbage_disk_size(&self) -> u64 {
        self.disk_size - self.live_disk_size
    }

    /// The ratio of on-disk garbage to total size.
    pub fn garbage_disk_percent(&self) -> f64 {
        if self.disk_size == 0 {
            return 0.0;
        }
        self.garbage_disk_size() as f64 / self.disk_size as f64 * 100.0
    }
}

/// Test helpers for engines.
#[cfg(test)]
pub mod test {
    use std::convert::Infallible;
    use std::error::Error as StdError;
    use std::fmt::Write as _;
    use std::ops::{Bound, Deref, RangeBounds};
    use std::result::Result as StdResult;
    use std::str::FromStr;

    use crossbeam::channel::Sender;
    use itertools::Itertools as _;
    use regex::Regex;

    use super::*;
    use crate::encoding::format::{self, Formatter as _};

    /// Goldenscript runner for engines. All engines use a common set of
    /// goldenscripts in src/storage/testscripts/engine, as well as their own
    /// engine-specific tests.
    pub struct Runner<E: Engine> {
        pub engine: E,
    }

    /// Commands accepted by the engine Goldenscript runner.
    #[derive(goldenscript::Command)]
    pub enum Command {
        /// Deletes a key.
        Delete(BinaryString),
        /// Fetches a key.
        Get(BinaryString),
        /// Scans a key range.
        Scan {
            /// The key range in Rust range syntax, or the full range if omitted.
            #[arg(optional)]
            range: KeyRange,
            /// Whether to scan in reverse.
            #[arg(key, optional)]
            reverse: bool,
        },
        /// Scans all keys with the given prefix.
        ScanPrefix(BinaryString),
        /// Sets a key/value pair.
        Set(
            /// The single key/value pair to set.
            Vec<(BinaryString, BinaryString)>,
        ),
        /// Displays engine status.
        Status,
    }

    /// A string parsed into its binary byte representation.
    ///
    /// Code points U+0080 through U+00FF are converted directly to bytes 0x80
    /// through 0xff. This allows using e.g. `\xff` in an input string literal
    /// to represent the byte 0xff rather than its UTF-8 encoding 0xc3bf.
    pub struct BinaryString(Vec<u8>);

    impl FromStr for BinaryString {
        type Err = Infallible;

        fn from_str(value: &str) -> StdResult<Self, Self::Err> {
            let mut buf = [0; 4];
            let mut bytes = Vec::new();
            for c in value.chars() {
                // u32 is the Unicode code point, not the UTF-8 encoding.
                match c as u32 {
                    b @ 0x80..=0xff => bytes.push(b as u8),
                    _ => bytes.extend(c.encode_utf8(&mut buf).as_bytes()),
                }
            }
            Ok(Self(bytes))
        }
    }

    impl Deref for BinaryString {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl From<BinaryString> for Vec<u8> {
        fn from(value: BinaryString) -> Self {
            value.0
        }
    }

    /// A binary key range parsed from Rust range syntax and BinaryString.
    pub struct KeyRange(Bound<Vec<u8>>, Bound<Vec<u8>>);

    impl Default for KeyRange {
        fn default() -> Self {
            Self(Bound::Unbounded, Bound::Unbounded)
        }
    }

    impl FromStr for KeyRange {
        type Err = Box<dyn StdError>;

        fn from_str(value: &str) -> StdResult<Self, Self::Err> {
            let mut range = Self::default();
            let re = Regex::new(r"^(\S+)?\.\.(=)?(\S+)?").expect("invalid regex");
            let groups = re.captures(value).ok_or_else(|| format!("invalid range {value}"))?;
            if let Some(start) = groups.get(1) {
                range.0 = Bound::Included(start.as_str().parse::<BinaryString>()?.into());
            }
            if let Some(end) = groups.get(3) {
                let end = end.as_str().parse::<BinaryString>()?.into();
                range.1 = match groups.get(2) {
                    Some(_) => Bound::Included(end),
                    None => Bound::Excluded(end),
                };
            }
            Ok(range)
        }
    }

    impl RangeBounds<Vec<u8>> for KeyRange {
        fn start_bound(&self) -> Bound<&Vec<u8>> {
            self.0.as_ref()
        }

        fn end_bound(&self) -> Bound<&Vec<u8>> {
            self.1.as_ref()
        }
    }

    impl RangeBounds<Vec<u8>> for &KeyRange {
        fn start_bound(&self) -> Bound<&Vec<u8>> {
            self.0.as_ref()
        }

        fn end_bound(&self) -> Bound<&Vec<u8>> {
            self.1.as_ref()
        }
    }

    impl<E: Engine> Runner<E> {
        pub fn new(engine: E) -> Self {
            Self { engine }
        }
    }

    impl<E: Engine> goldenscript::Runner for Runner<E> {
        type Command = Command;

        fn run(
            &mut self,
            command: &Command,
            _: &goldenscript::Context,
        ) -> StdResult<String, Box<dyn StdError>> {
            let mut output = String::new();
            match command {
                Command::Delete(key) => {
                    self.engine.delete(key)?;
                }

                Command::Get(key) => {
                    let value = self.engine.get(key)?;
                    writeln!(output, "{}", format::Raw::key_maybe_value(key, value.as_deref()))?;
                }

                &Command::Scan { ref range, reverse } => {
                    let items: Vec<_> = if reverse {
                        self.engine.scan(range).rev().try_collect()?
                    } else {
                        self.engine.scan(range).try_collect()?
                    };
                    for (key, value) in items {
                        let fmtkv = format::Raw::key_value(&key, &value);
                        writeln!(output, "{fmtkv}")?;
                    }
                }

                Command::ScanPrefix(prefix) => {
                    let mut scan = self.engine.scan_prefix(prefix);
                    while let Some((key, value)) = scan.next().transpose()? {
                        let fmtkv = format::Raw::key_value(&key, &value);
                        writeln!(output, "{fmtkv}")?;
                    }
                }

                Command::Set(entries) => {
                    let [(key, value)] = entries.as_slice() else {
                        return Err("must specify one key=value pair".into());
                    };
                    self.engine.set(key, value.to_vec())?;
                }

                Command::Status => {
                    writeln!(output, "{:#?}", self.engine.status()?)?;
                }
            }
            Ok(output)
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
        type ScanIterator<'a>
            = E::ScanIterator<'a>
        where
            E: 'a;

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

        fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_> {
            self.inner.scan(range)
        }

        fn scan_dyn(
            &mut self,
            range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
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
        type ScanIterator<'a>
            = MirrorIterator<'a, A, B>
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

        fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
        where
            Self: Sized,
        {
            let a = self.a.scan((range.start_bound().cloned(), range.end_bound().cloned()));
            let b = self.b.scan(range);
            MirrorIterator { a, b }
        }

        fn scan_dyn(
            &mut self,
            range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
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

    impl<A: Engine, B: Engine> Iterator for MirrorIterator<'_, A, B> {
        type Item = Result<(Vec<u8>, Vec<u8>)>;

        fn next(&mut self) -> Option<Self::Item> {
            let a = self.a.next();
            let b = self.b.next();
            assert_eq!(a, b);
            a
        }
    }

    impl<A: Engine, B: Engine> DoubleEndedIterator for MirrorIterator<'_, A, B> {
        fn next_back(&mut self) -> Option<Self::Item> {
            let a = self.a.next_back();
            let b = self.b.next_back();
            assert_eq!(a, b);
            a
        }
    }
}
