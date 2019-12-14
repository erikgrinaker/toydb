use super::storage::{Range, Storage};
use crate::utility::{deserialize, serialize};
use crate::Error;

use std::collections::HashSet;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// An MVCC-based transactional key-value store
pub struct MVCC<S: Storage> {
    storage: Arc<RwLock<S>>,
}

impl<S: Storage> MVCC<S> {
    /// Creates a new MVCC key-value store
    pub fn new(storage: S) -> Self {
        Self { storage: Arc::new(RwLock::new(storage)) }
    }

    /// Begins a new transaction in the default mutable mode
    #[allow(dead_code)]
    pub fn begin(&self) -> Result<Transaction<S>, Error> {
        Transaction::begin(self.storage.clone(), Mode::ReadWrite)
    }

    /// Begins a new transaction in the given mode
    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction<S>, Error> {
        Transaction::begin(self.storage.clone(), mode)
    }

    /// Resumes a transaction
    pub fn resume(&self, id: u64) -> Result<Transaction<S>, Error> {
        Transaction::resume(self.storage.clone(), id)
    }
}

/// An MVCC transaction mode
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Mode {
    ReadWrite,
    // FIXME Add transient read-only transactions
    ReadOnly,
    Snapshot { version: u64 },
}

impl Mode {
    fn assert_mutable(&self) -> Result<(), Error> {
        if self.is_mutable() {
            Ok(())
        } else {
            Err(Error::ReadOnly)
        }
    }

    fn is_mutable(&self) -> bool {
        match self {
            Self::ReadWrite => true,
            Self::ReadOnly => false,
            Self::Snapshot { .. } => false,
        }
    }
}

/// An MVCC transaction
pub struct Transaction<S: Storage> {
    storage: Arc<RwLock<S>>,
    id: u64,
    mode: Mode,
    snapshot: Snapshot,
}

impl<S: Storage> Transaction<S> {
    /// Begins a new transaction
    fn begin(storage: Arc<RwLock<S>>, mode: Mode) -> Result<Self, Error> {
        // FIXME Handle transient
        let id = {
            let mut storage = storage.write()?;
            let id = match storage.read(&Key::TxnNext.encode())? {
                Some(v) => deserialize(v)?,
                None => 1,
            };
            storage.write(&Key::TxnNext.encode(), serialize(id + 1)?)?;
            storage.write(&Key::TxnActive(id).encode(), serialize(mode.clone())?)?;
            id
        };
        let snapshot = match &mode {
            Mode::Snapshot { version } => Snapshot::restore(&storage.read()?, *version)?,
            _ => Snapshot::take(&mut storage.write()?, id)?,
        };
        Ok(Self { storage, id, mode, snapshot })
    }

    /// Resumes a transaction
    fn resume(storage: Arc<RwLock<S>>, id: u64) -> Result<Self, Error> {
        let key = Key::TxnActive(id).encode();
        let mode = if let Some(v) = storage.read()?.read(&key)? {
            deserialize(v)?
        } else {
            return Err(Error::Value(format!("Unable to resume non-existant transaction {}", id)));
        };
        let snapshot = match &mode {
            Mode::Snapshot { version } => Snapshot::restore(&storage.read()?, *version)?,
            _ => Snapshot::restore(&storage.read()?, id)?,
        };
        Ok(Self { storage, id, mode, snapshot })
    }

    /// Returns the transaction ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the transaction mode
    pub fn mode(&self) -> Mode {
        self.mode.clone()
    }

    /// Commits the transaction
    pub fn commit(self) -> Result<(), Error> {
        if self.mode.is_mutable() {
            self.storage.write()?.remove(&Key::TxnActive(self.id).encode())?;
        }
        Ok(())
    }

    /// Rolls back the transaction
    pub fn rollback(self) -> Result<(), Error> {
        if self.mode.is_mutable() {
            let mut storage = self.storage.write()?;
            let start = Key::TxnUpdatedStart(self.id).encode();
            let end = Key::TxnUpdatedEnd(self.id).encode();
            for r in storage.scan(start..end).rev() {
                let (k, _) = r?;
                if let Key::TxnUpdated(_, key) = Key::decode(k.clone())? {
                    storage.remove(&key)?;
                    storage.remove(&k)?;
                } else {
                    return Err(Error::Internal("Unexpected key".into()));
                }
            }
            storage.remove(&Key::TxnActive(self.id).encode())?;
        }
        Ok(())
    }

    /// Deletes a key
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.mode.assert_mutable()?;
        let mut storage = self.storage.write()?;
        if self.is_dirty(&mut storage, key)? {
            return Err(Error::Serialization);
        }

        let key = Key::Record(key.to_vec(), self.id).encode();
        let update_key = Key::TxnUpdated(self.id, key.clone()).encode();
        storage.write(&update_key, Vec::new())?;
        storage.write(&key, Value::Delete.encode())?;
        Ok(())
    }

    /// Fetches a key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let from = Key::Record(key.to_vec(), 0).encode();
        let to = Key::Record(key.to_vec(), self.id).encode();
        let mut range = self.storage.read()?.scan(from..=to).rev();
        while let Some((k, v)) = range.next().transpose()? {
            if !self.snapshot.is_visible(Key::decode(k)?.version()?) {
                continue;
            }
            return match Value::decode(v)? {
                Value::Set(v) => Ok(Some(v)),
                Value::Delete => Ok(None),
            };
        }
        Ok(None)
    }

    /// Scans a key range
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<Range, Error> {
        let from = match range.start_bound() {
            Bound::Excluded(key) => Bound::Included(Key::Record(key.clone(), 0).encode()),
            Bound::Included(key) => Bound::Included(Key::Record(key.clone(), 0).encode()),
            Bound::Unbounded => Bound::Included(Key::RecordStart.encode()),
        };
        let to = match range.end_bound() {
            Bound::Excluded(key) => Bound::Excluded(Key::Record(key.clone(), 0).encode()),
            Bound::Included(key) => {
                Bound::Included(Key::Record(key.clone(), std::u64::MAX).encode())
            }
            Bound::Unbounded => Bound::Included(Key::RecordEnd.encode()),
        };
        Ok(Box::new(Scan {
            range: self.storage.read()?.scan((from, to)),
            snapshot: self.snapshot.clone(),
            seen: None,
            rev_ignore: None,
        }))
    }

    /// Sets a key
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        self.mode.assert_mutable()?;
        let mut storage = self.storage.write()?;
        if self.is_dirty(&mut storage, key)? {
            return Err(Error::Serialization);
        }
        let key = Key::Record(key.to_vec(), self.id).encode();
        let update_key = Key::TxnUpdated(self.id, key.clone()).encode();
        storage.write(&update_key, Vec::new())?;
        storage.write(&key, Value::Set(value).encode())?;
        Ok(())
    }

    /// Checks whether the key is dirty, i.e. if it has any uncommitted changes
    fn is_dirty(&self, storage: &mut RwLockWriteGuard<S>, key: &[u8]) -> Result<bool, Error> {
        let mut min = self.id + 1;
        for id in self.snapshot.invisible.iter().cloned() {
            if id < min {
                min = id
            }
        }
        let from = Key::Record(key.to_vec(), min).encode();
        let to = Key::Record(key.to_vec(), std::u64::MAX).encode();
        let mut range = storage.scan(from..to);
        while let Some((k, _)) = range.next().transpose()? {
            if !self.snapshot.is_visible(Key::decode(k)?.version()?) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// A versioned snapshot
#[derive(Clone)]
pub struct Snapshot {
    version: u64,
    invisible: HashSet<u64>,
}

impl Snapshot {
    /// Takes a new snapshot
    fn take(storage: &mut RwLockWriteGuard<impl Storage>, version: u64) -> Result<Self, Error> {
        let mut snapshot = Self { version, invisible: HashSet::new() };
        for r in storage.scan(&Key::TxnActive(0).encode()..&Key::TxnActive(version).encode()) {
            let (k, _) = r?;
            match Key::decode(k)? {
                Key::TxnActive(id) => snapshot.invisible.insert(id),
                _ => return Err(Error::Value("Unexpected MVCC key, wanted TxnActive".into())),
            };
        }
        storage
            .write(&Key::TxnSnapshot(version).encode(), serialize(snapshot.invisible.clone())?)?;
        Ok(snapshot)
    }

    /// Restores an existing snapshot
    fn restore(storage: &RwLockReadGuard<impl Storage>, version: u64) -> Result<Self, Error> {
        if let Some(v) = storage.read(&Key::TxnSnapshot(version).encode())? {
            Ok(Self { version, invisible: deserialize(v)? })
        } else {
            Err(Error::Value(format!("Unable to find snapshot for version {}", version)))
        }
    }

    /// Checks whether the given version is visible to us
    fn is_visible(&self, version: u64) -> bool {
        version <= self.version && self.invisible.get(&version).is_none()
    }
}

/// A key range scan
pub struct Scan {
    range: Range,
    snapshot: Snapshot,
    seen: Option<(Vec<u8>, Vec<u8>)>,
    rev_ignore: Option<(Vec<u8>)>,
}

impl Iterator for Scan {
    type Item = Result<(Vec<u8>, Vec<u8>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(r) = self.range.next() {
            match r {
                Ok((k, v)) => {
                    let (key, version) = match Key::decode(k).unwrap() {
                        Key::Record(key, version) => (key, version),
                        _ => panic!("Not implemented"),
                    };
                    if !self.snapshot.is_visible(version) {
                        continue;
                    }
                    let value = match Value::decode(v).unwrap() {
                        Value::Set(value) => value,
                        Value::Delete => {
                            let mut clear_seen = false;
                            if let Some((k, _)) = &self.seen {
                                if k == &key {
                                    clear_seen = true;
                                }
                            }
                            if clear_seen {
                                self.seen = None;
                            }
                            continue;
                        }
                    };

                    let mut ret = None;
                    if let Some((k, v)) = &self.seen {
                        if k != &key {
                            ret = Some(Ok((k.clone(), v.clone())));
                        }
                    };
                    self.seen = Some((key, value));
                    if ret.is_some() {
                        return ret;
                    }
                }
                Err(err) => return Some(Err(err)),
            }
        }
        let mut pair = None;
        if let Some((k, v)) = &self.seen {
            pair = Some(Ok((k.clone(), v.clone())));
        }
        self.seen = None;
        pair
    }
}

impl DoubleEndedIterator for Scan {
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some(r) = self.range.next_back() {
            match r {
                Ok((k, v)) => {
                    let (key, version) = match Key::decode(k).unwrap() {
                        Key::Record(key, version) => (key, version),
                        _ => panic!("Not implemented"),
                    };
                    if !self.snapshot.is_visible(version) {
                        continue;
                    }
                    if let Some(ignore) = &self.rev_ignore {
                        if &key == ignore {
                            continue;
                        }
                    }
                    self.rev_ignore = Some(key.clone());
                    let value = match Value::decode(v).unwrap() {
                        Value::Set(value) => value,
                        Value::Delete => continue,
                    };
                    return Some(Ok((key, value)));
                }
                Err(err) => return Some(Err(err)),
            }
        }
        None
    }
}

#[derive(Debug)]
enum Key {
    TxnNext,
    TxnActive(u64),
    TxnSnapshot(u64),
    TxnUpdatedStart(u64),
    TxnUpdated(u64, Vec<u8>),
    TxnUpdatedEnd(u64),
    Record(Vec<u8>, u64),
    RecordStart,
    RecordEnd,
}

impl Key {
    fn decode(key: Vec<u8>) -> Result<Self, Error> {
        let mut iter = key.into_iter();
        match iter.next() {
            Some(0x01) => Ok(Key::TxnNext),
            Some(0x02) => {
                let bytes: Vec<u8> = iter.collect();
                if bytes.len() != 8 {
                    return Err(Error::Value("Unable to parse MVCC active transaction ID".into()));
                }
                let mut id = [0; 8];
                id.copy_from_slice(&bytes[..]);
                Ok(Key::TxnActive(u64::from_be_bytes(id)))
            }
            Some(0x03) => {
                let bytes: Vec<u8> = iter.collect();
                if bytes.len() != 8 {
                    return Err(Error::Value("Unable to parse MVCC snapshot version".into()));
                }
                let mut version = [0; 8];
                version.copy_from_slice(&bytes[..]);
                Ok(Key::TxnSnapshot(u64::from_be_bytes(version)))
            }
            Some(0x04) => {
                let bytes: Vec<u8> = iter.collect();
                if bytes.len() < 10 {
                    return Err(Error::Value("Unable to parse MVCC update key".into()));
                }
                let mut id = [0; 8];
                id.copy_from_slice(&bytes[0..8]);
                let id = u64::from_be_bytes(id);
                if bytes.len() == 10 {
                    return match bytes[9] {
                        0x00 => Ok(Key::TxnUpdatedStart(id)),
                        0xff => Ok(Key::TxnUpdatedEnd(id)),
                        _ => return Err(Error::Value("Invalid MVCC update key".into())),
                    };
                }

                let key = bytes[9..].to_vec();
                Ok(Key::TxnUpdated(id, key))
            }
            Some(0xf1) => {
                let mut key: Vec<u8> = iter.collect();
                if key.len() < 9 {
                    return Err(Error::Value("Unable to parse MVCC record key".into()));
                }
                let mut v = [0; 8];
                v.copy_from_slice(&key.split_off(key.len() - 8)[..]);
                assert_eq!(Some(0x00), key.pop());
                let version = u64::from_be_bytes(v);

                Ok(Self::Record(Self::unescape(key), version))
            }
            _ => Err(Error::Value("Unable to parse MVCC key".into())),
        }
    }

    fn encode(self) -> Vec<u8> {
        match self {
            Self::TxnNext => vec![0x01],
            Self::TxnActive(id) => [vec![0x02], id.to_be_bytes().to_vec()].concat(),
            Self::TxnSnapshot(version) => [vec![0x03], version.to_be_bytes().to_vec()].concat(),
            Self::TxnUpdatedStart(id) => {
                [vec![0x04], id.to_be_bytes().to_vec(), vec![0x00]].concat()
            }
            Self::TxnUpdated(id, key) => {
                [vec![0x04], id.to_be_bytes().to_vec(), vec![0x00], key].concat()
            }
            Self::TxnUpdatedEnd(id) => [vec![0x04], id.to_be_bytes().to_vec(), vec![0xff]].concat(),
            Self::RecordStart => vec![0xf0],
            Self::Record(key, version) => {
                // We have to use 0x00 as a key/version separator, and use 0x00 0xff as an
                // escape sequence for 0x00 in order to avoid key/version overlaps from
                // messing up the key sequence during scans. For more information, see:
                // https://activesphere.com/blog/2018/08/17/order-preserving-serialization
                [vec![0xf1], Self::escape(key), vec![0x00], version.to_be_bytes().to_vec()].concat()
            }
            Self::RecordEnd => vec![0xf2],
        }
    }

    fn escape(bytes: Vec<u8>) -> Vec<u8> {
        let mut escaped = vec![];
        for b in bytes.into_iter() {
            escaped.push(b);
            if b == 0x00 {
                escaped.push(0xff);
            }
        }
        escaped
    }

    fn unescape(bytes: Vec<u8>) -> Vec<u8> {
        let mut unescaped = vec![];
        let mut iter = bytes.into_iter();
        while let Some(b) = iter.next() {
            unescaped.push(b);
            if b == 0x00 {
                assert_eq!(Some(0xff), iter.next())
            }
        }
        unescaped
    }

    fn version(&self) -> Result<u64, Error> {
        match self {
            Self::Record(_, version) => Ok(*version),
            _ => Err(Error::Value(format!("MVCC key {:?} has no version", self))),
        }
    }
}

enum Value {
    Delete,
    Set(Vec<u8>),
}

impl Value {
    fn decode(value: Vec<u8>) -> Result<Self, Error> {
        let mut iter = value.into_iter();
        match iter.next() {
            Some(0x00) => Ok(Value::Delete),
            Some(0xff) => Ok(Value::Set(iter.collect())),
            None => Err(Error::Value("No MVCC value found".into())),
            _ => Err(Error::Value("Unable to decode invalid MVCC value".into())),
        }
    }

    fn encode(self) -> Vec<u8> {
        match self {
            Self::Delete => vec![0x00],
            Self::Set(value) => [vec![0xff], value].concat(),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    fn setup() -> MVCC<super::super::storage::Memory> {
        MVCC::new(super::super::storage::Memory::new())
    }

    #[test]
    fn test_begin() -> Result<(), Error> {
        let mvcc = setup();

        let txn = mvcc.begin()?;
        assert_eq!(txn.id(), 1);
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(txn.id(), 2);
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_resume() -> Result<(), Error> {
        let mvcc = setup();

        // We first write a set of values that should be visible
        let mut t0 = mvcc.begin()?;
        t0.set(b"a", b"t0".to_vec())?;
        t0.set(b"b", b"t0".to_vec())?;
        t0.commit()?;

        // We then start three transactions, of which we will resume t2.
        // We commit t1 and t3's changes, which should not be visible,
        // and write a change for t2 which should be visible.
        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t1.set(b"a", b"t1".to_vec())?;
        t2.set(b"b", b"t2".to_vec())?;
        t3.set(b"c", b"t3".to_vec())?;

        t1.commit()?;
        t3.commit()?;

        // We now resume t2, who should see it's own changes but none
        // of the others'
        let id = t2.id();
        std::mem::drop(t2);
        let tr = mvcc.resume(id)?;

        assert_eq!(Some(b"t0".to_vec()), tr.get(b"a")?);
        assert_eq!(Some(b"t2".to_vec()), tr.get(b"b")?);
        assert_eq!(None, tr.get(b"c")?);

        // A separate transaction should not see t2's changes, but should see the others
        let t = mvcc.begin()?;
        assert_eq!(Some(b"t1".to_vec()), t.get(b"a")?);
        assert_eq!(Some(b"t0".to_vec()), t.get(b"b")?);
        assert_eq!(Some(b"t3".to_vec()), t.get(b"c")?);
        t.rollback()?;

        // Once tr commits, a separate transaction should see t2's changes
        tr.commit()?;

        let t = mvcc.begin()?;
        assert_eq!(Some(b"t1".to_vec()), t.get(b"a")?);
        assert_eq!(Some(b"t2".to_vec()), t.get(b"b")?);
        assert_eq!(Some(b"t3".to_vec()), t.get(b"c")?);
        t.rollback()?;

        Ok(())
    }

    #[test]
    fn test_txn_delete_conflict() -> Result<(), Error> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(b"key", vec![0x00])?;
        txn.commit()?;

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t2.delete(b"key")?;
        assert_eq!(Err(Error::Serialization), t1.delete(b"key"));
        assert_eq!(Err(Error::Serialization), t3.delete(b"key"));
        t2.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_delete_idempotent() -> Result<(), Error> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.delete(b"key")?;
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_get() -> Result<(), Error> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        assert_eq!(None, txn.get(b"a")?);
        txn.set(b"a", vec![0x01])?;
        assert_eq!(Some(vec![0x01]), txn.get(b"a")?);
        txn.set(b"a", vec![0x02])?;
        assert_eq!(Some(vec![0x02]), txn.get(b"a")?);
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_get_deleted() -> Result<(), Error> {
        let mvcc = setup();
        let mut txn = mvcc.begin()?;
        txn.set(b"a", vec![0x01])?;
        txn.commit()?;

        let mut txn = mvcc.begin()?;
        txn.delete(b"a")?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(None, txn.get(b"a")?);
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_get_hides_newer() -> Result<(), Error> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t1.set(b"a", vec![0x01])?;
        t1.commit()?;
        t3.set(b"c", vec![0x03])?;
        t3.commit()?;

        assert_eq!(None, t2.get(b"a")?);
        assert_eq!(None, t2.get(b"c")?);

        Ok(())
    }

    #[test]
    fn test_txn_get_hides_uncommitted() -> Result<(), Error> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        t1.set(b"a", vec![0x01])?;
        let t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;
        t3.set(b"c", vec![0x03])?;

        assert_eq!(None, t2.get(b"a")?);
        assert_eq!(None, t2.get(b"c")?);

        Ok(())
    }

    #[test]
    fn test_txn_get_readonly_historical() -> Result<(), Error> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(b"a", vec![0x01])?;
        txn.commit()?;

        let mut txn = mvcc.begin()?;
        txn.set(b"b", vec![0x02])?;
        txn.commit()?;

        let mut txn = mvcc.begin()?;
        txn.set(b"c", vec![0x03])?;
        txn.commit()?;

        let tr = mvcc.begin_with_mode(Mode::Snapshot { version: 2 })?;
        assert_eq!(Some(vec![0x01]), tr.get(b"a")?);
        assert_eq!(Some(vec![0x02]), tr.get(b"b")?);
        assert_eq!(None, tr.get(b"c")?);

        Ok(())
    }

    #[test]
    fn test_txn_get_serial() -> Result<(), Error> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(b"a", vec![0x01])?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(Some(vec![0x01]), txn.get(b"a")?);

        Ok(())
    }

    #[test]
    fn test_txn_scan() -> Result<(), Error> {
        let mvcc = setup();
        let mut txn = mvcc.begin()?;

        txn.set(b"a", vec![0x01])?;

        txn.delete(b"b")?;

        txn.set(b"c", vec![0x01])?;
        txn.set(b"c", vec![0x02])?;
        txn.set(b"c", vec![0x03])?;

        txn.set(b"d", vec![0x01])?;
        txn.set(b"d", vec![0x02])?;
        txn.set(b"d", vec![0x03])?;
        txn.set(b"d", vec![0x04])?;
        txn.delete(b"d")?;

        txn.set(b"e", vec![0x01])?;
        txn.set(b"e", vec![0x02])?;
        txn.set(b"e", vec![0x03])?;
        txn.set(b"e", vec![0x04])?;
        txn.delete(b"e")?;
        txn.set(b"e", vec![0x05])?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(
            vec![
                (b"a".to_vec(), vec![0x01]),
                (b"c".to_vec(), vec![0x03]),
                (b"e".to_vec(), vec![0x05]),
            ],
            txn.scan(..)?.collect::<Result<Vec<_>, _>>()?
        );

        assert_eq!(
            vec![
                (b"e".to_vec(), vec![0x05]),
                (b"c".to_vec(), vec![0x03]),
                (b"a".to_vec(), vec![0x01]),
            ],
            txn.scan(..)?.rev().collect::<Result<Vec<_>, _>>()?
        );
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_scan_key_version_overlap() -> Result<(), Error> {
        // The idea here is that with a naive key/version concatenation
        // we get overlapping entries that mess up scans. For example:
        //
        // 00|00 00 00 00 00 00 00 01
        // 00 00 00 00 00 00 00 00 02|00 00 00 00 00 00 00 02
        // 00|00 00 00 00 00 00 00 03
        //
        // The key encoding should be resistant to this.
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(&vec![0], vec![0])?; // v0
        txn.set(&vec![0], vec![1])?; // v1
        txn.set(&vec![0, 0, 0, 0, 0, 0, 0, 0, 2], vec![2])?; // v2
        txn.set(&vec![0], vec![3])?; // v3
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(
            vec![(vec![0].to_vec(), vec![3]), (vec![0, 0, 0, 0, 0, 0, 0, 0, 2].to_vec(), vec![2]),],
            txn.scan(..)?.collect::<Result<Vec<_>, _>>()?
        );
        Ok(())
    }

    #[test]
    fn test_txn_set_conflict() -> Result<(), Error> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t2.set(b"key", vec![0x02])?;
        assert_eq!(Err(Error::Serialization), t1.set(b"key", vec![0x01]));
        assert_eq!(Err(Error::Serialization), t3.set(b"key", vec![0x03]));
        t2.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_set_conflict_committed() -> Result<(), Error> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t2.set(b"key", vec![0x02])?;
        t2.commit()?;
        assert_eq!(Err(Error::Serialization), t1.set(b"key", vec![0x01]));
        assert_eq!(Err(Error::Serialization), t3.set(b"key", vec![0x03]));

        Ok(())
    }

    #[test]
    fn test_txn_set_rollback() -> Result<(), Error> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(b"key", vec![0x00])?;
        txn.commit()?;

        let t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t2.set(b"key", vec![0x02])?;
        t2.rollback()?;
        assert_eq!(Some(vec![0x00]), t1.get(b"key")?);
        t1.commit()?;
        t3.set(b"key", vec![0x03])?;
        t3.commit()?;

        Ok(())
    }
}
