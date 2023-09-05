use super::{keycode, Engine};
use crate::error::{Error, Result};

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashSet;
use std::iter::Peekable;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, MutexGuard};

/// An MVCC version represents a logical timestamp. The latest version
/// is incremented when beginning each read-write transaction.
type Version = u64;

/// MVCC keys, using the KeyCode encoding which preserves the ordering and
/// grouping of keys. Cow byte slices allow encoding borrowed values and
/// decoding into owned values.
#[derive(Debug, Deserialize, Serialize)]
enum Key<'a> {
    /// The next available version.
    NextVersion,
    /// Active (uncommitted) transactions by version.
    TxnActive(Version),
    /// A snapshot of the active set at each version. Only written for
    /// versions where the active set is non-empty (excluding itself).
    TxnActiveSnapshot(Version),
    /// Update marker for a version and key, used for rollback.
    TxnUpdate(
        Version,
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    /// A versioned key/value pair.
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
        Version,
    ),
    /// Unversioned non-transactional key/value pairs. These exist separately
    /// from versioned keys, i.e. the unversioned key "foo" is entirely
    /// independent of the versioned key "foo@7".
    Unversioned(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
}

impl<'a> Key<'a> {
    fn decode(bytes: &'a [u8]) -> Result<Self> {
        keycode::deserialize(bytes)
    }

    fn encode(&self) -> Result<Vec<u8>> {
        keycode::serialize(&self)
    }
}

/// MVCC status
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub txns: u64,
    pub txns_active: u64,
    pub storage: String,
}

/// An MVCC-based transactional key-value engine.
pub struct MVCC<E: Engine> {
    /// The underlying KV engine. It is protected by a mutex so it can be shared between txns.
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for MVCC<E> {
    fn clone(&self) -> Self {
        MVCC { engine: self.engine.clone() }
    }
}

impl<E: Engine> MVCC<E> {
    /// Creates a new MVCC engine with the given storage engine.
    pub fn new(engine: E) -> Self {
        Self { engine: Arc::new(Mutex::new(engine)) }
    }

    /// Begins a new read-write transaction.
    pub fn begin(&self) -> Result<Transaction<E>> {
        Transaction::begin(self.engine.clone())
    }

    /// Begins a new read-only transaction at the latest version.
    pub fn begin_read_only(&self) -> Result<Transaction<E>> {
        Transaction::begin_read_only(self.engine.clone(), None)
    }

    /// Begins a new read-only transaction as of the given version.
    pub fn begin_as_of(&self, version: Version) -> Result<Transaction<E>> {
        Transaction::begin_read_only(self.engine.clone(), Some(version))
    }

    /// Resumes a transaction from the given transaction state.
    pub fn resume(&self, state: TransactionState) -> Result<Transaction<E>> {
        Transaction::resume(self.engine.clone(), state)
    }

    /// Fetches the value of an unversioned key.
    pub fn get_unversioned(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.engine.lock()?.get(&Key::Unversioned(key.into()).encode()?)
    }

    /// Sets the value of an unversioned key.
    pub fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.engine.lock()?.set(&Key::Unversioned(key.into()).encode()?, value)
    }

    /// Returns engine status
    //
    // Bizarrely, the return statement is in fact necessary - see:
    // https://github.com/rust-lang/reference/issues/452
    #[allow(clippy::needless_return)]
    pub fn status(&self) -> Result<Status> {
        let mut engine = self.engine.lock()?;
        return Ok(Status {
            storage: engine.to_string(),
            txns: match engine.get(&Key::NextVersion.encode()?)? {
                Some(ref v) => bincode::deserialize(v)?,
                None => 1,
            } - 1,
            txns_active: engine
                .scan(Key::TxnActive(0).encode()?..Key::TxnActive(std::u64::MAX).encode()?)
                .try_fold(0, |count, r| r.map(|_| count + 1))?,
        });
    }
}

/// An MVCC transaction.
pub struct Transaction<E: Engine> {
    /// The underlying engine for the transaction. Shared between transactions using a mutex.
    engine: Arc<Mutex<E>>,
    /// The version this transaction is running at. Only one read-write
    /// transaction can run at a given version, since this identifies its
    /// writes.
    version: Version,
    /// If true, the transaction is read only.
    read_only: bool,
    /// The set of concurrent active (uncommitted) transactions. Their writes
    /// should be invisible to this transaction even if they're writing at a
    /// lower version, since they're not committed yet.
    active: HashSet<Version>,
}

/// A serializable representation of a Transaction's state. It can be exported
/// from a Transaction and later used to instantiate a new Transaction that's
/// functionally equivalent via Transaction::resume(). In particular, this
/// allows passing the transaction between the SQL engine and storage engine
/// across the Raft state machine boundary.
#[derive(Clone, Serialize, Deserialize)]
pub struct TransactionState {
    pub version: u64,
    pub read_only: bool,
    pub active: HashSet<u64>,
}

impl<E: Engine> Transaction<E> {
    /// Begins a new transaction in read-write mode. This will allocate a new
    /// version that the transaction can write at, add it to the active set, and
    /// record its active snapshot for time-travel queries.
    fn begin(engine: Arc<Mutex<E>>) -> Result<Self> {
        let mut session = engine.lock()?;

        // Allocate a new version to write at.
        let version = match session.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => bincode::deserialize(v)?,
            None => 1,
        };
        session.set(&Key::NextVersion.encode()?, bincode::serialize(&(version + 1))?)?;

        // Fetch the current set of active transactions, persist it for
        // time-travel queries if non-empty, then add this txn to it.
        let active = Self::scan_active(&mut session)?;
        if !active.is_empty() {
            session.set(&Key::TxnActiveSnapshot(version).encode()?, bincode::serialize(&active)?)?
        }
        session.set(&Key::TxnActive(version).encode()?, vec![])?;
        drop(session);

        Ok(Self { engine, version, read_only: false, active })
    }

    /// Begins a new read-only transaction. If version is given it will see the
    /// state as of the beginning of that version (ignoring writes at that
    /// version). In other words, it sees the same state as the read-write
    /// transaction at that version saw when it began.
    fn begin_read_only(engine: Arc<Mutex<E>>, as_of: Option<Version>) -> Result<Self> {
        let mut session = engine.lock()?;

        // Fetch the latest version.
        let mut version = match session.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => bincode::deserialize(v)?,
            None => 1,
        };

        // If requested, create the transaction as of a past version, restoring
        // the active snapshot as of the beginning of that version. Otherwise,
        // use the latest version and get the current, real-time snapshot.
        let mut active = HashSet::new();
        if let Some(as_of) = as_of {
            if as_of >= version {
                return Err(Error::Value(format!("Version {} does not exist", as_of)));
            }
            version = as_of;
            if let Some(value) = session.get(&Key::TxnActiveSnapshot(version).encode()?)? {
                active = bincode::deserialize(&value)?;
            }
        } else {
            active = Self::scan_active(&mut session)?;
        }

        drop(session);

        Ok(Self { engine, version, read_only: true, active })
    }

    /// Resumes a transaction from the given state.
    fn resume(engine: Arc<Mutex<E>>, s: TransactionState) -> Result<Self> {
        // For read-write transactions, verify that the transaction is still
        // active before making further writes.
        if !s.read_only && engine.lock()?.get(&Key::TxnActive(s.version).encode()?)?.is_none() {
            return Err(Error::Internal(format!("No active transaction at version {}", s.version)));
        }
        Ok(Self { engine, version: s.version, read_only: s.read_only, active: s.active })
    }

    /// Scans the set of currently active transactions.
    fn scan_active(session: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active = HashSet::new();
        // TODO: Add Engine.scan_prefix() trait method.
        let mut scan =
            session.scan(Key::TxnActive(0).encode()?..=Key::TxnActive(u64::MAX).encode()?);
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnActive(version) => active.insert(version),
                _ => return Err(Error::Internal(format!("Expected TxnActive key, got {:?}", key))),
            };
        }
        Ok(active)
    }

    /// Returns the version the transaction is running at.
    pub fn version(&self) -> Version {
        self.version
    }

    /// Returns whether the transaction is read-only.
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// Returns the transaction's state. This can be used to instantiate a
    /// functionally equivalent transaction via resume().
    pub fn state(&self) -> TransactionState {
        TransactionState {
            version: self.version,
            read_only: self.read_only,
            active: self.active.clone(), // TODO: avoid cloning
        }
    }

    /// Commits the transaction, by removing the txn from the active set.
    pub fn commit(self) -> Result<()> {
        let mut session = self.engine.lock()?;
        session.delete(&Key::TxnActive(self.version).encode()?)?;
        session.flush()
    }

    /// Rolls back the transaction, by removing all updated entries.
    pub fn rollback(self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let mut session = self.engine.lock()?;
        let mut rollback = Vec::new();
        let mut scan = session.scan(
            Key::TxnUpdate(self.version, vec![].into()).encode()?
                ..Key::TxnUpdate(self.version + 1, vec![].into()).encode()?,
        );
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnUpdate(_, updated_key) => rollback.push(updated_key.into_owned()),
                k => return Err(Error::Internal(format!("Expected TxnUpdate, got {:?}", k))),
            };
            rollback.push(key);
        }
        std::mem::drop(scan);
        for key in rollback.into_iter() {
            session.delete(&key)?;
        }
        session.delete(&Key::TxnActive(self.version).encode()?)
    }

    /// Checks whether the given version is visible to this transaction.
    fn is_visible(&self, version: Version) -> bool {
        if self.active.get(&version).is_some() {
            false
        } else if self.read_only {
            version < self.version
        } else {
            version <= self.version
        }
    }

    /// Deletes a key.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    /// Fetches a key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut session = self.engine.lock()?;
        let mut scan = session
            .scan(
                Key::Version(key.into(), 0).encode()?
                    ..=Key::Version(key.into(), self.version).encode()?,
            )
            .rev();
        while let Some((k, v)) = scan.next().transpose()? {
            match Key::decode(&k)? {
                Key::Version(_, version) => {
                    if self.is_visible(version) {
                        return Ok(bincode::deserialize(&v)?);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Txn::Record, got {:?}", k))),
            };
        }
        Ok(None)
    }

    /// Scans a key range.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<ScanIterator> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.into(), u64::MAX).encode()?),
            Bound::Included(k) => Bound::Included(Key::Version(k.into(), 0).encode()?),
            Bound::Unbounded => Bound::Included(Key::Version(vec![].into(), 0).encode()?),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.into(), 0).encode()?),
            Bound::Included(k) => Bound::Included(Key::Version(k.into(), u64::MAX).encode()?),
            Bound::Unbounded => Bound::Unbounded,
        };
        // TODO: For now, collect results from the engine to not have to deal with lifetimes.
        let scan = Box::new(self.engine.lock()?.scan((start, end)).collect::<Vec<_>>().into_iter());
        Ok(Box::new(Scan::new(scan, self.version, self.read_only, self.active.clone())))
    }

    /// Scans keys under a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<ScanIterator> {
        if prefix.is_empty() {
            return Err(Error::Internal("Scan prefix cannot be empty".into()));
        }
        let start = prefix.to_vec();
        let mut end = start.clone();
        for i in (0..end.len()).rev() {
            match end[i] {
                // If all 0xff we could in principle use Range::Unbounded, but it won't happen
                0xff if i == 0 => return Err(Error::Internal("Invalid prefix scan range".into())),
                0xff => {
                    end[i] = 0x00;
                    continue;
                }
                v => {
                    end[i] = v + 1;
                    break;
                }
            }
        }
        self.scan(start..end)
    }

    /// Sets a key.
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// Writes a value for a key. None is used for deletion.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let mut session = self.engine.lock()?;

        // Check if the key is dirty, i.e. if it has any uncommitted changes, by scanning for any
        // versions that aren't visible to us.
        let min = self.active.iter().min().cloned().unwrap_or(self.version + 1);
        let mut scan = session
            .scan(
                Key::Version(key.into(), min).encode()?
                    ..=Key::Version(key.into(), u64::MAX).encode()?,
            )
            .rev();
        while let Some((k, _)) = scan.next().transpose()? {
            match Key::decode(&k)? {
                Key::Version(_, version) => {
                    if !self.is_visible(version) {
                        return Err(Error::Serialization);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Txn::Record, got {:?}", k))),
            };
        }
        std::mem::drop(scan);

        // Write the key and its update record.
        let key = Key::Version(key.into(), self.version).encode()?;
        let update = Key::TxnUpdate(self.version, (&key).into()).encode()?;
        session.set(&update, vec![])?;
        session.set(&key, bincode::serialize(&value)?)
    }
}

pub type ScanIterator<'a> =
    Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send + 'a>;

/// A key range scan.
pub struct Scan<'a> {
    /// The augmented KV engine iterator, with key (decoded) and value. Note that we don't retain
    /// the decoded version, so there will be multiple keys (for each version). We want the last.
    scan: Peekable<ScanIterator<'a>>,
    /// Keeps track of next_back() seen key, whose previous versions should be ignored.
    next_back_seen: Option<Vec<u8>>,
}

impl<'a> Scan<'a> {
    /// Creates a new scan.
    fn new(
        mut scan: ScanIterator<'a>,
        txn_version: Version,
        read_only: bool,
        active: HashSet<Version>,
    ) -> Self {
        /// Checks whether the given version is visible to this transaction.
        fn is_visible(
            txn_version: Version,
            read_only: bool,
            active: &HashSet<Version>,
            version: Version,
        ) -> bool {
            if active.get(&version).is_some() {
                false
            } else if read_only {
                version < txn_version
            } else {
                version <= txn_version
            }
        }

        // Augment the underlying scan to decode the key and filter invisible versions. We don't
        // return the version, since we don't need it, but beware that all versions of the key
        // will still be returned - we usually only need the last, which is what the next() and
        // next_back() methods need to handle. We also don't decode the value, since we only need
        // to decode the last version.
        scan = Box::new(scan.filter_map(move |r| {
            r.and_then(|(k, v)| match Key::decode(&k)? {
                Key::Version(_, version)
                    if !is_visible(txn_version, read_only, &active, version) =>
                {
                    Ok(None)
                }
                Key::Version(key, _) => Ok(Some((key.into_owned(), v))),
                k => Err(Error::Internal(format!("Expected Record, got {:?}", k))),
            })
            .transpose()
        }));
        Self { scan: scan.peekable(), next_back_seen: None }
    }

    // next() with error handling.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next().transpose()? {
            // Only return the item if it is the last version of the key.
            if match self.scan.peek() {
                Some(Ok((peek_key, _))) if *peek_key != key => true,
                Some(Ok(_)) => false,
                Some(Err(err)) => return Err(err.clone()),
                None => true,
            } {
                // Only return non-deleted items.
                if let Some(value) = bincode::deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    /// next_back() with error handling.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next_back().transpose()? {
            // Only return the last version of the key (so skip if seen).
            if match &self.next_back_seen {
                Some(seen_key) if *seen_key != key => true,
                Some(_) => false,
                None => true,
            } {
                self.next_back_seen = Some(key.clone());
                // Only return non-deleted items.
                if let Some(value) = bincode::deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}

impl<'a> Iterator for Scan<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a> DoubleEndedIterator for Scan<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[cfg(test)]
pub mod tests {
    use super::super::engine::Memory;
    use super::*;

    fn setup() -> MVCC<Memory> {
        MVCC::new(Memory::new())
    }

    #[test]
    fn test_begin() -> Result<()> {
        let mvcc = setup();

        let txn = mvcc.begin()?;
        assert_eq!(1, txn.version());
        assert!(!txn.read_only());
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(2, txn.version());
        txn.rollback()?;

        let txn = mvcc.begin()?;
        assert_eq!(3, txn.version());
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_begin_read_only() -> Result<()> {
        let mvcc = setup();
        let txn = mvcc.begin_read_only()?;
        assert_eq!(txn.version(), 1);
        assert!(txn.read_only());
        txn.commit()?;
        Ok(())
    }

    #[test]
    fn test_begin_as_of() -> Result<()> {
        let mvcc = setup();

        // Start a concurrent transaction that should be invisible.
        let mut t1 = mvcc.begin()?;
        t1.set(b"other", vec![1])?;

        // Write a couple of versions for a key. Commit the concurrent one in between.
        let mut t2 = mvcc.begin()?;
        t2.set(b"key", vec![2])?;
        t2.commit()?;

        let mut t3 = mvcc.begin()?;
        t3.set(b"key", vec![3])?;
        t3.commit()?;

        t1.commit()?;

        let mut t4 = mvcc.begin()?;
        t4.set(b"key", vec![4])?;
        t4.commit()?;

        // Check that we can start a snapshot as of version 3. It should see
        // key=2 and other=None (because it hadn't committed yet).
        let txn = mvcc.begin_as_of(3)?;
        assert_eq!(txn.version(), 3);
        assert!(txn.read_only());
        assert_eq!(txn.get(b"key")?, Some(vec![2]));
        assert_eq!(txn.get(b"other")?, None);
        txn.commit()?;

        // A snapshot as of version 4 should see key=3 and Other=2.
        let txn = mvcc.begin_as_of(4)?;
        assert_eq!(txn.version(), 4);
        assert!(txn.read_only());
        assert_eq!(txn.get(b"key")?, Some(vec![3]));
        assert_eq!(txn.get(b"other")?, Some(vec![1]));
        txn.commit()?;

        // Check that any future versions are invalid.
        assert_eq!(
            mvcc.begin_as_of(9).err(),
            Some(Error::Value("Version 9 does not exist".into()))
        );

        Ok(())
    }

    #[test]
    fn test_resume() -> Result<()> {
        let mvcc = setup();

        // We first write a set of values that should be visible
        let mut t1 = mvcc.begin()?;
        t1.set(b"a", b"t1".to_vec())?;
        t1.set(b"b", b"t1".to_vec())?;
        t1.commit()?;

        // We then start three transactions, of which we will resume t3.
        // We commit t2 and t4's changes, which should not be visible,
        // and write a change for t3 which should be visible.
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;
        let mut t4 = mvcc.begin()?;

        t2.set(b"a", b"t2".to_vec())?;
        t3.set(b"b", b"t3".to_vec())?;
        t4.set(b"c", b"t4".to_vec())?;

        t2.commit()?;
        t4.commit()?;

        // We now resume t3, who should see it's own changes but none
        // of the others'
        let state = t3.state();
        std::mem::drop(t3);
        let tr = mvcc.resume(state.clone())?;
        assert_eq!(3, tr.version());
        assert!(!tr.read_only());

        assert_eq!(Some(b"t1".to_vec()), tr.get(b"a")?);
        assert_eq!(Some(b"t3".to_vec()), tr.get(b"b")?);
        assert_eq!(None, tr.get(b"c")?);

        // A separate transaction should not see t3's changes, but should see the others
        let t = mvcc.begin()?;
        assert_eq!(Some(b"t2".to_vec()), t.get(b"a")?);
        assert_eq!(Some(b"t1".to_vec()), t.get(b"b")?);
        assert_eq!(Some(b"t4".to_vec()), t.get(b"c")?);
        t.rollback()?;

        // Once tr commits, a separate transaction should see t3's changes
        tr.commit()?;

        // Resuming an inactive transaction should error.
        assert_eq!(
            mvcc.resume(state).err(),
            Some(Error::Internal("No active transaction at version 3".into()))
        );

        let t = mvcc.begin()?;
        assert_eq!(Some(b"t2".to_vec()), t.get(b"a")?);
        assert_eq!(Some(b"t3".to_vec()), t.get(b"b")?);
        assert_eq!(Some(b"t4".to_vec()), t.get(b"c")?);
        t.rollback()?;

        // It should also be possible to start a snapshot transaction and resume it.
        let ts = mvcc.begin_as_of(2)?;
        assert_eq!(2, ts.version());
        assert_eq!(Some(b"t1".to_vec()), ts.get(b"a")?);

        let state = ts.state();
        std::mem::drop(ts);
        let ts = mvcc.resume(state)?;
        assert_eq!(2, ts.version());
        assert!(ts.read_only());
        assert_eq!(Some(b"t1".to_vec()), ts.get(b"a")?);
        ts.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_delete_conflict() -> Result<()> {
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
    fn test_txn_delete_idempotent() -> Result<()> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.delete(b"key")?;
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_get() -> Result<()> {
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
    fn test_txn_get_deleted() -> Result<()> {
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
    fn test_txn_get_hides_newer() -> Result<()> {
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
    fn test_txn_get_hides_uncommitted() -> Result<()> {
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
    fn test_txn_get_readonly_historical() -> Result<()> {
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

        let tr = mvcc.begin_as_of(3)?;
        assert_eq!(Some(vec![0x01]), tr.get(b"a")?);
        assert_eq!(Some(vec![0x02]), tr.get(b"b")?);
        assert_eq!(None, tr.get(b"c")?);

        Ok(())
    }

    #[test]
    fn test_txn_get_serial() -> Result<()> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(b"a", vec![0x01])?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(Some(vec![0x01]), txn.get(b"a")?);

        Ok(())
    }

    #[test]
    fn test_txn_scan() -> Result<()> {
        let mvcc = setup();
        let mut txn = mvcc.begin()?;

        txn.set(b"a", vec![0x01])?;

        txn.delete(b"b")?;

        txn.set(b"c", vec![0x01])?;
        txn.set(b"c", vec![0x02])?;
        txn.delete(b"c")?;
        txn.set(b"c", vec![0x03])?;

        txn.set(b"d", vec![0x01])?;
        txn.set(b"d", vec![0x02])?;
        txn.set(b"d", vec![0x03])?;
        txn.set(b"d", vec![0x04])?;
        txn.delete(b"d")?;

        txn.set(b"e", vec![0x01])?;
        txn.set(b"e", vec![0x02])?;
        txn.set(b"e", vec![0x03])?;
        txn.delete(b"e")?;
        txn.set(b"e", vec![0x04])?;
        txn.set(b"e", vec![0x05])?;
        txn.commit()?;

        // Forward scan
        let txn = mvcc.begin()?;
        assert_eq!(
            vec![
                (b"a".to_vec(), vec![0x01]),
                (b"c".to_vec(), vec![0x03]),
                (b"e".to_vec(), vec![0x05]),
            ],
            txn.scan(..)?.collect::<Result<Vec<_>>>()?
        );

        // Reverse scan
        assert_eq!(
            vec![
                (b"e".to_vec(), vec![0x05]),
                (b"c".to_vec(), vec![0x03]),
                (b"a".to_vec(), vec![0x01]),
            ],
            txn.scan(..)?.rev().collect::<Result<Vec<_>>>()?
        );

        // Alternate forward/backward scan
        let mut scan = txn.scan(..)?;
        assert_eq!(Some((b"a".to_vec(), vec![0x01])), scan.next().transpose()?);
        assert_eq!(Some((b"e".to_vec(), vec![0x05])), scan.next_back().transpose()?);
        assert_eq!(Some((b"c".to_vec(), vec![0x03])), scan.next_back().transpose()?);
        assert_eq!(None, scan.next().transpose()?);
        std::mem::drop(scan);

        txn.commit()?;
        Ok(())
    }

    #[test]
    fn test_txn_scan_key_version_overlap() -> Result<()> {
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
        txn.set(&[0], vec![0])?; // v0
        txn.set(&[0], vec![1])?; // v1
        txn.set(&[0, 0, 0, 0, 0, 0, 0, 0, 2], vec![2])?; // v2
        txn.set(&[0], vec![3])?; // v3
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(
            vec![(vec![0].to_vec(), vec![3]), (vec![0, 0, 0, 0, 0, 0, 0, 0, 2].to_vec(), vec![2]),],
            txn.scan(..)?.collect::<Result<Vec<_>>>()?
        );
        Ok(())
    }

    #[test]
    fn test_txn_scan_prefix() -> Result<()> {
        let mvcc = setup();
        let mut txn = mvcc.begin()?;

        txn.set(b"a", vec![0x01])?;
        txn.set(b"az", vec![0x01, 0x1a])?;
        txn.set(b"b", vec![0x02])?;
        txn.set(b"ba", vec![0x02, 0x01])?;
        txn.set(b"bb", vec![0x02, 0x02])?;
        txn.set(b"bc", vec![0x02, 0x03])?;
        txn.set(b"c", vec![0x03])?;
        txn.commit()?;

        // Forward scan
        let txn = mvcc.begin()?;
        assert_eq!(
            vec![
                (b"b".to_vec(), vec![0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
                (b"bc".to_vec(), vec![0x02, 0x03]),
            ],
            txn.scan_prefix(b"b")?.collect::<Result<Vec<_>>>()?
        );

        // Reverse scan
        assert_eq!(
            vec![
                (b"bc".to_vec(), vec![0x02, 0x03]),
                (b"bb".to_vec(), vec![0x02, 0x02]),
                (b"ba".to_vec(), vec![0x02, 0x01]),
                (b"b".to_vec(), vec![0x02]),
            ],
            txn.scan_prefix(b"b")?.rev().collect::<Result<Vec<_>>>()?
        );

        // Alternate forward/backward scan
        let mut scan = txn.scan_prefix(b"b")?;
        assert_eq!(Some((b"b".to_vec(), vec![0x02])), scan.next().transpose()?);
        assert_eq!(Some((b"bc".to_vec(), vec![0x02, 0x03])), scan.next_back().transpose()?);
        assert_eq!(Some((b"bb".to_vec(), vec![0x02, 0x02])), scan.next_back().transpose()?);
        assert_eq!(Some((b"ba".to_vec(), vec![0x02, 0x01])), scan.next().transpose()?);
        assert_eq!(None, scan.next_back().transpose()?);
        std::mem::drop(scan);

        txn.commit()?;
        Ok(())
    }

    #[test]
    fn test_txn_set_conflict() -> Result<()> {
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
    fn test_txn_set_conflict_committed() -> Result<()> {
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
    fn test_txn_set_rollback() -> Result<()> {
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

    #[test]
    // A dirty write is when t2 overwrites an uncommitted value written by t1.
    fn test_txn_anomaly_dirty_write() -> Result<()> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;

        t1.set(b"key", b"t1".to_vec())?;
        assert_eq!(t2.set(b"key", b"t2".to_vec()), Err(Error::Serialization));

        Ok(())
    }

    #[test]
    // A dirty read is when t2 can read an uncommitted value set by t1.
    fn test_txn_anomaly_dirty_read() -> Result<()> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;

        t1.set(b"key", b"t1".to_vec())?;
        assert_eq!(None, t2.get(b"key")?);

        Ok(())
    }

    #[test]
    // A lost update is when t1 and t2 both read a value and update it, where t2's update replaces t1.
    fn test_txn_anomaly_lost_update() -> Result<()> {
        let mvcc = setup();

        let mut t0 = mvcc.begin()?;
        t0.set(b"key", b"t0".to_vec())?;
        t0.commit()?;

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;

        t1.get(b"key")?;
        t2.get(b"key")?;

        t1.set(b"key", b"t1".to_vec())?;
        assert_eq!(t2.set(b"key", b"t2".to_vec()), Err(Error::Serialization));

        Ok(())
    }

    #[test]
    // A fuzzy (or unrepeatable) read is when t2 sees a value change after t1 updates it.
    fn test_txn_anomaly_fuzzy_read() -> Result<()> {
        let mvcc = setup();

        let mut t0 = mvcc.begin()?;
        t0.set(b"key", b"t0".to_vec())?;
        t0.commit()?;

        let mut t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;

        assert_eq!(Some(b"t0".to_vec()), t2.get(b"key")?);
        t1.set(b"key", b"t1".to_vec())?;
        t1.commit()?;
        assert_eq!(Some(b"t0".to_vec()), t2.get(b"key")?);

        Ok(())
    }

    #[test]
    // Read skew is when t1 reads a and b, but t2 modifies b in between the reads.
    fn test_txn_anomaly_read_skew() -> Result<()> {
        let mvcc = setup();

        let mut t0 = mvcc.begin()?;
        t0.set(b"a", b"t0".to_vec())?;
        t0.set(b"b", b"t0".to_vec())?;
        t0.commit()?;

        let t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;

        assert_eq!(Some(b"t0".to_vec()), t1.get(b"a")?);
        t2.set(b"a", b"t2".to_vec())?;
        t2.set(b"b", b"t2".to_vec())?;
        t2.commit()?;
        assert_eq!(Some(b"t0".to_vec()), t1.get(b"b")?);

        Ok(())
    }

    #[test]
    // A phantom read is when t1 reads entries matching some predicate, but a modification by
    // t2 changes the entries that match the predicate such that a later read by t1 returns them.
    fn test_txn_anomaly_phantom_read() -> Result<()> {
        let mvcc = setup();

        let mut t0 = mvcc.begin()?;
        t0.set(b"a", b"true".to_vec())?;
        t0.set(b"b", b"false".to_vec())?;
        t0.commit()?;

        let t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;

        assert_eq!(Some(b"true".to_vec()), t1.get(b"a")?);
        assert_eq!(Some(b"false".to_vec()), t1.get(b"b")?);

        t2.set(b"b", b"true".to_vec())?;
        t2.commit()?;

        assert_eq!(Some(b"true".to_vec()), t1.get(b"a")?);
        assert_eq!(Some(b"false".to_vec()), t1.get(b"b")?);

        Ok(())
    }

    /* FIXME To avoid write skew we need to implement serializable snapshot isolation.
    #[test]
    // Write skew is when t1 reads b and writes it to a while t2 reads a and writes it to b.¨
    fn test_txn_anomaly_write_skew() -> Result<()> {
        let mvcc = setup();

        let mut t0 = mvcc.begin()?;
        t0.set(b"a", b"1".to_vec())?;
        t0.set(b"b", b"2".to_vec())?;
        t0.commit()?;

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;

        assert_eq!(Some(b"1".to_vec()), t1.get(b"a")?);
        assert_eq!(Some(b"2".to_vec()), t2.get(b"b")?);

        // Some of the following operations should error
        t1.set(b"a", b"2".to_vec())?;
        t2.set(b"b", b"1".to_vec())?;

        t1.commit()?;
        t2.commit()?;

        Ok(())
    }*/

    #[test]
    /// Tests unversioned key/value pairs, via set/get_unversioned().
    fn test_unversioned() -> Result<()> {
        let m = setup();

        // Unversioned keys should not interact with versioned keys.
        let mut txn = m.begin()?;
        txn.set(b"foo", b"bar".to_vec())?;
        txn.commit()?;

        // The unversioned key should return None.
        assert_eq!(m.get_unversioned(b"foo")?, None);

        // Setting and then fetching the unversioned key should return its value.
        m.set_unversioned(b"foo", b"bar".to_vec())?;
        assert_eq!(m.get_unversioned(b"foo")?, Some(b"bar".to_vec()));

        // Replacing it should return the new value.
        m.set_unversioned(b"foo", b"baz".to_vec())?;
        assert_eq!(m.get_unversioned(b"foo")?, Some(b"baz".to_vec()));

        // The versioned key should remain unaffected.
        let txn = m.begin_read_only()?;
        assert_eq!(txn.get(b"foo")?, Some(b"bar".to_vec()));

        Ok(())
    }
}
