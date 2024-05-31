//! This module implements MVCC (Multi-Version Concurrency Control), a widely
//! used method for ACID transactions and concurrency control. It allows
//! multiple concurrent transactions to access and modify the same dataset,
//! isolates them from each other, detects and handles conflicts, and commits
//! their writes atomically as a single unit. It uses an underlying storage
//! engine to store raw keys and values.
//!
//! VERSIONS
//! ========
//!
//! MVCC handles concurrency control by managing multiple historical versions of
//! keys, identified by a timestamp. Every write adds a new version at a higher
//! timestamp, with deletes having a special tombstone value. For example, the
//! keys a,b,c,d may have the following values at various logical timestamps (x
//! is tombstone):
//!
//! Time
//! 5
//! 4  a4          
//! 3      b3      x
//! 2            
//! 1  a1      c1  d1
//!    a   b   c   d   Keys
//!
//! A transaction t2 that started at T=2 will see the values a=a1, c=c1, d=d1. A
//! different transaction t5 running at T=5 will see a=a4, b=b3, c=c1.
//!
//! ToyDB uses logical timestamps with a sequence number stored in
//! Key::NextVersion. Each new read-write transaction takes its timestamp from
//! the current value of Key::NextVersion and then increments the value for the
//! next transaction.
//!
//! ISOLATION
//! =========
//!
//! MVCC provides an isolation level called snapshot isolation. Briefly,
//! transactions see a consistent snapshot of the database state as of their
//! start time. Writes made by concurrent or subsequent transactions are never
//! visible to it. If two concurrent transactions write to the same key they
//! will conflict and one of them must retry. A transaction's writes become
//! atomically visible to subsequent transactions only when they commit, and are
//! rolled back on failure. Read-only transactions never conflict with other
//! transactions.
//!
//! Transactions write new versions at their timestamp, storing them as
//! Key::Version(key, version) => value. If a transaction writes to a key and
//! finds a newer version, it returns an error and the client must retry.
//!
//! Active (uncommitted) read-write transactions record their version in the
//! active set, stored as Key::Active(version). When new transactions begin, they
//! take a snapshot of this active set, and any key versions that belong to a
//! transaction in the active set are considered invisible (to anyone except that
//! transaction itself). Writes to keys that already have a past version in the
//! active set will also return an error.
//!
//! To commit, a transaction simply deletes its record in the active set. This
//! will immediately (and, crucially, atomically) make all of its writes visible
//! to subsequent transactions, but not ongoing ones. If the transaction is
//! cancelled and rolled back, it maintains a record of all keys it wrote as
//! Key::TxnWrite(version, key), so that it can find the corresponding versions
//! and delete them before removing itself from the active set.
//!
//! Consider the following example, where we have two ongoing transactions at
//! time T=2 and T=5, with some writes that are not yet committed marked in
//! parentheses.
//!
//! Active set: [2, 5]
//!
//! Time
//! 5 (a5)
//! 4  a4          
//! 3      b3      x
//! 2         (x)     (e2)
//! 1  a1      c1  d1
//!    a   b   c   d   e   Keys
//!
//! Here, t2 will see a=a1, d=d1, e=e2 (it sees its own writes). t5 will see
//! a=a5, b=b3, c=c1. t2 does not see any newer versions, and t5 does not see
//! the tombstone at c@2 nor the value e=e2, because version=2 is in its active
//! set.
//!
//! If t2 tries to write b=b2, it receives an error and must retry, because a
//! newer version exists. Similarly, if t5 tries to write e=e5, it receives an
//! error and must retry, because the version e=e2 is in its active set.
//!
//! To commit, t2 can remove itself from the active set. A new transaction t6
//! starting after the commit will then see c as deleted and e=e2. t5 will still
//! not see any of t2's writes, because it's still in its local snapshot of the
//! active set at the time it began.
//!
//! READ-ONLY AND TIME TRAVEL QUERIES
//! =================================
//!
//! Since MVCC stores historical versions, it can trivially support time travel
//! queries where a transaction reads at a past timestamp and has a consistent
//! view of the database at that time.
//!
//! This is done by a transaction simply using a past version, as if it had
//! started far in the past, ignoring newer versions like any other transaction.
//! This transaction cannot write, as it does not have a unique timestamp (the
//! original read-write transaction originally owned this timestamp).
//!
//! The only wrinkle is that the time-travel query must also know what the active
//! set was at that version. Otherwise, it may see past transactions that committed
//! after that time, which were not visible to the original transaction that wrote
//! at that version. Similarly, if a time-travel query reads at a version that is
//! still active, it should not see its in-progress writes, and after it commits
//! a different time-travel query should not see those writes either, to maintain
//! version consistency.
//!
//! To achieve this, every read-write transaction stores its active set snapshot
//! in the storage engine as well, as Key::TxnActiveSnapshot, such that later
//! time-travel queries can restore its original snapshot. Furthermore, a
//! time-travel query can only see versions below the snapshot version, otherwise
//! it could see spurious in-progress or since-committed versions.
//!
//! In the following example, a time-travel query at version=3 would see a=a1,
//! c=c1, d=d1.
//!
//! Time
//! 5
//! 4  a4          
//! 3      b3      x
//! 2            
//! 1  a1      c1  d1
//!    a   b   c   d   Keys
//!
//! Read-only queries work similarly to time-travel queries, with one exception:
//! they read at the next (current) version, i.e. Key::NextVersion, and use the
//! current active set, storing the snapshot in memory only. Read-only queries
//! do not increment the version sequence number in Key::NextVersion.
//!
//! GARBAGE COLLECTION
//! ==================
//!
//! Normally, old versions would be garbage collected regularly, when they are
//! no longer needed by active transactions or time-travel queries. However,
//! ToyDB does not implement garbage collection, instead keeping all history
//! forever, both out of laziness and also because it allows unlimited time
//! travel queries (it's a feature, not a bug!).

use super::engine::Engine;
use crate::encoding::{self, bincode, Key as _, Value as _};
use crate::error::{Error, Result};

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashSet;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, MutexGuard};

/// An MVCC version represents a logical timestamp. The latest version
/// is incremented when beginning each read-write transaction.
type Version = u64;

impl encoding::Value for Version {}

/// MVCC keys, using the KeyCode encoding which preserves the ordering and
/// grouping of keys. Cow byte slices allow encoding borrowed values and
/// decoding into owned values.
#[derive(Debug, Deserialize, Serialize)]
pub enum Key<'a> {
    /// The next available version.
    NextVersion,
    /// Active (uncommitted) transactions by version.
    TxnActive(Version),
    /// A snapshot of the active set at each version. Only written for
    /// versions where the active set is non-empty (excluding itself).
    TxnActiveSnapshot(Version),
    /// Keeps track of all keys written to by an active transaction (identified
    /// by its version), in case it needs to roll back.
    TxnWrite(
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
    /// independent of the versioned key "foo@7". These are mostly used
    /// for metadata.
    Unversioned(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
}

impl<'a> encoding::Key<'a> for Key<'a> {}

/// MVCC key prefixes, for prefix scans. These must match the keys above,
/// including the enum variant index.
#[derive(Debug, Deserialize, Serialize)]
enum KeyPrefix<'a> {
    NextVersion,
    TxnActive,
    TxnActiveSnapshot,
    TxnWrite(Version),
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    Unversioned,
}

impl<'a> encoding::Key<'a> for KeyPrefix<'a> {}

/// An MVCC-based transactional key-value engine. It wraps an underlying storage
/// engine that's used for raw key/value storage.
///
/// While it supports any number of concurrent transactions, individual read or
/// write operations are executed sequentially, serialized via a mutex. There
/// are two reasons for this: the storage engine itself is not thread-safe,
/// requiring serialized access, and the Raft state machine that manages the
/// MVCC engine applies commands one at a time from the Raft log, which will
/// serialize them anyway.
pub struct MVCC<E: Engine> {
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

    /// Returns the status of the MVCC and storage engines.
    pub fn status(&self) -> Result<Status> {
        let mut engine = self.engine.lock()?;
        let versions = match engine.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => Version::decode(v)? - 1,
            None => 0,
        };
        let active_txns = engine.scan_prefix(&KeyPrefix::TxnActive.encode()?).count() as u64;
        Ok(Status { versions, active_txns, storage: engine.status()? })
    }
}

/// MVCC engine status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// The total number of MVCC versions (i.e. read-write transactions).
    pub versions: u64,
    /// Number of currently active transactions.
    pub active_txns: u64,
    /// The storage engine.
    pub storage: super::engine::Status,
}

impl encoding::Value for Status {}

/// An MVCC transaction.
pub struct Transaction<E: Engine> {
    /// The underlying engine, shared by all transactions.
    engine: Arc<Mutex<E>>,
    /// The transaction state.
    st: TransactionState,
}

/// A Transaction's state, which determines its write version and isolation. It
/// is separate from Transaction to allow it to be passed around independently
/// of the engine. There are two main motivations for this:
///
/// - It can be exported via Transaction.state(), (de)serialized, and later used
///   to instantiate a new functionally equivalent Transaction via
///   Transaction::resume(). This allows passing the transaction between the
///   storage engine and SQL engine (potentially running on a different node)
///   across the Raft state machine boundary.
///
/// - It can be borrowed independently of Engine, allowing references to it
///   in VisibleIterator, which would otherwise result in self-references.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionState {
    /// The version this transaction is running at. Only one read-write
    /// transaction can run at a given version, since this identifies its
    /// writes.
    pub version: Version,
    /// If true, the transaction is read only.
    pub read_only: bool,
    /// The set of concurrent active (uncommitted) transactions, as of the start
    /// of this transaction. Their writes should be invisible to this
    /// transaction even if they're writing at a lower version, since they're
    /// not committed yet.
    pub active: HashSet<Version>,
}

impl encoding::Value for TransactionState {}

impl TransactionState {
    /// Checks whether the given version is visible to this transaction.
    ///
    /// Future versions, and versions belonging to active transactions as of
    /// the start of this transaction, are never isible.
    ///
    /// Read-write transactions see their own writes at their version.
    ///
    /// Read-only queries only see versions below the transaction's version,
    /// excluding the version itself. This is to ensure time-travel queries see
    /// a consistent version both before and after any active transaction at
    /// that version commits its writes. See the module documentation for
    /// details.
    fn is_visible(&self, version: Version) -> bool {
        if self.active.get(&version).is_some() {
            false
        } else if self.read_only {
            version < self.version
        } else {
            version <= self.version
        }
    }
}

impl<E: Engine> Transaction<E> {
    /// Begins a new transaction in read-write mode. This will allocate a new
    /// version that the transaction can write at, add it to the active set, and
    /// record its active snapshot for time-travel queries.
    fn begin(engine: Arc<Mutex<E>>) -> Result<Self> {
        let mut session = engine.lock()?;

        // Allocate a new version to write at.
        let version = match session.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => Version::decode(v)?,
            None => 1,
        };
        session.set(&Key::NextVersion.encode()?, (version + 1).encode()?)?;

        // Fetch the current set of active transactions, persist it for
        // time-travel queries if non-empty, then add this txn to it.
        let active = Self::scan_active(&mut session)?;
        if !active.is_empty() {
            session.set(&Key::TxnActiveSnapshot(version).encode()?, active.encode()?)?
        }
        session.set(&Key::TxnActive(version).encode()?, vec![])?;
        drop(session);

        Ok(Self { engine, st: TransactionState { version, read_only: false, active } })
    }

    /// Begins a new read-only transaction. If version is given it will see the
    /// state as of the beginning of that version (ignoring writes at that
    /// version). In other words, it sees the same state as the read-write
    /// transaction at that version saw when it began.
    fn begin_read_only(engine: Arc<Mutex<E>>, as_of: Option<Version>) -> Result<Self> {
        let mut session = engine.lock()?;

        // Fetch the latest version.
        let mut version = match session.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => Version::decode(v)?,
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
                active = HashSet::<Version>::decode(&value)?;
            }
        } else {
            active = Self::scan_active(&mut session)?;
        }

        drop(session);

        Ok(Self { engine, st: TransactionState { version, read_only: true, active } })
    }

    /// Resumes a transaction from the given state.
    fn resume(engine: Arc<Mutex<E>>, s: TransactionState) -> Result<Self> {
        // For read-write transactions, verify that the transaction is still
        // active before making further writes.
        if !s.read_only && engine.lock()?.get(&Key::TxnActive(s.version).encode()?)?.is_none() {
            return Err(Error::Internal(format!("No active transaction at version {}", s.version)));
        }
        Ok(Self { engine, st: s })
    }

    /// Fetches the set of currently active transactions.
    fn scan_active(session: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active = HashSet::new();
        let mut scan = session.scan_prefix(&KeyPrefix::TxnActive.encode()?);
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
        self.st.version
    }

    /// Returns whether the transaction is read-only.
    pub fn read_only(&self) -> bool {
        self.st.read_only
    }

    /// Returns the transaction's state. This can be used to instantiate a
    /// functionally equivalent transaction via resume().
    pub fn state(&self) -> &TransactionState {
        &self.st
    }

    /// Commits the transaction, by removing it from the active set. This will
    /// immediately make its writes visible to subsequent transactions. Also
    /// removes its TxnWrite records, which are no longer needed.
    ///
    /// NB: commit does not flush writes to durable storage, since we rely on
    /// the Raft log for persistence.
    pub fn commit(self) -> Result<()> {
        if self.st.read_only {
            return Ok(());
        }
        let mut session = self.engine.lock()?;
        let remove = session
            .scan_prefix(&KeyPrefix::TxnWrite(self.st.version).encode()?)
            .map(|r| r.map(|(k, _)| k))
            .collect::<Result<Vec<_>>>()?;
        for key in remove {
            session.delete(&key)?
        }
        session.delete(&Key::TxnActive(self.st.version).encode()?)
    }

    /// Rolls back the transaction, by undoing all written versions and removing
    /// it from the active set. The active set snapshot is left behind, since
    /// this is needed for time travel queries at this version.
    pub fn rollback(self) -> Result<()> {
        if self.st.read_only {
            return Ok(());
        }
        let mut session = self.engine.lock()?;
        let mut rollback = Vec::new();
        let mut scan = session.scan_prefix(&KeyPrefix::TxnWrite(self.st.version).encode()?);
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnWrite(_, key) => {
                    rollback.push(Key::Version(key, self.st.version).encode()?) // the version
                }
                key => return Err(Error::Internal(format!("Expected TxnWrite, got {:?}", key))),
            };
            rollback.push(key); // the TxnWrite record
        }
        drop(scan);
        for key in rollback.into_iter() {
            session.delete(&key)?;
        }
        session.delete(&Key::TxnActive(self.st.version).encode()?) // remove from active set
    }

    /// Deletes a key.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_version(key, None)
    }

    /// Sets a value for a key.
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write_version(key, Some(value))
    }

    /// Writes a new version for a key at the transaction's version. None writes
    /// a deletion tombstone. If a write conflict is found (either a newer or
    /// uncommitted version), a serialization error is returned.  Replacing our
    /// own uncommitted write is fine.
    fn write_version(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if self.st.read_only {
            return Err(Error::ReadOnly);
        }
        let mut session = self.engine.lock()?;

        // Check for write conflicts, i.e. if the latest key is invisible to us
        // (either a newer version, or an uncommitted version in our past). We
        // can only conflict with the latest key, since all transactions enforce
        // the same invariant.
        let from = Key::Version(
            key.into(),
            self.st.active.iter().min().copied().unwrap_or(self.st.version + 1),
        )
        .encode()?;
        let to = Key::Version(key.into(), u64::MAX).encode()?;
        if let Some((key, _)) = session.scan(from..=to).last().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if !self.st.is_visible(version) {
                        return Err(Error::Serialization);
                    }
                }
                key => return Err(Error::Internal(format!("Expected Key::Version got {:?}", key))),
            }
        }

        // Write the new version and its write record.
        //
        // NB: TxnWrite contains the provided user key, not the encoded engine
        // key, since we can construct the engine key using the version.
        session.set(&Key::TxnWrite(self.st.version, key.into()).encode()?, vec![])?;
        session
            .set(&Key::Version(key.into(), self.st.version).encode()?, bincode::serialize(&value)?)
    }

    /// Fetches a key's value, or None if it does not exist.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut session = self.engine.lock()?;
        let from = Key::Version(key.into(), 0).encode()?;
        let to = Key::Version(key.into(), self.st.version).encode()?;
        let mut scan = session.scan(from..=to).rev();
        while let Some((key, value)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if self.st.is_visible(version) {
                        return bincode::deserialize(&value);
                    }
                }
                key => return Err(Error::Internal(format!("Expected Key::Version got {:?}", key))),
            };
        }
        Ok(None)
    }

    /// Returns an iterator over the latest visible key/value pairs at the
    /// transaction's version.
    pub fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Result<Scan<E>> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.into(), u64::MAX).encode()?),
            Bound::Included(k) => Bound::Included(Key::Version(k.into(), 0).encode()?),
            Bound::Unbounded => Bound::Included(Key::Version(vec![].into(), 0).encode()?),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.into(), 0).encode()?),
            Bound::Included(k) => Bound::Included(Key::Version(k.into(), u64::MAX).encode()?),
            Bound::Unbounded => Bound::Excluded(KeyPrefix::Unversioned.encode()?),
        };
        Ok(Scan::new(self.engine.lock()?, self.state(), start, end))
    }

    /// Scans keys under a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Scan<E>> {
        // Normally, KeyPrefix::Version will only match all versions of the
        // exact given key. We want all keys maching the prefix, so we chop off
        // the KeyCode byte slice terminator 0x0000 at the end.
        let mut prefix = KeyPrefix::Version(prefix.into()).encode()?;
        prefix.truncate(prefix.len() - 2);
        Ok(Scan::new_prefix(self.engine.lock()?, self.state(), prefix))
    }
}

/// A scan result. Can produce an iterator or collect an owned Vec.
///
/// This intermediate struct is unfortunately needed to hold the MutexGuard for
/// the scan() caller, since placing it in ScanIterator along with the inner
/// iterator borrowing from it would create a self-referential struct.
///
/// TODO: is there a better way?
pub struct Scan<'a, E: Engine + 'a> {
    /// Access to the locked engine.
    engine: MutexGuard<'a, E>,
    /// The transaction state.
    txn: &'a TransactionState,
    /// The scan type and parameter.
    param: ScanType,
}

enum ScanType {
    Range((Bound<Vec<u8>>, Bound<Vec<u8>>)),
    Prefix(Vec<u8>),
}

impl<'a, E: Engine + 'a> Scan<'a, E> {
    /// Creates a new range scan.
    fn new(
        engine: MutexGuard<'a, E>,
        txn: &'a TransactionState,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Self {
        Self { engine, txn, param: ScanType::Range((start, end)) }
    }

    /// Creates a new prefix scan.
    fn new_prefix(engine: MutexGuard<'a, E>, txn: &'a TransactionState, prefix: Vec<u8>) -> Self {
        Self { engine, txn, param: ScanType::Prefix(prefix) }
    }

    /// Returns an iterator over the result.
    pub fn iter(&mut self) -> ScanIterator<'_, E> {
        let inner = match &self.param {
            ScanType::Range(range) => self.engine.scan(range.clone()),
            ScanType::Prefix(prefix) => self.engine.scan_prefix(prefix),
        };
        ScanIterator::new(self.txn, inner)
    }

    /// Collects the result to a vector.
    pub fn to_vec(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.iter().collect()
    }
}

/// An iterator over the latest live and visible key/value pairs at the txn
/// version.
pub struct ScanIterator<'a, E: Engine + 'a> {
    /// Decodes and filters visible MVCC versions from the inner engine iterator.
    inner: std::iter::Peekable<VersionIterator<'a, E>>,
    /// The previous key emitted by try_next_back(). Note that try_next() does
    /// not affect reverse positioning: double-ended iterators consume from each
    /// end independently.
    last_back: Option<Vec<u8>>,
}

impl<'a, E: Engine + 'a> ScanIterator<'a, E> {
    /// Creates a new scan iterator.
    fn new(txn: &'a TransactionState, inner: E::ScanIterator<'a>) -> Self {
        Self { inner: VersionIterator::new(txn, inner).peekable(), last_back: None }
    }

    /// Fallible next(), emitting the next item, or None if exhausted.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, _version, value)) = self.inner.next().transpose()? {
            // If the next key equals this one, we're not at the latest version.
            match self.inner.peek() {
                Some(Ok((next, _, _))) if next == &key => continue,
                Some(Err(err)) => return Err(err.clone()),
                Some(Ok(_)) | None => {}
            }
            // If the key is live (not a tombstone), emit it.
            if let Some(value) = bincode::deserialize(&value)? {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }

    /// Fallible next_back(), emitting the next item from the back, or None if
    /// exhausted.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, _version, value)) = self.inner.next_back().transpose()? {
            // If this key is the same as the last emitted key from the back,
            // this must be an older version, so skip it.
            if let Some(last) = &self.last_back {
                if last == &key {
                    continue;
                }
            }
            self.last_back = Some(key.clone());

            // If the key is live (not a tombstone), emit it.
            if let Some(value) = bincode::deserialize(&value)? {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
}

impl<'a, E: Engine> Iterator for ScanIterator<'a, E> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a, E: Engine> DoubleEndedIterator for ScanIterator<'a, E> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

/// An iterator that decodes raw engine key/value pairs into MVCC key/value
/// versions, and skips invisible versions. Helper for ScanIterator.
struct VersionIterator<'a, E: Engine + 'a> {
    /// The transaction the scan is running in.
    txn: &'a TransactionState,
    /// The inner engine scan iterator.
    inner: E::ScanIterator<'a>,
}

#[allow(clippy::type_complexity)]
impl<'a, E: Engine + 'a> VersionIterator<'a, E> {
    /// Creates a new MVCC version iterator for the given engine iterator.
    fn new(txn: &'a TransactionState, inner: E::ScanIterator<'a>) -> Self {
        Self { txn, inner }
    }

    /// Decodes a raw engine key into an MVCC key and version, returning None if
    /// the version is not visible.
    fn decode_visible(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Version)>> {
        let (key, version) = match Key::decode(key)? {
            Key::Version(key, version) => (key.into_owned(), version),
            key => return Err(Error::Internal(format!("Expected Key::Version got {:?}", key))),
        };
        if self.txn.is_visible(version) {
            Ok(Some((key, version)))
        } else {
            Ok(None)
        }
    }

    // Fallible next(), emitting the next item, or None if exhausted.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            if let Some((key, version)) = self.decode_visible(&key)? {
                return Ok(Some((key, version, value)));
            }
        }
        Ok(None)
    }

    // Fallible next_back(), emitting the previous item, or None if exhausted.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>> {
        while let Some((key, value)) = self.inner.next_back().transpose()? {
            if let Some((key, version)) = self.decode_visible(&key)? {
                return Ok(Some((key, version, value)));
            }
        }
        Ok(None)
    }
}

impl<'a, E: Engine> Iterator for VersionIterator<'a, E> {
    type Item = Result<(Vec<u8>, Version, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a, E: Engine> DoubleEndedIterator for VersionIterator<'a, E> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[cfg(test)]
pub mod tests {
    use super::super::debug;
    use super::super::{Debug, Memory};
    use super::*;
    use std::collections::HashMap;
    use std::io::Write as _;

    const GOLDEN_DIR: &str = "src/storage/golden/mvcc";

    /// An MVCC wrapper that records transaction schedules to golden masters.
    /// TODO: migrate this to goldenscript.
    struct Schedule {
        mvcc: MVCC<Debug<Memory>>,
        mint: goldenfile::Mint,
        file: Arc<Mutex<std::fs::File>>,
        next_id: u8,
    }

    impl Schedule {
        /// Creates a new schedule using the given golden master filename.
        fn new(name: &str) -> Result<Self> {
            let mvcc = MVCC::new(Debug::new(Memory::new()));
            let mut mint = goldenfile::Mint::new(GOLDEN_DIR);
            let file = Arc::new(Mutex::new(mint.new_goldenfile(name)?));
            Ok(Self { mvcc, mint, file, next_id: 1 })
        }

        /// Sets up an initial, versioned dataset from the given data as a
        /// vector of key,version,value tuples. These transactions are not
        /// assigned transaction IDs, nor are the writes logged, except for the
        /// initial engine state.
        #[allow(clippy::type_complexity)]
        fn setup(&mut self, data: Vec<(&[u8], Version, Option<&[u8]>)>) -> Result<()> {
            // Segment the writes by version.
            let mut writes = HashMap::new();
            for (key, version, value) in data {
                writes
                    .entry(version)
                    .or_insert(Vec::new())
                    .push((key.to_vec(), value.map(|v| v.to_vec())));
            }
            // Insert the writes with individual transactions.
            for i in 1..=writes.keys().max().copied().unwrap_or(0) {
                let txn = self.mvcc.begin()?;
                for (key, value) in writes.get(&i).unwrap_or(&Vec::new()) {
                    if let Some(value) = value {
                        txn.set(key, value.clone())?;
                    } else {
                        txn.delete(key)?;
                    }
                }
                txn.commit()?;
            }
            // Flush the write log, but dump the engine contents.
            while self.mvcc.engine.lock()?.op_rx().try_recv().is_ok() {}
            self.print_engine()?;
            writeln!(&mut self.file.lock()?)?;
            Ok(())
        }

        fn begin(&mut self) -> Result<ScheduleTransaction> {
            self.new_txn("begin", self.mvcc.begin())
        }

        fn begin_read_only(&mut self) -> Result<ScheduleTransaction> {
            self.new_txn("begin read-only", self.mvcc.begin_read_only())
        }

        fn begin_as_of(&mut self, version: Version) -> Result<ScheduleTransaction> {
            self.new_txn(&format!("begin as of {}", version), self.mvcc.begin_as_of(version))
        }

        fn resume(&mut self, state: TransactionState) -> Result<ScheduleTransaction> {
            self.new_txn("resume", self.mvcc.resume(state))
        }

        /// Processes a begin/resume result.
        fn new_txn(
            &mut self,
            name: &str,
            result: Result<Transaction<Debug<Memory>>>,
        ) -> Result<ScheduleTransaction> {
            let id = self.next_id;
            self.next_id += 1;
            self.print_begin(id, name, &result)?;
            result.map(|txn| ScheduleTransaction { id, txn, file: self.file.clone() })
        }

        /// Prints a transaction begin to the golden file.
        fn print_begin(
            &mut self,
            id: u8,
            name: &str,
            result: &Result<Transaction<Debug<Memory>>>,
        ) -> Result<()> {
            let mut f = self.file.lock()?;
            write!(f, "T{}: {} → ", id, name)?;
            match result {
                Ok(txn) => writeln!(f, "{}", debug::format_txn(txn.state()))?,
                Err(err) => writeln!(f, "Error::{:?}", err)?,
            };
            Self::print_log(&mut f, &mut self.mvcc.engine.lock()?)?;
            writeln!(f)?;
            Ok(())
        }

        /// Prints the engine write log since the last call to the golden file.
        fn print_log(
            f: &mut MutexGuard<'_, std::fs::File>,
            engine: &mut MutexGuard<'_, Debug<Memory>>,
        ) -> Result<()> {
            while let Ok(op) = engine.op_rx().try_recv() {
                match op {
                    debug::Operation::Delete(key) => {
                        let (fkey, _) = debug::format_key_value(&key, &None);
                        writeln!(f, "    del {fkey}")
                    }
                    debug::Operation::Flush => writeln!(f, "    flush"),
                    debug::Operation::Set(key, value) => {
                        let (fkey, fvalue) = debug::format_key_value(&key, &Some(value));
                        writeln!(f, "    set {} = {}", fkey, fvalue.unwrap())
                    }
                }?;
            }
            Ok(())
        }

        /// Prints the engine contents to the golden file.
        fn print_engine(&self) -> Result<()> {
            let mut f = self.file.lock()?;
            let mut engine = self.mvcc.engine.lock()?;
            let mut scan = engine.scan(..);
            writeln!(f, "Engine state:")?;
            while let Some((key, value)) = scan.next().transpose()? {
                if let (fkey, Some(fvalue)) = debug::format_key_value(&key, &Some(value)) {
                    writeln!(f, "{} = {}", fkey, fvalue)?;
                }
            }
            Ok(())
        }

        fn get_unversioned(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            let value = self.mvcc.get_unversioned(key)?;
            write!(
                self.file.lock()?,
                "T_: get unversioned {} → {}\n\n",
                debug::format_raw(key),
                if let Some(ref value) = value {
                    debug::format_raw(value)
                } else {
                    String::from("None")
                }
            )?;
            Ok(value)
        }

        fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
            let mut f = self.file.lock()?;
            write!(
                f,
                "T_: set unversioned {} = {}",
                debug::format_raw(key),
                debug::format_raw(&value)
            )?;
            let result = self.mvcc.set_unversioned(key, value);
            match &result {
                Ok(_) => writeln!(f)?,
                Err(err) => writeln!(f, " → Error::{:?}", err)?,
            }
            Schedule::print_log(&mut f, &mut self.mvcc.engine.lock()?)?;
            writeln!(f)?;
            result
        }
    }

    impl Drop for Schedule {
        /// Print engine contents when the schedule is dropped.
        fn drop(&mut self) {
            _ = self.print_engine();
            _ = self.mint; // goldenfile assertions run when mint is dropped
        }
    }

    struct ScheduleTransaction {
        id: u8,
        txn: Transaction<Debug<Memory>>,
        file: Arc<Mutex<std::fs::File>>,
    }

    impl Clone for ScheduleTransaction {
        /// Allow cloning a schedule transaction, to simplify handling when
        /// commit/rollback consumes it. We don't want to allow this in general,
        /// since a commit/rollback will invalidate the cloned transactions.
        fn clone(&self) -> Self {
            let txn = Transaction { engine: self.txn.engine.clone(), st: self.txn.st.clone() };
            Self { id: self.id, txn, file: self.file.clone() }
        }
    }

    impl ScheduleTransaction {
        fn state(&self) -> TransactionState {
            self.txn.state().clone()
        }

        fn commit(self) -> Result<()> {
            let result = self.clone().txn.commit(); // clone to retain self.txn for printing
            self.print_mutation("commit", &result)?;
            result
        }

        fn rollback(self) -> Result<()> {
            let result = self.clone().txn.rollback(); // clone to retain self.txn for printing
            self.print_mutation("rollback", &result)?;
            result
        }

        fn delete(&self, key: &[u8]) -> Result<()> {
            let result = self.txn.delete(key);
            self.print_mutation(&format!("del {}", debug::format_raw(key)), &result)?;
            result
        }

        fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
            let result = self.txn.set(key, value.clone());
            self.print_mutation(
                &format!("set {} = {}", debug::format_raw(key), debug::format_raw(&value)),
                &result,
            )?;
            result
        }

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            let value = self.txn.get(key)?;
            write!(
                self.file.lock()?,
                "T{}: get {} → {}\n\n",
                self.id,
                debug::format_raw(key),
                if let Some(ref value) = value {
                    debug::format_raw(value)
                } else {
                    String::from("None")
                }
            )?;
            Ok(value)
        }

        fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Result<Scan<Debug<Memory>>> {
            let name = format!(
                "scan {}..{}",
                match range.start_bound() {
                    Bound::Excluded(k) => format!("({}", debug::format_raw(k)),
                    Bound::Included(k) => format!("[{}", debug::format_raw(k)),
                    Bound::Unbounded => "".to_string(),
                },
                match range.end_bound() {
                    Bound::Excluded(k) => format!("{})", debug::format_raw(k)),
                    Bound::Included(k) => format!("{}]", debug::format_raw(k)),
                    Bound::Unbounded => "".to_string(),
                },
            );
            let mut scan = self.txn.scan(range)?;
            self.print_scan(&name, scan.to_vec()?)?;
            Ok(scan)
        }

        fn scan_prefix(&self, prefix: &[u8]) -> Result<Scan<Debug<Memory>>> {
            let mut scan = self.txn.scan_prefix(prefix)?;
            self.print_scan(&format!("scan prefix {}", debug::format_raw(prefix)), scan.to_vec()?)?;
            Ok(scan)
        }

        /// Prints the result of a mutation to the golden file.
        fn print_mutation(&self, name: &str, result: &Result<()>) -> Result<()> {
            let mut f = self.file.lock()?;
            write!(f, "T{}: {}", self.id, name)?;
            match result {
                Ok(_) => writeln!(f)?,
                Err(err) => writeln!(f, " → Error::{:?}", err)?,
            }
            Schedule::print_log(&mut f, &mut self.txn.engine.lock()?)?;
            writeln!(f)?;
            Ok(())
        }

        /// Prints the results of a scan to the golden file.
        fn print_scan(&self, name: &str, scan: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
            let mut f = self.file.lock()?;
            writeln!(f, "T{}: {}", self.id, name)?;
            for (key, value) in scan {
                writeln!(f, "    {} = {}", debug::format_raw(&key), debug::format_raw(&value))?
            }
            writeln!(f)?;
            Ok(())
        }
    }

    /// Asserts that a scan yields the expected result.
    macro_rules! assert_scan {
        ( $scan:expr => { $( $key:expr => $value:expr),* $(,)? } ) => {
            let result = $scan.to_vec()?;
            let expect = vec![
                $( ($key.to_vec(), $value.to_vec()), )*
            ];
            assert_eq!(result, expect);
        };
    }

    // Asserts scan invariants.
    #[track_caller]
    fn assert_scan_invariants(scan: &mut Scan<Debug<Memory>>) -> Result<()> {
        // Iterator and vec should yield same results.
        let result = scan.to_vec()?;
        assert_eq!(scan.iter().collect::<Result<Vec<_>>>()?, result);

        // Forward and reverse scans should give the same results.
        let mut forward = result.clone();
        forward.reverse();
        let reverse = scan.iter().rev().collect::<Result<Vec<_>>>()?;
        assert_eq!(reverse, forward);

        // Alternating next/next_back calls should give the same results.
        let mut forward = Vec::new();
        let mut reverse = Vec::new();
        let mut iter = scan.iter();
        while let Some(b) = iter.next().transpose()? {
            forward.push(b);
            if let Some(b) = iter.next_back().transpose()? {
                reverse.push(b);
            }
        }
        reverse.reverse();
        forward.extend_from_slice(&reverse);
        assert_eq!(forward, result);

        Ok(())
    }

    #[test]
    /// Tests that key prefixes are actually prefixes of keys.
    fn key_prefix() -> Result<()> {
        let cases = vec![
            (KeyPrefix::NextVersion, Key::NextVersion),
            (KeyPrefix::TxnActive, Key::TxnActive(1)),
            (KeyPrefix::TxnActiveSnapshot, Key::TxnActiveSnapshot(1)),
            (KeyPrefix::TxnWrite(1), Key::TxnWrite(1, b"foo".as_slice().into())),
            (
                KeyPrefix::Version(b"foo".as_slice().into()),
                Key::Version(b"foo".as_slice().into(), 1),
            ),
            (KeyPrefix::Unversioned, Key::Unversioned(b"foo".as_slice().into())),
        ];

        for (prefix, key) in cases {
            let prefix = prefix.encode()?;
            let key = key.encode()?;
            assert_eq!(prefix, key[..prefix.len()])
        }
        Ok(())
    }

    #[test]
    /// Begin should create txns with new versions and current active sets.
    fn begin() -> Result<()> {
        let mut mvcc = Schedule::new("begin")?;

        let t1 = mvcc.begin()?;
        assert_eq!(
            t1.state(),
            TransactionState { version: 1, read_only: false, active: HashSet::new() }
        );

        let t2 = mvcc.begin()?;
        assert_eq!(
            t2.state(),
            TransactionState { version: 2, read_only: false, active: HashSet::from([1]) }
        );

        let t3 = mvcc.begin()?;
        assert_eq!(
            t3.state(),
            TransactionState { version: 3, read_only: false, active: HashSet::from([1, 2]) }
        );

        t2.commit()?; // commit to remove from active set

        let t4 = mvcc.begin()?;
        assert_eq!(
            t4.state(),
            TransactionState { version: 4, read_only: false, active: HashSet::from([1, 3]) }
        );

        Ok(())
    }

    #[test]
    /// Begin read-only should not create a new version, instead using the
    /// next one, but it should use the current active set.
    fn begin_read_only() -> Result<()> {
        let mut mvcc = Schedule::new("begin_read_only")?;

        // Start an initial read-only transaction, and make sure it's actually
        // read-only.
        let t1 = mvcc.begin_read_only()?;
        assert_eq!(
            t1.state(),
            TransactionState { version: 1, read_only: true, active: HashSet::new() }
        );
        assert_eq!(t1.set(b"foo", vec![1]), Err(Error::ReadOnly));
        assert_eq!(t1.delete(b"foo"), Err(Error::ReadOnly));

        // Start a new read-write transaction, then another read-only
        // transaction which should have it in its active set. t1 should not be
        // in the active set, because it's read-only.
        let t2 = mvcc.begin()?;
        assert_eq!(
            t2.state(),
            TransactionState { version: 1, read_only: false, active: HashSet::new() }
        );

        let t3 = mvcc.begin_read_only()?;
        assert_eq!(
            t3.state(),
            TransactionState { version: 2, read_only: true, active: HashSet::from([1]) }
        );

        Ok(())
    }

    #[test]
    /// Begin as of should provide a read-only view of a historical version.
    fn begin_as_of() -> Result<()> {
        let mut mvcc = Schedule::new("begin_as_of")?;

        // Start a concurrent transaction that should be invisible.
        let t1 = mvcc.begin()?;
        t1.set(b"other", vec![1])?;

        // Write a couple of versions for a key.
        let t2 = mvcc.begin()?;
        t2.set(b"key", vec![2])?;
        t2.commit()?;

        let t3 = mvcc.begin()?;
        t3.set(b"key", vec![3])?;

        // Reading as of version 3 should only see key=2, because t1 and
        // t3 haven't committed yet.
        let t4 = mvcc.begin_as_of(3)?;
        assert_eq!(
            t4.state(),
            TransactionState { version: 3, read_only: true, active: HashSet::from([1]) }
        );
        assert_scan!(t4.scan(..)? => {b"key" => [2]});

        // Writes should error.
        assert_eq!(t4.set(b"foo", vec![1]), Err(Error::ReadOnly));
        assert_eq!(t4.delete(b"foo"), Err(Error::ReadOnly));

        // Once we commit t1 and t3, neither the existing as of transaction nor
        // a new one should see their writes, since versions must be stable.
        t1.commit()?;
        t3.commit()?;

        assert_scan!(t4.scan(..)? => {b"key" => [2]});

        let t5 = mvcc.begin_as_of(3)?;
        assert_scan!(t5.scan(..)? => {b"key" => [2]});

        // Rolling back and committing read-only transactions is noops.
        t4.rollback()?;
        t5.commit()?;

        // Commit a new value.
        let t6 = mvcc.begin()?;
        t6.set(b"key", vec![4])?;
        t6.commit()?;

        // A snapshot as of version 4 should see key=3 and other=1.
        let t7 = mvcc.begin_as_of(4)?;
        assert_eq!(
            t7.state(),
            TransactionState { version: 4, read_only: true, active: HashSet::new() }
        );
        assert_scan!(t7.scan(..)? => {b"key" => [3], b"other" => [1]});

        // Check that future versions are invalid, including the next.
        assert_eq!(
            mvcc.begin_as_of(5).err(),
            Some(Error::Value("Version 5 does not exist".into()))
        );
        assert_eq!(
            mvcc.begin_as_of(9).err(),
            Some(Error::Value("Version 9 does not exist".into()))
        );

        Ok(())
    }

    #[test]
    /// Resume should resume a transaction with the same state.
    fn resume() -> Result<()> {
        let mut mvcc = Schedule::new("resume")?;

        // We first write a set of values that should be visible.
        let t1 = mvcc.begin()?;
        t1.set(b"a", vec![1])?;
        t1.set(b"b", vec![1])?;
        t1.commit()?;

        // We then start three transactions, of which we will resume t3.
        // We commit t2 and t4's changes, which should not be visible,
        // and write a change for t3 which should be visible.
        let t2 = mvcc.begin()?;
        let t3 = mvcc.begin()?;
        let t4 = mvcc.begin()?;

        t2.set(b"a", vec![2])?;
        t3.set(b"b", vec![3])?;
        t4.set(b"c", vec![4])?;

        t2.commit()?;
        t4.commit()?;

        // We now resume t3, who should only see its own changes.
        let state = t3.state().clone();
        assert_eq!(
            state,
            TransactionState { version: 3, read_only: false, active: HashSet::from([2]) }
        );
        drop(t3);

        let t5 = mvcc.resume(state.clone())?;
        assert_eq!(t5.state(), state);

        assert_scan!(t5.scan(..)? => {
            b"a" => [1],
            b"b" => [3],
        });

        // A separate transaction should not see t3's changes.
        let t6 = mvcc.begin()?;
        assert_scan!(t6.scan(..)? => {
            b"a" => [2],
            b"b" => [1], // not 3
            b"c" => [4],
        });
        t6.rollback()?;

        // Once t5 commits, a separate transaction should see its changes.
        t5.commit()?;

        let t7 = mvcc.begin()?;
        assert_scan!(t7.scan(..)? => {
            b"a" => [2],
            b"b" => [3], // now 3
            b"c" => [4],
        });
        t7.rollback()?;

        // Resuming an inactive transaction should error.
        assert_eq!(
            mvcc.resume(state).err(),
            Some(Error::Internal("No active transaction at version 3".into()))
        );

        // It should also be possible to start a snapshot transaction in t3
        // and resume it. It should not see t3's writes, nor t2's.
        let t8 = mvcc.begin_as_of(3)?;
        assert_eq!(
            t8.state(),
            TransactionState { version: 3, read_only: true, active: HashSet::from([2]) }
        );

        assert_scan!(t8.scan(..)? => {
            b"a" => [1],
            b"b" => [1],
        });

        let state = t8.state().clone();
        drop(t8);

        let t9 = mvcc.resume(state.clone())?;
        assert_eq!(t9.state(), state);
        assert_scan!(t9.scan(..)? => {
            b"a" => [1],
            b"b" => [1],
        });

        Ok(())
    }

    #[test]
    /// Deletes should work on both existing, missing, and deleted keys, be
    /// idempotent.
    fn delete() -> Result<()> {
        let mut mvcc = Schedule::new("delete")?;
        mvcc.setup(vec![(b"key", 1, Some(&[1])), (b"tombstone", 1, None)])?;

        let t1 = mvcc.begin()?;
        t1.set(b"key", vec![2])?;
        t1.delete(b"key")?; // delete uncommitted version
        t1.delete(b"key")?; // idempotent
        t1.delete(b"tombstone")?;
        t1.delete(b"missing")?;
        t1.commit()?;

        Ok(())
    }

    #[test]
    /// Delete should return serialization errors both for uncommitted versions
    /// (past and future), and future committed versions.
    fn delete_conflict() -> Result<()> {
        let mut mvcc = Schedule::new("delete_conflict")?;

        let t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;
        let t3 = mvcc.begin()?;
        let t4 = mvcc.begin()?;

        t1.set(b"a", vec![1])?;
        t3.set(b"c", vec![3])?;
        t4.set(b"d", vec![4])?;
        t4.commit()?;

        assert_eq!(t2.delete(b"a"), Err(Error::Serialization)); // past uncommitted
        assert_eq!(t2.delete(b"c"), Err(Error::Serialization)); // future uncommitted
        assert_eq!(t2.delete(b"d"), Err(Error::Serialization)); // future committed

        Ok(())
    }

    #[test]
    /// Get should return the correct latest value.
    fn get() -> Result<()> {
        let mut mvcc = Schedule::new("get")?;
        mvcc.setup(vec![
            (b"key", 1, Some(&[1])),
            (b"updated", 1, Some(&[1])),
            (b"updated", 2, Some(&[2])),
            (b"deleted", 1, Some(&[1])),
            (b"deleted", 2, None),
            (b"tombstone", 1, None),
        ])?;

        let t1 = mvcc.begin_read_only()?;
        assert_eq!(t1.get(b"key")?, Some(vec![1]));
        assert_eq!(t1.get(b"updated")?, Some(vec![2]));
        assert_eq!(t1.get(b"deleted")?, None);
        assert_eq!(t1.get(b"tombstone")?, None);

        Ok(())
    }

    #[test]
    /// Get should be isolated from future and uncommitted transactions.
    fn get_isolation() -> Result<()> {
        let mut mvcc = Schedule::new("get_isolation")?;

        let t1 = mvcc.begin()?;
        t1.set(b"a", vec![1])?;
        t1.set(b"b", vec![1])?;
        t1.set(b"d", vec![1])?;
        t1.set(b"e", vec![1])?;
        t1.commit()?;

        let t2 = mvcc.begin()?;
        t2.set(b"a", vec![2])?;
        t2.delete(b"b")?;
        t2.set(b"c", vec![2])?;

        let t3 = mvcc.begin_read_only()?;

        let t4 = mvcc.begin()?;
        t4.set(b"d", vec![3])?;
        t4.delete(b"e")?;
        t4.set(b"f", vec![3])?;
        t4.commit()?;

        assert_eq!(t3.get(b"a")?, Some(vec![1])); // uncommitted update
        assert_eq!(t3.get(b"b")?, Some(vec![1])); // uncommitted delete
        assert_eq!(t3.get(b"c")?, None); // uncommitted write
        assert_eq!(t3.get(b"d")?, Some(vec![1])); // future update
        assert_eq!(t3.get(b"e")?, Some(vec![1])); // future delete
        assert_eq!(t3.get(b"f")?, None); // future write

        Ok(())
    }

    #[test]
    /// Scans should use correct key and time bounds. Sets up an initial data
    /// set as follows, and asserts results via the golden file.
    ///
    /// T
    /// 4             x   ba,4
    /// 3   x   a,3  b,3        x
    /// 2        x        ba,2 bb,2 bc,2
    /// 1  0,1  a,1   x                  c,1
    ///     B    a    b    ba   bb   bc   c
    fn scan() -> Result<()> {
        let mut mvcc = Schedule::new("scan")?;
        mvcc.setup(vec![
            (b"B", 1, Some(&[0, 1])),
            (b"B", 3, None),
            (b"a", 1, Some(&[0x0a, 1])),
            (b"a", 2, None),
            (b"a", 3, Some(&[0x0a, 3])),
            (b"b", 1, None),
            (b"b", 3, Some(&[0x0b, 3])),
            (b"b", 4, None),
            (b"ba", 2, Some(&[0xba, 2])),
            (b"ba", 4, Some(&[0xba, 4])),
            (b"bb", 2, Some(&[0xbb, 2])),
            (b"bb", 3, None),
            (b"bc", 2, Some(&[0xbc, 2])),
            (b"c", 1, Some(&[0x0c, 1])),
        ])?;

        // Full scans at all timestamps.
        for version in 1..5 {
            let txn = match version {
                5 => mvcc.begin_read_only()?,
                v => mvcc.begin_as_of(v)?,
            };
            let mut scan = txn.scan(..)?; // see golden master
            assert_scan_invariants(&mut scan)?;
        }

        // All bounded scans around ba-bc at version 3.
        let txn = mvcc.begin_as_of(3)?;
        let starts =
            [Bound::Unbounded, Bound::Included(b"ba".to_vec()), Bound::Excluded(b"ba".to_vec())];
        let ends =
            [Bound::Unbounded, Bound::Included(b"bc".to_vec()), Bound::Excluded(b"bc".to_vec())];
        for start in &starts {
            for end in &ends {
                let mut scan = txn.scan((start.to_owned(), end.to_owned()))?; // see golden master
                assert_scan_invariants(&mut scan)?;
            }
        }

        Ok(())
    }

    #[test]
    /// Prefix scans should use correct key and time bounds. Sets up an initial
    /// data set as follows, and asserts results via the golden file.
    ///
    /// T
    /// 4             x   ba,4
    /// 3   x   a,3  b,3        x
    /// 2        x        ba,2 bb,2 bc,2
    /// 1  0,1  a,1   x                  c,1
    ///     B    a    b    ba   bb   bc   c
    fn scan_prefix() -> Result<()> {
        let mut mvcc = Schedule::new("scan_prefix")?;
        mvcc.setup(vec![
            (b"B", 1, Some(&[0, 1])),
            (b"B", 3, None),
            (b"a", 1, Some(&[0x0a, 1])),
            (b"a", 2, None),
            (b"a", 3, Some(&[0x0a, 3])),
            (b"b", 1, None),
            (b"b", 3, Some(&[0x0b, 3])),
            (b"b", 4, None),
            (b"ba", 2, Some(&[0xba, 2])),
            (b"ba", 4, Some(&[0xba, 4])),
            (b"bb", 2, Some(&[0xbb, 2])),
            (b"bb", 3, None),
            (b"bc", 2, Some(&[0xbc, 2])),
            (b"c", 1, Some(&[0x0c, 1])),
        ])?;

        // Full scans at all timestamps.
        for version in 1..5 {
            let txn = match version {
                5 => mvcc.begin_read_only()?,
                v => mvcc.begin_as_of(v)?,
            };
            let mut scan = txn.scan_prefix(&[])?; // see golden master
            assert_scan_invariants(&mut scan)?;
        }

        // All prefixes at version 3 and version 4.
        for version in 3..=4 {
            let txn = mvcc.begin_as_of(version)?;
            for prefix in [b"B" as &[u8], b"a", b"b", b"ba", b"bb", b"bbb", b"bc", b"c", b"d"] {
                let mut scan = txn.scan_prefix(prefix)?; // see golden master
                assert_scan_invariants(&mut scan)?;
            }
        }

        Ok(())
    }

    #[test]
    /// Scan should be isolated from future and uncommitted transactions.
    fn scan_isolation() -> Result<()> {
        let mut mvcc = Schedule::new("scan_isolation")?;

        let t1 = mvcc.begin()?;
        t1.set(b"a", vec![1])?;
        t1.set(b"b", vec![1])?;
        t1.set(b"d", vec![1])?;
        t1.set(b"e", vec![1])?;
        t1.commit()?;

        let t2 = mvcc.begin()?;
        t2.set(b"a", vec![2])?;
        t2.delete(b"b")?;
        t2.set(b"c", vec![2])?;

        let t3 = mvcc.begin_read_only()?;

        let t4 = mvcc.begin()?;
        t4.set(b"d", vec![3])?;
        t4.delete(b"e")?;
        t4.set(b"f", vec![3])?;
        t4.commit()?;

        assert_scan!(t3.scan(..)? => {
            b"a" => [1], // uncommitted update
            b"b" => [1], // uncommitted delete
            // b"c" is uncommitted write
            b"d" => [1], // future update
            b"e" => [1], // future delete
            // b"f" is future write
        });

        Ok(())
    }

    #[test]
    /// Tests that the key encoding is resistant to key/version overlap.
    /// For example, a naïve concatenation of keys and versions would
    /// produce incorrect ordering in this case:
    ///
    // 00|00 00 00 00 00 00 00 01
    // 00 00 00 00 00 00 00 00 02|00 00 00 00 00 00 00 02
    // 00|00 00 00 00 00 00 00 03
    fn scan_key_version_encoding() -> Result<()> {
        let mut mvcc = Schedule::new("scan_key_version_encoding")?;

        let t1 = mvcc.begin()?;
        t1.set(&[0], vec![1])?;
        t1.commit()?;

        let t2 = mvcc.begin()?;
        t2.set(&[0], vec![2])?;
        t2.set(&[0, 0, 0, 0, 0, 0, 0, 0, 2], vec![2])?;
        t2.commit()?;

        let t3 = mvcc.begin()?;
        t3.set(&[0], vec![3])?;
        t3.commit()?;

        let t4 = mvcc.begin_read_only()?;
        assert_scan!(t4.scan(..)? => {
            b"\x00" => [3],
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x02" => [2],
        });
        Ok(())
    }

    #[test]
    /// Sets should work on both existing, missing, and deleted keys, and be
    /// idempotent.
    fn set() -> Result<()> {
        let mut mvcc = Schedule::new("set")?;
        mvcc.setup(vec![(b"key", 1, Some(&[1])), (b"tombstone", 1, None)])?;

        let t1 = mvcc.begin()?;
        t1.set(b"key", vec![2])?; // update
        t1.set(b"tombstone", vec![2])?; // update tombstone
        t1.set(b"new", vec![1])?; // new write
        t1.set(b"new", vec![1])?; // idempotent
        t1.set(b"new", vec![2])?; // update own
        t1.commit()?;

        Ok(())
    }

    #[test]
    /// Set should return serialization errors both for uncommitted versions
    /// (past and future), and future committed versions.
    fn set_conflict() -> Result<()> {
        let mut mvcc = Schedule::new("set_conflict")?;

        let t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;
        let t3 = mvcc.begin()?;
        let t4 = mvcc.begin()?;

        t1.set(b"a", vec![1])?;
        t3.set(b"c", vec![3])?;
        t4.set(b"d", vec![4])?;
        t4.commit()?;

        assert_eq!(t2.set(b"a", vec![2]), Err(Error::Serialization)); // past uncommitted
        assert_eq!(t2.set(b"c", vec![2]), Err(Error::Serialization)); // future uncommitted
        assert_eq!(t2.set(b"d", vec![2]), Err(Error::Serialization)); // future committed

        Ok(())
    }

    #[test]
    /// Tests that transaction rollback properly rolls back uncommitted writes,
    /// allowing other concurrent transactions to write the keys.
    fn rollback() -> Result<()> {
        let mut mvcc = Schedule::new("rollback")?;
        mvcc.setup(vec![
            (b"a", 1, Some(&[0])),
            (b"b", 1, Some(&[0])),
            (b"c", 1, Some(&[0])),
            (b"d", 1, Some(&[0])),
        ])?;

        // t2 will be rolled back. t1 and t3 are concurrent transactions.
        let t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;
        let t3 = mvcc.begin()?;

        t1.set(b"a", vec![1])?;
        t2.set(b"b", vec![2])?;
        t2.delete(b"c")?;
        t3.set(b"d", vec![3])?;

        // Both t1 and t3 will get serialization errors with t2.
        assert_eq!(t1.set(b"b", vec![1]), Err(Error::Serialization));
        assert_eq!(t3.set(b"c", vec![3]), Err(Error::Serialization));

        // When t2 is rolled back, none of its writes will be visible, and t1
        // and t3 can perform their writes and successfully commit.
        t2.rollback()?;

        let t4 = mvcc.begin_read_only()?;
        assert_scan!(t4.scan(..)? => {
            b"a" => [0],
            b"b" => [0],
            b"c" => [0],
            b"d" => [0],
        });

        t1.set(b"b", vec![1])?;
        t3.set(b"c", vec![3])?;
        t1.commit()?;
        t3.commit()?;

        let t5 = mvcc.begin_read_only()?;
        assert_scan!(t5.scan(..)? => {
            b"a" => [1],
            b"b" => [1],
            b"c" => [3],
            b"d" => [3],
        });

        Ok(())
    }

    #[test]
    // A dirty write is when t2 overwrites an uncommitted value written by t1.
    // Snapshot isolation prevents this.
    fn anomaly_dirty_write() -> Result<()> {
        let mut mvcc = Schedule::new("anomaly_dirty_write")?;

        let t1 = mvcc.begin()?;
        t1.set(b"key", vec![1])?;

        let t2 = mvcc.begin()?;
        assert_eq!(t2.set(b"key", vec![2]), Err(Error::Serialization));

        Ok(())
    }

    #[test]
    // A dirty read is when t2 can read an uncommitted value set by t1.
    // Snapshot isolation prevents this.
    fn anomaly_dirty_read() -> Result<()> {
        let mut mvcc = Schedule::new("anomaly_dirty_read")?;

        let t1 = mvcc.begin()?;
        t1.set(b"key", vec![1])?;

        let t2 = mvcc.begin()?;
        assert_eq!(t2.get(b"key")?, None);

        Ok(())
    }

    #[test]
    // A lost update is when t1 and t2 both read a value and update it, where
    // t2's update replaces t1. Snapshot isolation prevents this.
    fn anomaly_lost_update() -> Result<()> {
        let mut mvcc = Schedule::new("anomaly_lost_update")?;
        mvcc.setup(vec![(b"key", 1, Some(&[0]))])?;

        let t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;

        t1.get(b"key")?;
        t2.get(b"key")?;

        t1.set(b"key", vec![1])?;
        assert_eq!(t2.set(b"key", vec![2]), Err(Error::Serialization));
        t1.commit()?;

        Ok(())
    }

    #[test]
    // A fuzzy (or unrepeatable) read is when t2 sees a value change after t1
    // updates it. Snapshot isolation prevents this.
    fn anomaly_fuzzy_read() -> Result<()> {
        let mut mvcc = Schedule::new("anomaly_fuzzy_read")?;
        mvcc.setup(vec![(b"key", 1, Some(&[0]))])?;

        let t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;

        assert_eq!(t2.get(b"key")?, Some(vec![0]));
        t1.set(b"key", b"t1".to_vec())?;
        t1.commit()?;
        assert_eq!(t2.get(b"key")?, Some(vec![0]));

        Ok(())
    }

    #[test]
    // Read skew is when t1 reads a and b, but t2 modifies b in between the
    // reads. Snapshot isolation prevents this.
    fn anomaly_read_skew() -> Result<()> {
        let mut mvcc = Schedule::new("anomaly_read_skew")?;
        mvcc.setup(vec![(b"a", 1, Some(&[0])), (b"b", 1, Some(&[0]))])?;

        let t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;

        assert_eq!(t1.get(b"a")?, Some(vec![0]));
        t2.set(b"a", vec![2])?;
        t2.set(b"b", vec![2])?;
        t2.commit()?;
        assert_eq!(t1.get(b"b")?, Some(vec![0]));

        Ok(())
    }

    #[test]
    // A phantom read is when t1 reads entries matching some predicate, but a
    // modification by t2 changes which entries that match the predicate such
    // that a later read by t1 returns them. Snapshot isolation prevents this.
    //
    // We use a prefix scan as our predicate.
    fn anomaly_phantom_read() -> Result<()> {
        let mut mvcc = Schedule::new("anomaly_phantom_read")?;
        mvcc.setup(vec![(b"a", 1, Some(&[0])), (b"ba", 1, Some(&[0])), (b"bb", 1, Some(&[0]))])?;

        let t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;

        assert_scan!(t1.scan_prefix(b"b")? => {
            b"ba" => [0],
            b"bb" => [0],
        });

        t2.delete(b"ba")?;
        t2.set(b"bc", vec![2])?;
        t2.commit()?;

        assert_scan!(t1.scan_prefix(b"b")? => {
            b"ba" => [0],
            b"bb" => [0],
        });

        Ok(())
    }

    #[test]
    // Write skew is when t1 reads a and writes it to b while t2 reads b and
    // writes it to a. Snapshot isolation DOES NOT prevent this, which is
    // expected, so we assert the current behavior. Fixing this requires
    // implementing serializable snapshot isolation.
    fn anomaly_write_skew() -> Result<()> {
        let mut mvcc = Schedule::new("anomaly_write_skew")?;
        mvcc.setup(vec![(b"a", 1, Some(&[1])), (b"b", 1, Some(&[2]))])?;

        let t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;

        assert_eq!(t1.get(b"a")?, Some(vec![1]));
        assert_eq!(t2.get(b"b")?, Some(vec![2]));

        t1.set(b"b", vec![1])?;
        t2.set(b"a", vec![2])?;

        t1.commit()?;
        t2.commit()?;

        Ok(())
    }

    #[test]
    /// Tests unversioned key/value pairs, via set/get_unversioned().
    fn unversioned() -> Result<()> {
        let mut mvcc = Schedule::new("unversioned")?;

        // Interleave versioned and unversioned writes.
        mvcc.set_unversioned(b"a", vec![0])?;

        let t1 = mvcc.begin()?;
        t1.set(b"a", vec![1])?;
        t1.set(b"b", vec![1])?;
        t1.set(b"c", vec![1])?;
        t1.commit()?;

        mvcc.set_unversioned(b"b", vec![0])?;
        mvcc.set_unversioned(b"d", vec![0])?;

        // Scans should not see the unversioned writes.
        let t2 = mvcc.begin_read_only()?;
        assert_scan!(t2.scan(..)? => {
            b"a" => [1],
            b"b" => [1],
            b"c" => [1],
        });

        // Unversioned gets should not see MVCC writes.
        assert_eq!(mvcc.get_unversioned(b"a")?, Some(vec![0]));
        assert_eq!(mvcc.get_unversioned(b"b")?, Some(vec![0]));
        assert_eq!(mvcc.get_unversioned(b"c")?, None);
        assert_eq!(mvcc.get_unversioned(b"d")?, Some(vec![0]));

        // Replacing an unversioned key should be fine.
        mvcc.set_unversioned(b"a", vec![1])?;
        assert_eq!(mvcc.get_unversioned(b"a")?, Some(vec![1]));

        Ok(())
    }
}
