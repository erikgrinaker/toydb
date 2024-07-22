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

use super::engine::{self, Engine};
use crate::encoding::{self, bincode, keycode, Key as _, Value as _};
use crate::error::{Error, Result};
use crate::{errdata, errinput};

use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeSet, VecDeque};
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, MutexGuard};

/// An MVCC version represents a logical timestamp. The latest version
/// is incremented when beginning each read-write transaction.
pub type Version = u64;

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
    pub engine: Arc<Mutex<E>>,
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
        self.engine.lock()?.get(&Key::Unversioned(key.into()).encode())
    }

    /// Sets the value of an unversioned key.
    pub fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.engine.lock()?.set(&Key::Unversioned(key.into()).encode(), value)
    }

    /// Returns the status of the MVCC and storage engines.
    pub fn status(&self) -> Result<Status> {
        let mut engine = self.engine.lock()?;
        let versions = match engine.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)? - 1,
            None => 0,
        };
        let active_txns = engine.scan_prefix(&KeyPrefix::TxnActive.encode()).count() as u64;
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
    /// not committed yet. Uses a BTreeSet for test determinism.
    pub active: BTreeSet<Version>,
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

impl From<TransactionState> for Cow<'_, TransactionState> {
    fn from(txn: TransactionState) -> Self {
        Cow::Owned(txn)
    }
}

impl<'a> From<&'a TransactionState> for Cow<'a, TransactionState> {
    fn from(txn: &'a TransactionState) -> Self {
        Cow::Borrowed(txn)
    }
}

impl<E: Engine> Transaction<E> {
    /// Begins a new transaction in read-write mode. This will allocate a new
    /// version that the transaction can write at, add it to the active set, and
    /// record its active snapshot for time-travel queries.
    fn begin(engine: Arc<Mutex<E>>) -> Result<Self> {
        let mut session = engine.lock()?;

        // Allocate a new version to write at.
        let version = match session.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)?,
            None => 1,
        };
        session.set(&Key::NextVersion.encode(), (version + 1).encode())?;

        // Fetch the current set of active transactions, persist it for
        // time-travel queries if non-empty, then add this txn to it.
        let active = Self::scan_active(&mut session)?;
        if !active.is_empty() {
            session.set(&Key::TxnActiveSnapshot(version).encode(), active.encode())?
        }
        session.set(&Key::TxnActive(version).encode(), vec![])?;
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
        let mut version = match session.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)?,
            None => 1,
        };

        // If requested, create the transaction as of a past version, restoring
        // the active snapshot as of the beginning of that version. Otherwise,
        // use the latest version and get the current, real-time snapshot.
        let mut active = BTreeSet::new();
        if let Some(as_of) = as_of {
            if as_of >= version {
                return errinput!("version {as_of} does not exist");
            }
            version = as_of;
            if let Some(value) = session.get(&Key::TxnActiveSnapshot(version).encode())? {
                active = BTreeSet::<Version>::decode(&value)?;
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
        if !s.read_only && engine.lock()?.get(&Key::TxnActive(s.version).encode())?.is_none() {
            return errinput!("no active transaction at version {}", s.version);
        }
        Ok(Self { engine, st: s })
    }

    /// Fetches the set of currently active transactions.
    fn scan_active(session: &mut MutexGuard<E>) -> Result<BTreeSet<Version>> {
        let mut active = BTreeSet::new();
        let mut scan = session.scan_prefix(&KeyPrefix::TxnActive.encode());
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnActive(version) => active.insert(version),
                key => return errdata!("expected TxnActive key, got {key:?}"),
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
        let mut engine = self.engine.lock()?;
        let remove: Vec<_> = engine
            .scan_prefix(&KeyPrefix::TxnWrite(self.st.version).encode())
            .map_ok(|(k, _)| k)
            .try_collect()?;
        for key in remove {
            engine.delete(&key)?
        }
        engine.delete(&Key::TxnActive(self.st.version).encode())
    }

    /// Rolls back the transaction, by undoing all written versions and removing
    /// it from the active set. The active set snapshot is left behind, since
    /// this is needed for time travel queries at this version.
    pub fn rollback(self) -> Result<()> {
        if self.st.read_only {
            return Ok(());
        }
        let mut engine = self.engine.lock()?;
        let mut rollback = Vec::new();
        let mut scan = engine.scan_prefix(&KeyPrefix::TxnWrite(self.st.version).encode());
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnWrite(_, key) => {
                    rollback.push(Key::Version(key, self.st.version).encode()) // the version
                }
                key => return errdata!("expected TxnWrite, got {key:?}"),
            };
            rollback.push(key); // the TxnWrite record
        }
        drop(scan);
        for key in rollback.into_iter() {
            engine.delete(&key)?;
        }
        engine.delete(&Key::TxnActive(self.st.version).encode()) // remove from active set
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
        let mut engine = self.engine.lock()?;

        // Check for write conflicts, i.e. if the latest key is invisible to us
        // (either a newer version, or an uncommitted version in our past). We
        // can only conflict with the latest key, since all transactions enforce
        // the same invariant.
        let from = Key::Version(
            key.into(),
            self.st.active.iter().min().copied().unwrap_or(self.st.version + 1),
        )
        .encode();
        let to = Key::Version(key.into(), u64::MAX).encode();
        if let Some((key, _)) = engine.scan(from..=to).last().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if !self.st.is_visible(version) {
                        return Err(Error::Serialization);
                    }
                }
                key => return errdata!("expected Key::Version got {key:?}"),
            }
        }

        // Write the new version and its write record.
        //
        // NB: TxnWrite contains the provided user key, not the encoded engine
        // key, since we can construct the engine key using the version.
        engine.set(&Key::TxnWrite(self.st.version, key.into()).encode(), vec![])?;
        engine.set(&Key::Version(key.into(), self.st.version).encode(), bincode::serialize(&value))
    }

    /// Fetches a key's value, or None if it does not exist.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut engine = self.engine.lock()?;
        let from = Key::Version(key.into(), 0).encode();
        let to = Key::Version(key.into(), self.st.version).encode();
        let mut scan = engine.scan(from..=to).rev();
        while let Some((key, value)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if self.st.is_visible(version) {
                        return bincode::deserialize(&value);
                    }
                }
                key => return errdata!("expected Key::Version got {key:?}"),
            };
        }
        Ok(None)
    }

    /// Returns an iterator over the latest visible key/value pairs at the
    /// transaction's version.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> ScanIterator<E> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.into(), u64::MAX).encode()),
            Bound::Included(k) => Bound::Included(Key::Version(k.into(), 0).encode()),
            Bound::Unbounded => Bound::Included(Key::Version(vec![].into(), 0).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.into(), 0).encode()),
            Bound::Included(k) => Bound::Included(Key::Version(k.into(), u64::MAX).encode()),
            Bound::Unbounded => Bound::Excluded(KeyPrefix::Unversioned.encode()),
        };
        ScanIterator::new(self.engine.clone(), self.state().clone(), (start, end))
    }

    /// Scans keys under a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> ScanIterator<E> {
        // Normally, KeyPrefix::Version will only match all versions of the
        // exact given key. We want all keys maching the prefix, so we chop off
        // the KeyCode byte slice terminator 0x0000 at the end.
        let mut prefix = KeyPrefix::Version(prefix.into()).encode();
        prefix.truncate(prefix.len() - 2);
        let range = keycode::prefix_range(&prefix);
        ScanIterator::new(self.engine.clone(), self.state().clone(), range)
    }
}

/// An iterator over the latest live and visible key/value pairs for the txn.
///
/// The (single-threaded) engine is protected by a mutex, and holding the mutex
/// for the duration of the iteration can cause deadlocks (e.g. when the local
/// SQL engine pulls from two tables concurrently during a join). Instead, we
/// pull and buffer a batch of rows at a time, and release the mutex in between.
///
/// This does not implement DoubleEndedIterator (reverse scans), since the SQL
/// layer doesn't currently need it.
#[allow(clippy::type_complexity)]
pub struct ScanIterator<E: Engine> {
    /// The engine.
    engine: Arc<Mutex<E>>,
    /// The transaction state.
    txn: TransactionState,
    /// A buffer of live and visible key/value pairs to emit.
    buffer: VecDeque<(Vec<u8>, Vec<u8>)>,
    /// The remaining range after the buffer.
    remainder: Option<(Bound<Vec<u8>>, Bound<Vec<u8>>)>,
}

/// Implement Clone manually. Deriving it requires Engine: Clone.
impl<E: Engine> Clone for ScanIterator<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            txn: self.txn.clone(),
            buffer: self.buffer.clone(),
            remainder: self.remainder.clone(),
        }
    }
}

impl<E: Engine> ScanIterator<E> {
    /// The number of live keys to pull from the engine at a time.
    #[cfg(not(test))]
    const BUFFER_SIZE: usize = 1000;
    /// Pull only 2 keys in tests, to exercise this more often.
    #[cfg(test)]
    const BUFFER_SIZE: usize = 2;

    /// Creates a new scan iterator.
    fn new(
        engine: Arc<Mutex<E>>,
        txn: TransactionState,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Self {
        let buffer = VecDeque::with_capacity(Self::BUFFER_SIZE);
        Self { engine, txn, buffer, remainder: Some(range) }
    }

    /// Fills the buffer, if there's any pending items.
    fn fill_buffer(&mut self) -> Result<()> {
        // Check if there's anything to buffer.
        if self.buffer.len() >= Self::BUFFER_SIZE {
            return Ok(());
        }
        let Some(range) = self.remainder.take() else { return Ok(()) };
        let range_end = range.1.clone();

        let mut engine = self.engine.lock()?;
        let mut iter = VersionIterator::new(&self.txn, engine.scan(range)).peekable();
        while let Some((key, _, value)) = iter.next().transpose()? {
            // If the next key equals this one, we're not at the latest version.
            match iter.peek() {
                Some(Ok((next, _, _))) if next == &key => continue,
                Some(Err(err)) => return Err(err.clone()),
                Some(Ok(_)) | None => {}
            }
            // Decode the value, and skip deleted keys (tombstones).
            let Some(value) = bincode::deserialize(&value)? else { continue };
            self.buffer.push_back((key, value));
            // If we filled the buffer, save the remaining range (if any) and
            // return. peek() has already buffered next(), so pull it.
            if self.buffer.len() == Self::BUFFER_SIZE {
                if let Some((next, version, _)) = iter.next().transpose()? {
                    // We have to re-encode it as a raw engine key, since we
                    // only have access to the decoded MVCC user key.
                    let range_start = Bound::Included(Key::Version(next.into(), version).encode());
                    self.remainder = Some((range_start, range_end));
                }
                return Ok(());
            }
        }
        Ok(())
    }
}

impl<E: Engine> Iterator for ScanIterator<E> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            if let Err(error) = self.fill_buffer() {
                return Some(Err(error));
            }
        }
        self.buffer.pop_front().map(Ok)
    }
}

/// An iterator that decodes raw engine key/value pairs into MVCC key/value
/// versions, and skips invisible versions. Helper for ScanIterator.
struct VersionIterator<'a, I: engine::ScanIterator> {
    /// The transaction the scan is running in.
    txn: &'a TransactionState,
    /// The inner engine scan iterator.
    inner: I,
}

impl<'a, I: engine::ScanIterator> VersionIterator<'a, I> {
    /// Creates a new MVCC version iterator for the given engine iterator.
    fn new(txn: &'a TransactionState, inner: I) -> Self {
        Self { txn, inner }
    }

    // Fallible next(). Returns the next visible key/version/value tuple.
    #[allow(clippy::type_complexity)]
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            let Key::Version(key, version) = Key::decode(&key)? else {
                return errdata!("expected Key::Version got {key:?}");
            };
            if !self.txn.is_visible(version) {
                continue;
            }
            return Ok(Some((key.into_owned(), version, value)));
        }
        Ok(None)
    }
}

impl<'a, I: engine::ScanIterator> Iterator for VersionIterator<'a, I> {
    type Item = Result<(Vec<u8>, Version, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::encoding::format::{self, Formatter as _};
    use crate::storage::engine::test::{decode_binary, parse_key_range, Emit, Mirror, Operation};
    use crate::storage::{BitCask, Memory};

    use crossbeam::channel::Receiver;
    use std::collections::HashMap;
    use std::fmt::Write as _;
    use std::{error::Error, result::Result};
    use test_case::test_case;
    use test_each_file::test_each_path;

    // Run goldenscript tests in src/storage/testscripts/mvcc.
    test_each_path! { in "src/storage/testscripts/mvcc" as scripts => test_goldenscript }

    fn test_goldenscript(path: &std::path::Path) {
        goldenscript::run(&mut MVCCRunner::new(), path).expect("goldenscript failed")
    }

    /// Tests that key prefixes are actually prefixes of keys.
    #[test_case(KeyPrefix::NextVersion, Key::NextVersion; "NextVersion")]
    #[test_case(KeyPrefix::TxnActive, Key::TxnActive(1); "TxnActive")]
    #[test_case(KeyPrefix::TxnActiveSnapshot, Key::TxnActiveSnapshot(1); "TxnActiveSnapshot")]
    #[test_case(KeyPrefix::TxnWrite(1), Key::TxnWrite(1, b"foo".as_slice().into()); "TxnWrite")]
    #[test_case(KeyPrefix::Version(b"foo".as_slice().into()), Key::Version(b"foo".as_slice().into(), 1); "Version")]
    #[test_case(KeyPrefix::Unversioned, Key::Unversioned(b"foo".as_slice().into()); "Unversioned")]
    fn key_prefix(prefix: KeyPrefix, key: Key) {
        let prefix = prefix.encode();
        let key = key.encode();
        assert_eq!(prefix, key[..prefix.len()])
    }

    /// Runs MVCC goldenscript tests.
    pub struct MVCCRunner {
        mvcc: MVCC<TestEngine>,
        txns: HashMap<String, Transaction<TestEngine>>,
        op_rx: Receiver<Operation>,
        #[allow(dead_code)]
        tempdir: tempfile::TempDir,
    }

    type TestEngine = Emit<Mirror<BitCask, Memory>>;

    impl MVCCRunner {
        fn new() -> Self {
            // Use both a BitCask and a Memory engine, and mirror operations
            // across them. Emit engine operations to op_rx.
            let (op_tx, op_rx) = crossbeam::channel::unbounded();
            let tempdir = tempfile::TempDir::with_prefix("toydb").expect("tempdir failed");
            let bitcask = BitCask::new(tempdir.path().join("bitcask")).expect("bitcask failed");
            let memory = Memory::new();
            let engine = Emit::new(Mirror::new(bitcask, memory), op_tx);
            let mvcc = MVCC::new(engine);
            Self { mvcc, op_rx, txns: HashMap::new(), tempdir }
        }

        /// Fetches the named transaction from a command prefix.
        fn get_txn(
            &mut self,
            prefix: &Option<String>,
        ) -> Result<&'_ mut Transaction<TestEngine>, Box<dyn Error>> {
            let name = Self::txn_name(prefix)?;
            self.txns.get_mut(name).ok_or(format!("unknown txn {name}").into())
        }

        /// Fetches the txn name from a command prefix, or errors.
        fn txn_name(prefix: &Option<String>) -> Result<&str, Box<dyn Error>> {
            prefix.as_deref().ok_or("no txn name".into())
        }

        /// Errors if a txn prefix is given.
        fn no_txn(command: &goldenscript::Command) -> Result<(), Box<dyn Error>> {
            if let Some(name) = &command.prefix {
                return Err(format!("can't run {} with txn {name}", command.name).into());
            }
            Ok(())
        }
    }

    impl goldenscript::Runner for MVCCRunner {
        fn run(&mut self, command: &goldenscript::Command) -> Result<String, Box<dyn Error>> {
            let mut output = String::new();
            match command.name.as_str() {
                // txn: begin [readonly] [as_of=VERSION]
                "begin" => {
                    let name = Self::txn_name(&command.prefix)?;
                    if self.txns.contains_key(name) {
                        return Err(format!("txn {name} already exists").into());
                    }
                    let mut args = command.consume_args();
                    let readonly = match args.next_pos().map(|a| a.value.as_str()) {
                        Some("readonly") => true,
                        None => false,
                        Some(v) => return Err(format!("invalid argument {v}").into()),
                    };
                    let as_of = args.lookup_parse("as_of")?;
                    args.reject_rest()?;
                    let txn = match (readonly, as_of) {
                        (false, None) => self.mvcc.begin()?,
                        (true, None) => self.mvcc.begin_read_only()?,
                        (true, Some(v)) => self.mvcc.begin_as_of(v)?,
                        (false, Some(_)) => return Err("as_of only valid for read-only txn".into()),
                    };
                    self.txns.insert(name.to_string(), txn);
                }

                // txn: commit
                "commit" => {
                    let name = Self::txn_name(&command.prefix)?;
                    let txn = self.txns.remove(name).ok_or(format!("unknown txn {name}"))?;
                    command.consume_args().reject_rest()?;
                    txn.commit()?;
                }

                // txn: delete KEY...
                "delete" => {
                    let txn = self.get_txn(&command.prefix)?;
                    let mut args = command.consume_args();
                    for arg in args.rest_pos() {
                        let key = decode_binary(&arg.value);
                        txn.delete(&key)?;
                    }
                    args.reject_rest()?;
                }

                // dump
                "dump" => {
                    command.consume_args().reject_rest()?;
                    let mut engine = self.mvcc.engine.lock().unwrap();
                    let mut scan = engine.scan(..);
                    while let Some((key, value)) = scan.next().transpose()? {
                        let fmtkv = format::MVCC::<format::Raw>::key_value(&key, &value);
                        let rawkv = format::Raw::key_value(&key, &value);
                        writeln!(output, "{fmtkv} [{rawkv}]")?;
                    }
                }

                // txn: get KEY...
                "get" => {
                    let txn = self.get_txn(&command.prefix)?;
                    let mut args = command.consume_args();
                    for arg in args.rest_pos() {
                        let key = decode_binary(&arg.value);
                        let value = txn.get(&key)?;
                        let fmtkv = format::Raw::key_maybe_value(&key, value.as_deref());
                        writeln!(output, "{fmtkv}")?;
                    }
                    args.reject_rest()?;
                }

                // get_unversioned KEY...
                "get_unversioned" => {
                    Self::no_txn(command)?;
                    let mut args = command.consume_args();
                    for arg in args.rest_pos() {
                        let key = decode_binary(&arg.value);
                        let value = self.mvcc.get_unversioned(&key)?;
                        let fmtkv = format::Raw::key_maybe_value(&key, value.as_deref());
                        writeln!(output, "{fmtkv}")?;
                    }
                    args.reject_rest()?;
                }

                // import [VERSION] KEY=VALUE...
                "import" => {
                    Self::no_txn(command)?;
                    let mut args = command.consume_args();
                    let version = args.next_pos().map(|a| a.parse()).transpose()?;
                    let mut txn = self.mvcc.begin()?;
                    if let Some(version) = version {
                        if txn.version() > version {
                            return Err(format!("version {version} already used").into());
                        }
                        while txn.version() < version {
                            txn = self.mvcc.begin()?;
                        }
                    }
                    for kv in args.rest_key() {
                        let key = decode_binary(kv.key.as_ref().unwrap());
                        let value = decode_binary(&kv.value);
                        if value.is_empty() {
                            txn.delete(&key)?;
                        } else {
                            txn.set(&key, value)?;
                        }
                    }
                    args.reject_rest()?;
                    txn.commit()?;
                }

                // txn: resume JSON
                "resume" => {
                    let name = Self::txn_name(&command.prefix)?;
                    let mut args = command.consume_args();
                    let raw = &args.next_pos().ok_or("state not given")?.value;
                    args.reject_rest()?;
                    let state: TransactionState = serde_json::from_str(raw)?;
                    let txn = self.mvcc.resume(state)?;
                    self.txns.insert(name.to_string(), txn);
                }

                // txn: rollback
                "rollback" => {
                    let name = Self::txn_name(&command.prefix)?;
                    let txn = self.txns.remove(name).ok_or(format!("unknown txn {name}"))?;
                    command.consume_args().reject_rest()?;
                    txn.rollback()?;
                }

                // txn: scan [RANGE]
                "scan" => {
                    let txn = self.get_txn(&command.prefix)?;
                    let mut args = command.consume_args();
                    let range =
                        parse_key_range(args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."))?;
                    args.reject_rest()?;

                    let kvs: Vec<_> = txn.scan(range).collect::<crate::error::Result<_>>()?;
                    for (key, value) in kvs {
                        writeln!(output, "{}", format::Raw::key_value(&key, &value))?;
                    }
                }

                // txn: scan_prefix PREFIX
                "scan_prefix" => {
                    let txn = self.get_txn(&command.prefix)?;
                    let mut args = command.consume_args();
                    let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
                    args.reject_rest()?;

                    let kvs: Vec<_> =
                        txn.scan_prefix(&prefix).collect::<crate::error::Result<_>>()?;
                    for (key, value) in kvs {
                        writeln!(output, "{}", format::Raw::key_value(&key, &value))?;
                    }
                }

                // txn: set KEY=VALUE...
                "set" => {
                    let txn = self.get_txn(&command.prefix)?;
                    let mut args = command.consume_args();
                    for kv in args.rest_key() {
                        let key = decode_binary(kv.key.as_ref().unwrap());
                        let value = decode_binary(&kv.value);
                        txn.set(&key, value)?;
                    }
                    args.reject_rest()?;
                }

                // set_unversioned KEY=VALUE...
                "set_unversioned" => {
                    Self::no_txn(command)?;
                    let mut args = command.consume_args();
                    for kv in args.rest_key() {
                        let key = decode_binary(kv.key.as_ref().unwrap());
                        let value = decode_binary(&kv.value);
                        self.mvcc.set_unversioned(&key, value)?;
                    }
                    args.reject_rest()?;
                }

                // txn: state
                "state" => {
                    command.consume_args().reject_rest()?;
                    let txn = self.get_txn(&command.prefix)?;
                    let state = txn.state();
                    write!(
                        output,
                        "v{} {} active={{{}}}",
                        state.version,
                        if state.read_only { "ro" } else { "rw" },
                        state.active.iter().sorted().join(",")
                    )?;
                }

                // status
                "status" => {
                    let status = self.mvcc.status()?;
                    writeln!(output, "{status:#?}")?;
                }

                name => return Err(format!("invalid command {name}").into()),
            }
            Ok(output)
        }

        /// If requested via [ops] tag, output engine operations for the command.
        fn end_command(
            &mut self,
            command: &goldenscript::Command,
        ) -> Result<String, Box<dyn Error>> {
            // Parse tags.
            let mut show_ops = false;
            for tag in &command.tags {
                match tag.as_str() {
                    "ops" => show_ops = true,
                    tag => return Err(format!("invalid tag {tag}").into()),
                }
            }

            // Process engine operations, either output or drain.
            let mut output = String::new();
            while let Ok(op) = self.op_rx.try_recv() {
                match op {
                    _ if !show_ops => {}
                    Operation::Delete { key } => {
                        let fmtkey = format::MVCC::<format::Raw>::key(&key);
                        let rawkey = format::Raw::key(&key);
                        writeln!(output, "engine delete {fmtkey} [{rawkey}]")?
                    }
                    Operation::Flush => writeln!(output, "engine flush")?,
                    Operation::Set { key, value } => {
                        let fmtkv = format::MVCC::<format::Raw>::key_value(&key, &value);
                        let rawkv = format::Raw::key_value(&key, &value);
                        writeln!(output, "engine set {fmtkv} [{rawkv}]")?
                    }
                }
            }
            Ok(output)
        }
    }
}
