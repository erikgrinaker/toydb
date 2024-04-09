use crate::encoding::{bincode, keycode};
use crate::error::{Error, Result};
use crate::storage;

use ::log::debug;
use serde::{Deserialize, Serialize};

use super::{NodeID, Term};

/// A log index.
pub type Index = u64;

/// A log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// The entry index.
    pub index: Index,
    /// The term in which the entry was added.
    pub term: Term,
    /// The state machine command. None is used to commit noops during leader election.
    pub command: Option<Vec<u8>>,
}

/// A log key, encoded using KeyCode.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Key {
    /// A log entry, storing the term and command.
    Entry(Index),
    /// Stores the current term and vote (if any).
    TermVote,
    /// Stores the current commit index (if any).
    CommitIndex,
}

impl Key {
    fn decode(bytes: &[u8]) -> Result<Self> {
        keycode::deserialize(bytes)
    }

    fn encode(&self) -> Result<Vec<u8>> {
        keycode::serialize(self)
    }
}

/// Log key prefixes, used for prefix scans.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum KeyPrefix {
    Entry,
    TermVote,
    CommitIndex,
}

impl KeyPrefix {
    fn encode(&self) -> Result<Vec<u8>> {
        keycode::serialize(self)
    }
}
/// A Raft log.
pub struct Log {
    /// The underlying storage engine. Uses a trait object instead of generics,
    /// to allow runtime selection of the engine (based on the program config)
    /// and avoid propagating the generic type parameters throughout.
    engine: Box<dyn storage::Engine>,
    /// The index of the last stored entry.
    last_index: Index,
    /// The term of the last stored entry.
    last_term: Term,
    /// The index of the last committed entry.
    commit_index: Index,
    /// The term of the last committed entry.
    commit_term: Term,
    /// Whether to sync writes to disk.
    sync: bool,
}

impl Log {
    /// Creates a new log, using the given storage engine.
    pub fn new(mut engine: impl storage::Engine + 'static, sync: bool) -> Result<Self> {
        let (last_index, last_term) = engine
            .scan_prefix(&KeyPrefix::Entry.encode()?)
            .last()
            .transpose()?
            .map(|(k, v)| Self::decode_entry(&k, &v))
            .transpose()?
            .map(|e| (e.index, e.term))
            .unwrap_or((0, 0));
        let (commit_index, commit_term) = engine
            .get(&Key::CommitIndex.encode()?)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?
            .unwrap_or((0, 0));
        Ok(Self {
            engine: Box::new(engine),
            last_index,
            last_term,
            commit_index,
            commit_term,
            sync,
        })
    }

    /// Decodes an entry from a log key/value pair.
    fn decode_entry(key: &[u8], value: &[u8]) -> Result<Entry> {
        if let Key::Entry(index) = Key::decode(key)? {
            Self::decode_entry_value(index, value)
        } else {
            Err(Error::Internal(format!("Invalid key {:x?}", key)))
        }
    }

    /// Decodes an entry from a value at a given index.
    fn decode_entry_value(index: Index, value: &[u8]) -> Result<Entry> {
        let (term, command) = bincode::deserialize(value)?;
        Ok(Entry { index, term, command })
    }

    /// Returns log engine name and status.
    pub fn status(&mut self) -> Result<storage::engine::Status> {
        self.engine.status()
    }

    /// Returns the commit index and term.
    pub fn get_commit_index(&self) -> (Index, Term) {
        (self.commit_index, self.commit_term)
    }

    /// Returns the last log index and term.
    pub fn get_last_index(&self) -> (Index, Term) {
        (self.last_index, self.last_term)
    }

    /// Returns the last known term (0 if none), and cast vote (if any).
    pub fn get_term(&mut self) -> Result<(Term, Option<NodeID>)> {
        let (term, voted_for) = self
            .engine
            .get(&Key::TermVote.encode()?)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?
            .unwrap_or((0, None));
        debug!("Loaded term {} and voted_for {:?} from log", term, voted_for);
        Ok((term, voted_for))
    }

    /// Sets the most recent term, and cast vote (if any).
    pub fn set_term(&mut self, term: Term, voted_for: Option<NodeID>) -> Result<()> {
        self.engine.set(&Key::TermVote.encode()?, bincode::serialize(&(term, voted_for))?)?;
        self.maybe_flush()
    }

    /// Flushes the log to stable storage, if enabled.
    fn maybe_flush(&mut self) -> Result<()> {
        if self.sync {
            self.engine.flush()?;
        }
        Ok(())
    }

    /// Appends a command to the log, returning its index. None implies a noop
    /// command, typically after Raft leader changes.
    pub fn append(&mut self, term: Term, command: Option<Vec<u8>>) -> Result<Index> {
        let index = self.last_index + 1;
        self.engine.set(&Key::Entry(index).encode()?, bincode::serialize(&(term, command))?)?;
        self.maybe_flush()?;
        self.last_index = index;
        self.last_term = term;
        Ok(index)
    }

    /// Commits entries up to and including the given index. The index must
    /// exist, and must be at or after the current commit index.
    pub fn commit(&mut self, index: Index) -> Result<Index> {
        if index < self.commit_index {
            return Err(Error::Internal(format!(
                "Commit index regression {} -> {}",
                self.commit_index, index
            )));
        }
        let Some(entry) = self.get(index)? else {
            return Err(Error::Internal(format!("Can't commit non-existant index {}", index)));
        };
        self.engine
            .set(&Key::CommitIndex.encode()?, bincode::serialize(&(entry.index, entry.term))?)?;
        self.maybe_flush()?;
        self.commit_index = entry.index;
        self.commit_term = entry.term;
        Ok(index)
    }

    /// Fetches an entry at an index, or None if it does not exist.
    pub fn get(&mut self, index: Index) -> Result<Option<Entry>> {
        self.engine
            .get(&Key::Entry(index).encode()?)?
            .map(|v| Self::decode_entry_value(index, &v))
            .transpose()
    }

    /// Checks if the log contains an entry with the given term.
    pub fn has(&mut self, index: Index, term: Term) -> Result<bool> {
        match self.get(index)? {
            Some(entry) => Ok(entry.term == term),
            None if index == 0 && term == 0 => Ok(true),
            None => Ok(false),
        }
    }

    /// Iterates over log entries in the given index range.
    pub fn scan(
        &mut self,
        range: impl std::ops::RangeBounds<Index>,
    ) -> Result<impl Iterator<Item = Result<Entry>> + '_> {
        let from = match range.start_bound() {
            std::ops::Bound::Excluded(i) => std::ops::Bound::Excluded(Key::Entry(*i).encode()?),
            std::ops::Bound::Included(i) => std::ops::Bound::Included(Key::Entry(*i).encode()?),
            std::ops::Bound::Unbounded => std::ops::Bound::Included(Key::Entry(0).encode()?),
        };
        let to = match range.end_bound() {
            std::ops::Bound::Excluded(i) => std::ops::Bound::Excluded(Key::Entry(*i).encode()?),
            std::ops::Bound::Included(i) => std::ops::Bound::Included(Key::Entry(*i).encode()?),
            std::ops::Bound::Unbounded => {
                std::ops::Bound::Included(Key::Entry(Index::MAX).encode()?)
            }
        };
        Ok(self
            .engine
            .scan_dyn((from, to))
            .map(|r| r.and_then(|(k, v)| Self::decode_entry(&k, &v))))
    }

    /// Splices a set of entries into the log. The entries must be contiguous,
    /// and the first entry must be at most last_index+1. If an entry does not
    /// exist, append it. If an existing entry has a term mismatch, replace it
    /// and all following entries.
    pub fn splice(&mut self, entries: Vec<Entry>) -> Result<Index> {
        if entries.is_empty() {
            return Ok(self.last_index);
        }
        if entries[0].index == 0 || entries[0].index > self.last_index + 1 {
            return Err(Error::Internal("Spliced entries must begin before last index".into()));
        }
        if !entries.windows(2).all(|w| w[0].index + 1 == w[1].index) {
            return Err(Error::Internal("Spliced entries must be contiguous".into()));
        }
        let (last_index, last_term) = entries.last().map(|e| (e.index, e.term)).unwrap();

        // Skip entries that are already in the log (identified by index and term).
        let mut entries = entries.as_slice();
        let mut scan = self.scan(entries[0].index..=entries.last().unwrap().index)?;
        while let Some(e) = scan.next().transpose()? {
            if e.term != entries[0].term {
                break;
            }
            entries = &entries[1..];
        }
        drop(scan);

        if !entries.is_empty() && entries[0].index <= self.commit_index {
            return Err(Error::Internal("Spliced entries must begin after commit index".into()));
        }

        // Write any entries not already in the log.
        for e in entries {
            self.engine
                .set(&Key::Entry(e.index).encode()?, bincode::serialize(&(&e.term, &e.command))?)?;
        }

        // Remove the remaining tail of the old log, if any, and update the index.
        for index in (last_index + 1)..=self.last_index {
            self.engine.delete(&Key::Entry(index).encode()?)?;
        }
        self.maybe_flush()?;
        self.last_index = last_index;
        self.last_term = last_term;
        Ok(self.last_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Memory;
    use pretty_assertions::assert_eq;

    fn setup() -> Log {
        Log::new(Memory::new(), false).expect("empty engine should never fail to open")
    }

    #[test]
    fn new() -> Result<()> {
        let mut l = setup();
        assert_eq!((0, 0), l.get_commit_index());
        assert_eq!((0, 0), l.get_last_index());
        assert_eq!(None, l.get(1)?);
        Ok(())
    }

    #[test]
    fn append() -> Result<()> {
        let mut l = setup();
        assert_eq!(l.get(1), Ok(None));

        assert_eq!(l.append(3, Some(vec![0x01]))?, 1,);
        assert_eq!(l.get(1)?, Some(Entry { index: 1, term: 3, command: Some(vec![0x01]) }));
        assert_eq!(l.get(2)?, None);

        assert_eq!(l.get_last_index(), (1, 3));
        assert_eq!(l.get_commit_index(), (0, 0));

        assert_eq!(l.append(3, None)?, 2);
        assert_eq!(l.get(2)?, Some(Entry { index: 2, term: 3, command: None }));
        assert_eq!(l.get_last_index(), (2, 3));
        assert_eq!(l.get_commit_index(), (0, 0));
        Ok(())
    }

    #[test]
    fn commit() -> Result<()> {
        let mut l = setup();
        l.append(1, Some(vec![0x01]))?;
        l.append(2, None)?;
        l.append(2, Some(vec![0x03]))?;

        // Committing a missing entry should error.
        assert_eq!(
            l.commit(0),
            Err(Error::Internal("Can't commit non-existant index 0".to_string()))
        );
        assert_eq!(
            l.commit(4),
            Err(Error::Internal("Can't commit non-existant index 4".to_string()))
        );

        // Committing an existing index works, and is idempotent.
        l.commit(2)?;
        assert_eq!(l.get_commit_index(), (2, 2));
        l.commit(2)?;
        assert_eq!(l.get_commit_index(), (2, 2));

        // Regressing the commit index should error.
        assert_eq!(l.commit(1), Err(Error::Internal("Commit index regression 2 -> 1".to_string())));

        // Committing a later index works.
        l.commit(3)?;
        assert_eq!(l.get_commit_index(), (3, 2));

        Ok(())
    }

    #[test]
    fn get() -> Result<()> {
        let mut l = setup();
        assert_eq!(l.get(1)?, None);

        l.append(3, Some(vec![0x01]))?;
        assert_eq!(l.get(1)?, Some(Entry { index: 1, term: 3, command: Some(vec![0x01]) }));
        assert_eq!(l.get(2)?, None);
        Ok(())
    }

    #[test]
    fn has() -> Result<()> {
        let mut l = setup();
        l.append(2, Some(vec![0x01]))?;

        assert!(l.has(1, 2)?);
        assert!(l.has(0, 0)?);
        assert!(!l.has(0, 1)?);
        assert!(!l.has(1, 0)?);
        assert!(!l.has(1, 3)?);
        assert!(!l.has(2, 0)?);
        assert!(!l.has(2, 1)?);
        Ok(())
    }

    #[test]
    fn scan() -> Result<()> {
        let mut l = setup();
        l.append(1, Some(vec![0x01]))?;
        l.append(1, Some(vec![0x02]))?;
        l.append(1, Some(vec![0x03]))?;

        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
            ],
        );
        assert_eq!(
            l.scan(2..=2)?.collect::<Result<Vec<_>>>()?,
            vec![Entry { index: 2, term: 1, command: Some(vec![0x02]) },],
        );
        assert!(l.scan(4..)?.collect::<Result<Vec<_>>>()?.is_empty());
        Ok(())
    }

    #[test]
    fn set_get_term() -> Result<()> {
        let mut l = setup();
        assert_eq!(l.get_term()?, (0, None));

        l.set_term(1, Some(1))?;
        assert_eq!(l.get_term()?, (1, Some(1)));

        l.set_term(0, None)?;
        assert_eq!(l.get_term()?, (0, None));
        Ok(())
    }

    #[test]
    fn splice() -> Result<()> {
        let mut l = setup();

        // Splicing an empty vec should return the current last_index.
        assert_eq!(l.splice(vec![])?, 0);

        // It should error if the first index is not 1.
        assert!(l.splice(vec![Entry { index: 0, term: 1, command: None }]).is_err());
        assert!(l.splice(vec![Entry { index: 2, term: 1, command: None }]).is_err());

        // ...or the entries are not contiguous.
        assert!(l
            .splice(vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            ])
            .is_err());

        // Splicing into an empty log should be fine.
        assert_eq!(
            l.splice(vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            ])?,
            2
        );
        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            ]
        );

        // Splicing an empty vec should be fine, and return the last index
        // without affecting data.
        assert_eq!(l.splice(vec![])?, 2);
        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            ]
        );

        // Splicing with a gap after the last_index should error.
        assert!(l
            .splice(vec![
                Entry { index: 4, term: 1, command: Some(vec![0x04]) },
                Entry { index: 5, term: 1, command: Some(vec![0x05]) },
            ])
            .is_err());

        // Splicing after the last index should be fine.
        assert_eq!(
            l.splice(vec![
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
                Entry { index: 4, term: 1, command: Some(vec![0x04]) },
            ])?,
            4
        );
        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
                Entry { index: 4, term: 1, command: Some(vec![0x04]) },
            ]
        );

        // Splicing with overlap should be a noop.
        assert_eq!(
            l.splice(vec![
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
                Entry { index: 4, term: 1, command: Some(vec![0x04]) },
            ])?,
            4
        );
        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
                Entry { index: 4, term: 1, command: Some(vec![0x04]) },
            ]
        );

        assert_eq!(
            l.splice(vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
                Entry { index: 4, term: 1, command: Some(vec![0x04]) },
            ])?,
            4
        );
        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
                Entry { index: 4, term: 1, command: Some(vec![0x04]) },
            ]
        );

        // Splicing in the middle should truncate the rest, even if the
        // entries match.
        assert_eq!(
            l.splice(vec![
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
            ])?,
            3
        );
        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
            ]
        );

        // Splicing at the start should truncate the rest, even if the
        // entries match.
        assert_eq!(
            l.splice(vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            ])?,
            2
        );
        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            ]
        );

        // Splicing a different command does nothing.
        assert_eq!(l.splice(vec![Entry { index: 2, term: 1, command: Some(vec![0x00]) },])?, 2);
        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            ]
        );

        // Splicing with overlap beyond the end works.
        assert_eq!(
            l.splice(vec![
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
                Entry { index: 4, term: 1, command: Some(vec![0x04]) },
            ])?,
            4
        );
        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
                Entry { index: 4, term: 1, command: Some(vec![0x04]) },
            ]
        );

        // Splicing with a different term replaces.
        assert_eq!(
            l.splice(vec![
                Entry { index: 3, term: 2, command: Some(vec![0x03]) },
                Entry { index: 4, term: 2, command: Some(vec![0x04]) },
            ])?,
            4
        );
        assert_eq!(
            l.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 2, command: Some(vec![0x03]) },
                Entry { index: 4, term: 2, command: Some(vec![0x04]) },
            ]
        );

        Ok(())
    }
}
