use crate::storage::log;
use crate::Error;

use ::log::debug;
use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::ops::RangeBounds;

/// A replicated log entry
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// The index of the entry.
    pub index: u64,
    /// The term in which the entry was added.
    pub term: u64,
    /// The state machine command. None is used to commit noops during leader election.
    pub command: Option<Vec<u8>>,
}

/// A metadata key
#[derive(Clone, Debug, PartialEq)]
pub enum Key {
    Term,
    VotedFor,
}

impl Key {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Term => vec![0],
            Self::VotedFor => vec![1],
        }
    }
}

/// A log scan
pub type Scan<'a> = Box<dyn Iterator<Item = Result<Entry, Error>> + 'a>;

/// The replicated Raft log
pub struct Log<S: log::Store> {
    /// The underlying log store.
    store: S,
    /// The index of the last stored entry.
    pub(super) last_index: u64,
    /// The term of the last stored entry.
    pub(super) last_term: u64,
    /// The last entry known to be committed.
    pub(super) commit_index: u64,
    /// The term of the last committed entry.
    pub(super) commit_term: u64,
}

impl<S: log::Store> Log<S> {
    /// Creates a new log, using a log::Store for storage.
    pub fn new(store: S) -> Result<Self, Error> {
        let (commit_index, commit_term) = match store.committed() {
            0 => (0, 0),
            index => store
                .get(index)?
                .map(|v| Self::deserialize::<Entry>(&v))
                .transpose()?
                .map(|e| (e.index, e.term))
                .ok_or_else(|| Error::Internal("Committed entry not found".into()))?,
        };
        let (last_index, last_term) = match store.len() {
            0 => (0, 0),
            index => store
                .get(index)?
                .map(|v| Self::deserialize::<Entry>(&v))
                .transpose()?
                .map(|e| (e.index, e.term))
                .ok_or_else(|| Error::Internal("Last entry not found".into()))?,
        };
        Ok(Self { store, last_index, last_term, commit_index, commit_term })
    }

    /// Appends a command to the log, returning the entry.
    pub fn append(&mut self, term: u64, command: Option<Vec<u8>>) -> Result<Entry, Error> {
        let entry = Entry { index: self.last_index + 1, term, command };
        debug!("Appending log entry {}: {:?}", entry.index, entry);
        self.store.append(Self::serialize(&entry)?)?;
        self.last_index = entry.index;
        self.last_term = entry.term;
        Ok(entry)
    }

    /// Commits entries up to and including an index.
    pub fn commit(&mut self, index: u64) -> Result<u64, Error> {
        let entry = self
            .get(index)?
            .ok_or_else(|| Error::Internal(format!("Entry {} not found", index)))?;
        self.store.commit(index)?;
        self.commit_index = entry.index;
        self.commit_term = entry.term;
        Ok(index)
    }

    /// Fetches an entry at an index
    pub fn get(&self, index: u64) -> Result<Option<Entry>, Error> {
        self.store.get(index)?.map(|v| Self::deserialize(&v)).transpose()
    }

    /// Checks if the log contains an entry
    pub fn has(&self, index: u64, term: u64) -> Result<bool, Error> {
        match self.get(index)? {
            Some(entry) => Ok(entry.term == term),
            None if index == 0 && term == 0 => Ok(true),
            None => Ok(false),
        }
    }

    /// Iterates over log entries
    pub fn scan(&self, range: impl RangeBounds<u64>) -> Scan {
        Box::new(self.store.scan(range).map(|r| r.and_then(|v| Self::deserialize(&v))))
    }

    /// Splices a set of entries onto an offset. The entries must be contiguous, and the first entry
    /// must be at most last_index+1. If an entry does not exist, append it. If an existing entry
    /// has a term mismatch, replace it and all following entries.
    pub fn splice(&mut self, entries: Vec<Entry>) -> Result<u64, Error> {
        for i in 0..entries.len() {
            if i == 0 && entries.get(i).unwrap().index > self.last_index + 1 {
                return Err(Error::Internal("Spliced entries cannot begin past last index".into()));
            }
            if entries.get(i).unwrap().index != entries.get(0).unwrap().index + i as u64 {
                return Err(Error::Internal("Spliced entries must be contiguous".into()));
            }
        }
        for entry in entries {
            if let Some(ref current) = self.get(entry.index)? {
                if current.term == entry.term {
                    continue;
                }
                self.truncate(entry.index - 1)?;
            }
            self.append(entry.term, entry.command)?;
        }
        Ok(self.last_index)
    }

    /// Truncates the log such that its last item is at most index.
    /// Refuses to remove entries that have been applied or committed.
    pub fn truncate(&mut self, index: u64) -> Result<u64, Error> {
        debug!("Truncating log from entry {}", index);
        let (index, term) = match self.store.truncate(index)? {
            0 => (0, 0),
            i => self
                .store
                .get(i)?
                .map(|v| Self::deserialize::<Entry>(&v))
                .transpose()?
                .map(|e| (e.index, e.term))
                .ok_or_else(|| Error::Internal(format!("Entry {} not found", index)))?,
        };
        self.last_index = index;
        self.last_term = term;
        Ok(index)
    }

    /// Loads information about the most recent term known by the log, containing the term number (0
    /// if none) and candidate voted for in current term (if any).
    pub fn load_term(&self) -> Result<(u64, Option<String>), Error> {
        let term = self
            .store
            .get_metadata(&Key::Term.encode())?
            .map(|v| Self::deserialize(&v))
            .transpose()?
            .unwrap_or(0);
        let voted_for = self
            .store
            .get_metadata(&Key::VotedFor.encode())?
            .map(|v| Self::deserialize(&v))
            .transpose()?
            .unwrap_or(None);
        debug!("Loaded term {} and voted_for {:?} from log", term, voted_for);
        Ok((term, voted_for))
    }

    /// Saves information about the most recent term.
    pub fn save_term(&mut self, term: u64, voted_for: Option<&str>) -> Result<(), Error> {
        self.store.set_metadata(&Key::Term.encode(), Self::serialize(&term)?)?;
        self.store.set_metadata(&Key::VotedFor.encode(), Self::serialize(&voted_for.clone())?)?;
        Ok(())
    }

    /// Serializes a value for the log store.
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>, Error> {
        Ok(serde_cbor::ser::to_vec_packed(value)?)
    }

    /// Deserializes a value from the log store.
    fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V, Error> {
        Ok(serde_cbor::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn setup() -> Result<(Log<log::Test>, log::Test), Error> {
        let store = log::Test::new();
        let log = Log::new(store.clone())?;
        Ok((log, store))
    }

    #[test]
    fn new() -> Result<(), Error> {
        let (l, _) = setup()?;
        assert_eq!(0, l.last_index);
        assert_eq!(0, l.last_term);
        assert_eq!(0, l.commit_index);
        assert_eq!(0, l.commit_term);
        assert_eq!(None, l.get(1)?);
        Ok(())
    }

    #[test]
    fn append() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        assert_eq!(Ok(None), l.get(1));

        assert_eq!(
            Entry { index: 1, term: 3, command: Some(vec![0x01]) },
            l.append(3, Some(vec![0x01]))?
        );
        assert_eq!(Some(Entry { index: 1, term: 3, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(None, l.get(2)?);

        assert_eq!(1, l.last_index);
        assert_eq!(3, l.last_term);
        assert_eq!(0, l.commit_index);
        assert_eq!(0, l.commit_term);
        Ok(())
    }

    #[test]
    fn append_none() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        assert_eq!(Entry { index: 1, term: 3, command: None }, l.append(3, None)?);
        assert_eq!(Some(Entry { index: 1, term: 3, command: None }), l.get(1)?);
        Ok(())
    }

    #[test]
    fn append_persistence() -> Result<(), Error> {
        let (mut l, store) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, None)?;
        l.append(2, Some(vec![0x03]))?;

        let l = Log::new(store)?;
        assert_eq!(Some(Entry { index: 1, term: 1, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(Some(Entry { index: 2, term: 2, command: None }), l.get(2)?);
        assert_eq!(Some(Entry { index: 3, term: 2, command: Some(vec![0x03]) }), l.get(3)?);
        Ok(())
    }

    #[test]
    fn commit() -> Result<(), Error> {
        let (mut l, store) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, None)?;
        l.append(2, Some(vec![0x03]))?;
        assert_eq!(3, l.commit(3)?);
        assert_eq!(3, l.commit_index);
        assert_eq!(2, l.commit_term);

        // The last committed entry must be persisted, to sync with state machine
        let l = Log::new(store)?;
        assert_eq!(3, l.commit_index);
        assert_eq!(2, l.commit_term);
        Ok(())
    }

    #[test]
    fn commit_beyond() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, None)?;
        l.append(2, Some(vec![0x03]))?;
        assert_eq!(Err(Error::Internal("Entry 4 not found".into())), l.commit(4));

        Ok(())
    }

    #[test]
    fn commit_partial() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, None)?;
        l.append(2, Some(vec![0x03]))?;
        assert_eq!(2, l.commit(2)?);
        assert_eq!(2, l.commit_index);
        assert_eq!(2, l.commit_term);
        Ok(())
    }

    #[test]
    fn get() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        assert_eq!(None, l.get(1)?);

        l.append(3, Some(vec![0x01]))?;
        assert_eq!(Some(Entry { index: 1, term: 3, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(None, l.get(2)?);
        Ok(())
    }

    #[test]
    fn has() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(2, Some(vec![0x01]))?;

        assert_eq!(true, l.has(1, 2)?);
        assert_eq!(true, l.has(0, 0)?);
        assert_eq!(false, l.has(0, 1)?);
        assert_eq!(false, l.has(1, 0)?);
        assert_eq!(false, l.has(1, 3)?);
        assert_eq!(false, l.has(2, 0)?);
        assert_eq!(false, l.has(2, 1)?);
        Ok(())
    }

    #[test]
    fn scan() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(1, Some(vec![0x02]))?;
        l.append(1, Some(vec![0x03]))?;

        assert_eq!(
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
            ],
            l.scan(0..).collect::<Result<Vec<_>, Error>>()?
        );
        assert_eq!(
            vec![
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
            ],
            l.scan(2..).collect::<Result<Vec<_>, Error>>()?
        );
        assert!(l.scan(4..).collect::<Result<Vec<_>, Error>>()?.is_empty());
        Ok(())
    }

    #[test]
    fn load_save_term() -> Result<(), Error> {
        // Test loading empty term
        let (l, _) = setup()?;
        assert_eq!((0, None), l.load_term()?);

        // Test loading saved term
        let (mut l, store) = setup()?;
        l.save_term(1, Some("a"))?;
        let l = Log::new(store)?;
        assert_eq!((1, Some("a".into())), l.load_term()?);

        // Test replacing saved term with none
        let (mut l, _) = setup()?;
        l.save_term(1, Some("a"))?;
        assert_eq!((1, Some("a".into())), l.load_term()?);
        l.save_term(0, None)?;
        assert_eq!((0, None), l.load_term()?);
        Ok(())
    }

    #[test]
    fn splice() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(
            4,
            l.splice(vec![
                Entry { index: 3, term: 3, command: Some(vec![0x03]) },
                Entry { index: 4, term: 4, command: Some(vec![0x04]) },
            ])?
        );
        assert_eq!(
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 2, command: Some(vec![0x02]) },
                Entry { index: 3, term: 3, command: Some(vec![0x03]) },
                Entry { index: 4, term: 4, command: Some(vec![0x04]) },
            ],
            l.scan(..).collect::<Result<Vec<_>, Error>>()?
        );
        assert_eq!(4, l.last_index);
        assert_eq!(4, l.last_term);
        Ok(())
    }

    #[test]
    fn splice_all() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(
            2,
            l.splice(vec![
                Entry { index: 1, term: 4, command: Some(vec![0x0a]) },
                Entry { index: 2, term: 4, command: Some(vec![0x0b]) },
            ])?
        );
        assert_eq!(
            vec![
                Entry { index: 1, term: 4, command: Some(vec![0x0a]) },
                Entry { index: 2, term: 4, command: Some(vec![0x0b]) },
            ],
            l.scan(..).collect::<Result<Vec<_>, Error>>()?
        );
        assert_eq!(2, l.last_index);
        assert_eq!(4, l.last_term);
        Ok(())
    }

    #[test]
    fn splice_append() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;

        assert_eq!(
            4,
            l.splice(vec![
                Entry { index: 3, term: 3, command: Some(vec![0x03]) },
                Entry { index: 4, term: 4, command: Some(vec![0x04]) },
            ])?
        );
        assert_eq!(
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 2, command: Some(vec![0x02]) },
                Entry { index: 3, term: 3, command: Some(vec![0x03]) },
                Entry { index: 4, term: 4, command: Some(vec![0x04]) },
            ],
            l.scan(..).collect::<Result<Vec<_>, Error>>()?
        );
        assert_eq!(4, l.last_index);
        assert_eq!(4, l.last_term);
        Ok(())
    }

    #[test]
    fn splice_conflict_term() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;
        l.append(4, Some(vec![0x04]))?;

        assert_eq!(
            3,
            l.splice(vec![
                Entry { index: 2, term: 3, command: Some(vec![0x0b]) },
                Entry { index: 3, term: 3, command: Some(vec![0x0c]) }
            ])?
        );
        assert_eq!(
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 3, command: Some(vec![0x0b]) },
                Entry { index: 3, term: 3, command: Some(vec![0x0c]) },
            ],
            l.scan(..).collect::<Result<Vec<_>, Error>>()?
        );
        assert_eq!(3, l.last_index);
        assert_eq!(3, l.last_term);
        Ok(())
    }

    #[test]
    fn splice_error_noncontiguous() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(
            Err(Error::Internal("Spliced entries must be contiguous".into())),
            l.splice(vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 3, term: 3, command: Some(vec![0x03]) },
            ])
        );
        assert_eq!(
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 2, command: Some(vec![0x02]) },
                Entry { index: 3, term: 3, command: Some(vec![0x03]) },
            ],
            l.scan(..).collect::<Result<Vec<_>, Error>>()?
        );
        Ok(())
    }

    #[test]
    fn splice_error_beyond_last() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(
            Err(Error::Internal("Spliced entries cannot begin past last index".into())),
            l.splice(vec![
                Entry { index: 5, term: 3, command: Some(vec![0x05]) },
                Entry { index: 6, term: 3, command: Some(vec![0x06]) },
            ])
        );
        assert_eq!(
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 2, command: Some(vec![0x02]) },
                Entry { index: 3, term: 3, command: Some(vec![0x03]) },
            ],
            l.scan(..).collect::<Result<Vec<_>, Error>>()?
        );
        Ok(())
    }

    #[test]
    fn splice_overlap_inside() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(3, l.splice(vec![Entry { index: 2, term: 2, command: Some(vec![0x02]) },])?);
        assert_eq!(
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 2, command: Some(vec![0x02]) },
                Entry { index: 3, term: 3, command: Some(vec![0x03]) },
            ],
            l.scan(..).collect::<Result<Vec<_>, Error>>()?
        );
        Ok(())
    }

    #[test]
    fn truncate() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(2, l.truncate(2)?);
        assert_eq!(
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 2, command: Some(vec![0x02]) },
            ],
            l.scan(..).collect::<Result<Vec<_>, Error>>()?
        );
        Ok(())
    }

    #[test]
    fn truncate_beyond() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(3, l.truncate(4)?);
        assert_eq!(
            vec![
                Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                Entry { index: 2, term: 2, command: Some(vec![0x02]) },
                Entry { index: 3, term: 3, command: Some(vec![0x03]) },
            ],
            l.scan(..).collect::<Result<Vec<_>, Error>>()?
        );
        Ok(())
    }

    #[test]
    fn truncate_committed() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;
        l.commit(2)?;

        assert_eq!(
            l.truncate(1),
            Err(Error::Internal("Cannot truncate below committed index 2".into()))
        );
        assert_eq!(l.truncate(2)?, 2);
        Ok(())
    }

    #[test]
    fn truncate_zero() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(0, l.truncate(0)?);
        assert!(l.scan(..).collect::<Result<Vec<_>, Error>>()?.is_empty());
        Ok(())
    }
}
