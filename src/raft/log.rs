use crate::storage::kv;
use crate::utility::{deserialize, serialize};
use crate::Error;

use log::debug;
use serde_derive::{Deserialize, Serialize};

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

/// The replicated Raft log
pub struct Log<S: kv::Store> {
    /// The underlying key-value store
    kv: S,
    /// The index of the last stored entry.
    last_index: u64,
    /// The term of the last stored entry.
    last_term: u64,
    /// The last entry known to be committed.
    commit_index: u64,
    /// The term of the last committed entry.
    commit_term: u64,
}

impl<S: kv::Store> Log<S> {
    /// Creates a new log, using a kv::Store for storage.
    pub fn new(store: S) -> Result<Self, Error> {
        let (mut commit_index, mut commit_term) = (0, 0);
        if let Some(index) =
            store.get(b"commit_index")?.map(|v| deserialize::<u64>(&v)).transpose()?
        {
            match store
                .get(&index.to_string().as_bytes())?
                .map(|v| deserialize::<Entry>(&v))
                .transpose()?
            {
                Some(Entry { term, .. }) => {
                    commit_index = index;
                    commit_term = term;
                }
                e => {
                    return Err(Error::Internal(format!(
                        "Failed to load committed entry {}, got {:?}",
                        index, e
                    )))
                }
            }
        }
        // FIXME This really needs to be done in a better way.
        let (mut last_index, mut last_term) = (0, 0);
        for i in 1..std::u64::MAX {
            if let Some(e) = store.get(&i.to_string().as_bytes())? {
                let entry: Entry = deserialize(&e)?;
                last_index = i;
                last_term = entry.term;
            } else {
                break;
            }
        }
        Ok(Self { kv: store, last_index, last_term, commit_index, commit_term })
    }

    /// Appends a command to the log, returning the entry.
    pub fn append(&mut self, term: u64, command: Option<Vec<u8>>) -> Result<Entry, Error> {
        let entry = Entry { index: self.last_index + 1, term, command };
        debug!("Appending log entry {}: {:?}", entry.index, entry);
        self.kv.set(&entry.index.to_string().as_bytes(), serialize(&entry)?)?;
        self.last_index = entry.index;
        self.last_term = entry.term;
        Ok(entry)
    }

    /// Commits entries up to and including an index
    pub fn commit(&mut self, mut index: u64) -> Result<u64, Error> {
        index = std::cmp::min(index, self.last_index);
        index = std::cmp::max(index, self.commit_index);
        if index != self.commit_index {
            if let Some(entry) = self.get(index)? {
                debug!("Committing log entry {}", index);
                self.kv.set(b"commit_index", serialize(&index)?)?;
                self.kv.flush()?;
                self.commit_index = index;
                self.commit_term = entry.term;
            } else {
                return Err(Error::Internal(format!(
                    "Entry at commit index {} does not exist",
                    index
                )));
            }
        }
        Ok(index)
    }

    /// Fetches an entry at an index
    pub fn get(&self, index: u64) -> Result<Option<Entry>, Error> {
        if let Some(value) = self.kv.get(&index.to_string().as_bytes())? {
            Ok(Some(deserialize(&value)?))
        } else {
            Ok(None)
        }
    }

    /// Fetches the last committed index and term
    pub fn get_committed(&self) -> (u64, u64) {
        (self.commit_index, self.commit_term)
    }

    /// Fetches the last stored index and term
    pub fn get_last(&self) -> (u64, u64) {
        (self.last_index, self.last_term)
    }

    /// Checks if the log contains an entry
    pub fn has(&self, index: u64, term: u64) -> Result<bool, Error> {
        if index == 0 && term == 0 {
            return Ok(true);
        }
        match self.get(index)? {
            Some(ref entry) => Ok(entry.term == term),
            None => Ok(false),
        }
    }

    /// Fetches a range of entries
    // FIXME Should take all kinds of ranges (generic over std::ops::RangeBounds),
    // and use kv::Store.range() once implemented.
    pub fn range(&self, range: std::ops::RangeFrom<u64>) -> Result<Vec<Entry>, Error> {
        let mut entries = Vec::new();
        for i in range.start..=self.last_index {
            if let Some(entry) = self.get(i)? {
                entries.push(entry)
            }
        }
        Ok(entries)
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
        if index < self.commit_index {
            return Err(Error::Value("Cannot remove committed log entry".into()));
        }

        // FIXME This shouldn't rely on last_index
        for i in (index + 1)..=self.last_index {
            self.kv.delete(&i.to_string().as_bytes())?
        }
        self.last_index = std::cmp::min(index, self.last_index);
        self.last_term = self.get(self.last_index)?.map(|e| e.term).unwrap_or(0);
        Ok(self.last_index)
    }

    /// Loads information about the most recent term known by the log, containing the term number (0
    /// if none) and candidate voted for in current term (if any).
    pub fn load_term(&self) -> Result<(u64, Option<String>), Error> {
        let term = if let Some(value) = self.kv.get(b"term")? { deserialize(&value)? } else { 0 };
        let voted_for = if let Some(value) = self.kv.get(b"voted_for")? {
            Some(deserialize(&value)?)
        } else {
            None
        };
        debug!("Loaded term {} and voted_for {:?} from log", term, voted_for);
        Ok((term, voted_for))
    }

    /// Saves information about the most recent term.
    pub fn save_term(&mut self, term: u64, voted_for: Option<&str>) -> Result<(), Error> {
        if term > 0 {
            self.kv.set(b"term", serialize(&term)?)?
        } else {
            self.kv.delete(b"term")?
        }
        if let Some(v) = voted_for {
            self.kv.set(b"voted_for", serialize(&v)?)?
        } else {
            self.kv.delete(b"voted_for")?
        }
        self.kv.flush()?;
        debug!("Saved term={} and voted_for={:?}", term, voted_for);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv;
    use pretty_assertions::assert_eq;

    fn setup() -> Result<(Log<kv::Test>, kv::Test), Error> {
        let store = kv::Test::new();
        let log = Log::new(store.clone())?;
        Ok((log, store))
    }

    #[test]
    fn new() -> Result<(), Error> {
        let (l, _) = setup()?;
        assert_eq!((0, 0), l.get_last());
        assert_eq!((0, 0), l.get_committed());
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

        assert_eq!((1, 3), l.get_last());
        assert_eq!((0, 0), l.get_committed());
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
        assert_eq!((3, 2), l.get_committed());

        // The last committed entry must be persisted, to sync with state machine
        let l = Log::new(store)?;
        assert_eq!((3, 2), l.get_committed());
        Ok(())
    }

    #[test]
    fn commit_beyond() -> Result<(), Error> {
        let (mut l, store) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, None)?;
        l.append(2, Some(vec![0x03]))?;
        assert_eq!(3, l.commit(4)?);
        assert_eq!((3, 2), l.get_committed());

        // The last committed entry must be persisted, to sync with state machine
        let l = Log::new(store)?;
        assert_eq!((3, 2), l.get_committed());
        Ok(())
    }

    #[test]
    fn commit_partial() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, None)?;
        l.append(2, Some(vec![0x03]))?;
        assert_eq!(2, l.commit(2)?);
        assert_eq!((2, 2), l.get_committed());
        Ok(())
    }

    #[test]
    fn commit_reduce() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, None)?;
        l.append(2, Some(vec![0x03]))?;
        assert_eq!(2, l.commit(2)?);
        assert_eq!((2, 2), l.get_committed());

        assert_eq!(2, l.commit(1)?);
        assert_eq!((2, 2), l.get_committed());
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
    fn range() -> Result<(), Error> {
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
            l.range(0..)?
        );
        assert_eq!(
            vec![
                Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                Entry { index: 3, term: 1, command: Some(vec![0x03]) },
            ],
            l.range(2..)?
        );
        assert_eq!(Vec::<Entry>::new(), l.range(4..)?);
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
        assert_eq!(Some(Entry { index: 1, term: 1, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(Some(Entry { index: 2, term: 2, command: Some(vec![0x02]) }), l.get(2)?);
        assert_eq!(Some(Entry { index: 3, term: 3, command: Some(vec![0x03]) }), l.get(3)?);
        assert_eq!(Some(Entry { index: 4, term: 4, command: Some(vec![0x04]) }), l.get(4)?);
        assert_eq!((4, 4), l.get_last());
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
        assert_eq!(Some(Entry { index: 1, term: 4, command: Some(vec![0x0a]) }), l.get(1)?);
        assert_eq!(Some(Entry { index: 2, term: 4, command: Some(vec![0x0b]) }), l.get(2)?);
        assert_eq!((2, 4), l.get_last());
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
        assert_eq!(Some(Entry { index: 1, term: 1, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(Some(Entry { index: 2, term: 2, command: Some(vec![0x02]) }), l.get(2)?);
        assert_eq!(Some(Entry { index: 3, term: 3, command: Some(vec![0x03]) }), l.get(3)?);
        assert_eq!(Some(Entry { index: 4, term: 4, command: Some(vec![0x04]) }), l.get(4)?);
        assert_eq!((4, 4), l.get_last());
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
        assert_eq!(Some(Entry { index: 1, term: 1, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(Some(Entry { index: 2, term: 3, command: Some(vec![0x0b]) }), l.get(2)?);
        assert_eq!(Some(Entry { index: 3, term: 3, command: Some(vec![0x0c]) }), l.get(3)?);
        assert_eq!((3, 3), l.get_last());
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
        // FIXME Use range for these assertions
        assert_eq!(Some(Entry { index: 1, term: 1, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(Some(Entry { index: 2, term: 2, command: Some(vec![0x02]) }), l.get(2)?);
        assert_eq!(Some(Entry { index: 3, term: 3, command: Some(vec![0x03]) }), l.get(3)?);
        assert_eq!((3, 3), l.get_last());
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
        // FIXME Use range for these assertions
        assert_eq!(Some(Entry { index: 1, term: 1, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(Some(Entry { index: 2, term: 2, command: Some(vec![0x02]) }), l.get(2)?);
        assert_eq!(Some(Entry { index: 3, term: 3, command: Some(vec![0x03]) }), l.get(3)?);
        assert_eq!((3, 3), l.get_last());
        Ok(())
    }

    #[test]
    fn splice_overlap_inside() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(3, l.splice(vec![Entry { index: 2, term: 2, command: Some(vec![0x02]) },])?);
        assert_eq!(Some(Entry { index: 1, term: 1, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(Some(Entry { index: 2, term: 2, command: Some(vec![0x02]) }), l.get(2)?);
        assert_eq!(Some(Entry { index: 3, term: 3, command: Some(vec![0x03]) }), l.get(3)?);
        assert_eq!((3, 3), l.get_last());
        Ok(())
    }

    #[test]
    fn truncate() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(2, l.truncate(2)?);
        assert_eq!(Some(Entry { index: 1, term: 1, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(Some(Entry { index: 2, term: 2, command: Some(vec![0x02]) }), l.get(2)?);
        assert_eq!(None, l.get(3)?);
        assert_eq!((2, 2), l.get_last());
        Ok(())
    }

    #[test]
    fn truncate_beyond() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;

        assert_eq!(3, l.truncate(4)?);
        assert_eq!(Some(Entry { index: 1, term: 1, command: Some(vec![0x01]) }), l.get(1)?);
        assert_eq!(Some(Entry { index: 2, term: 2, command: Some(vec![0x02]) }), l.get(2)?);
        assert_eq!(Some(Entry { index: 3, term: 3, command: Some(vec![0x03]) }), l.get(3)?);
        assert_eq!(None, l.get(4)?);
        assert_eq!((3, 3), l.get_last());
        Ok(())
    }

    #[test]
    fn truncate_committed() -> Result<(), Error> {
        let (mut l, _) = setup()?;
        l.append(1, Some(vec![0x01]))?;
        l.append(2, Some(vec![0x02]))?;
        l.append(3, Some(vec![0x03]))?;
        l.commit(2)?;

        assert_eq!(l.truncate(1), Err(Error::Value("Cannot remove committed log entry".into())));
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
        assert_eq!(None, l.get(1)?);
        assert_eq!(None, l.get(2)?);
        assert_eq!(None, l.get(3)?);
        assert_eq!((0, 0), l.get_last());
        Ok(())
    }
}
