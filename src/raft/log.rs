use super::State;
use crate::kv;
use crate::utility::{deserialize, serialize};
use crate::Error;

/// A replicated log entry
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// The term in which the entry was added
    pub term: u64,
    /// The state machine command. None is used to commit noops during leader election.
    pub command: Option<Vec<u8>>,
}

/// The replicated Raft log
pub struct Log<S: kv::storage::Storage> {
    /// The underlying key-value store
    kv: kv::Simple<S>,
    /// The index of the last stored entry.
    last_index: u64,
    /// The term of the last stored entry.
    last_term: u64,
    /// The last entry known to be committed. Not persisted,
    /// since leaders will determine this when they're elected.
    commit_index: u64,
    /// The term of the last committed entry.
    commit_term: u64,
    /// The last entry applied to the state machine. This is
    /// persisted, since the state machine is also persisted.
    apply_index: u64,
    /// The term of the last applied entry.
    apply_term: u64,
}

impl<S: kv::storage::Storage> Log<S> {
    /// Creates a new log, using a kv::Store for storage.
    pub fn new(store: kv::Simple<S>) -> Result<Self, Error> {
        let apply_index = match store.get(b"apply_index")? {
            Some(v) => deserialize(&v)?,
            None => 0,
        };
        let (commit_index, commit_term) = match store.get(&apply_index.to_string().as_bytes())? {
            Some(raw_entry) => (apply_index, deserialize::<Entry>(&raw_entry)?.term),
            None if apply_index == 0 => (0, 0),
            None => {
                return Err(Error::Internal(format!("Applied entry {} not found", apply_index)))
            }
        };
        let apply_term = commit_term;
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
        Ok(Self {
            kv: store,
            last_index,
            last_term,
            commit_index,
            commit_term,
            apply_index,
            apply_term,
        })
    }

    /// Appends an entry to the log
    pub fn append(&mut self, entry: Entry) -> Result<u64, Error> {
        debug!("Appending log entry {}: {:?}", self.last_index + 1, entry);
        let index = self.last_index + 1;
        self.last_index = index;
        self.last_term = entry.term;
        self.kv.set(&index.to_string().as_bytes(), serialize(&entry)?)?;
        Ok(index)
    }

    /// Applies the next committed entry to the state machine, if any. Returns the applied entry
    /// index and its result, or None if no entry. If the state machine returns Error::Internal, the
    /// entry is not applied and the error is propagated. If the state machine returns any other
    /// error, the entry is applied and the error returned to the caller.
    #[allow(clippy::borrowed_box)] // Currently this is correct
    #[allow(clippy::type_complexity)]
    pub fn apply(
        &mut self,
        state: &mut impl State,
    ) -> Result<Option<(u64, Result<Vec<u8>, Error>)>, Error> {
        if self.apply_index >= self.commit_index {
            return Ok(None);
        }

        let mut output = Ok(vec![]);
        if let Some(entry) = self.get(self.apply_index + 1)? {
            debug!("Applying log entry {}: {:?}", self.apply_index + 1, entry);
            if let Some(command) = entry.command {
                output = match state.mutate(command) {
                    Err(err @ Error::Internal(_)) => return Err(err),
                    o => o,
                }
            }
            self.apply_index += 1;
            self.apply_term = entry.term;
        }
        self.kv.set(b"apply_index", serialize(&self.apply_index)?)?;
        Ok(Some((self.apply_index, output)))
    }

    /// Commits entries up to and including an index
    pub fn commit(&mut self, mut index: u64) -> Result<u64, Error> {
        index = std::cmp::min(index, self.last_index);
        index = std::cmp::max(index, self.commit_index);
        if index != self.commit_index {
            if let Some(entry) = self.get(index)? {
                debug!("Committing log entry {}", index);
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

    /// Fetches the last applied index and term
    pub fn get_applied(&self) -> (u64, u64) {
        (self.apply_index, self.apply_term)
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

    /// Splices a set of entries onto an offset, with Raft-specific semantics:
    ///
    /// * If the base and base term does not match an existing entry, return error
    /// * If no existing entry exists at an index, append it
    /// * If the existing entry has a different term, replace it and following entries
    /// * If the existing entry has the same term, assume entry is equal and skip it
    ///
    /// The caller must have checked that the base is valid first (i.e. that it exists
    /// and has the correct term).
    //
    // FIXME Should be atomic
    pub fn splice(&mut self, base: u64, entries: Vec<Entry>) -> Result<u64, Error> {
        for (i, entry) in entries.into_iter().enumerate() {
            if let Some(ref current) = self.get(base + i as u64 + 1)? {
                if current.term == entry.term {
                    continue;
                }
                self.truncate(base + i as u64)?;
            }
            self.append(entry)?;
        }
        Ok(self.last_index)
    }

    /// Truncates the log such that its last item is at most index.
    /// Refuses to remove entries that have been applied or committed.
    pub fn truncate(&mut self, index: u64) -> Result<u64, Error> {
        debug!("Truncating log from entry {}", index);
        if index < self.apply_index {
            return Err(Error::Value("Cannot remove applied log entry".into()));
        } else if index < self.commit_index {
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

    /// Loads information about the most recent term known by the log,
    /// containing the term number (0 if none) and candidate voted for
    /// in current term (if any).
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
    // FIXME Should be transactional.
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
        debug!("Saved term={} and voted_for={:?}", term, voted_for);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::TestState;
    use super::*;

    fn setup() -> (Log<kv::storage::Test>, kv::Simple<kv::storage::Test>) {
        let backend = kv::storage::Test::new();
        let log = Log::new(kv::Simple::new(backend.clone())).unwrap();
        let store = kv::Simple::new(backend);
        (log, store)
    }

    #[test]
    fn new() {
        let (l, _) = setup();
        assert_eq!((0, 0), l.get_last());
        assert_eq!((0, 0), l.get_committed());
        assert_eq!((0, 0), l.get_applied());
        assert_eq!(None, l.get(1).unwrap());
    }

    #[test]
    fn append() {
        let (mut l, _) = setup();
        assert_eq!(Ok(None), l.get(1));

        assert_eq!(Ok(1), l.append(Entry { term: 3, command: Some(vec![0x01]) }));
        assert_eq!(Ok(Some(Entry { term: 3, command: Some(vec![0x01]) })), l.get(1));
        assert_eq!(Ok(None), l.get(2));

        assert_eq!((1, 3), l.get_last());
        assert_eq!((0, 0), l.get_committed());
        assert_eq!((0, 0), l.get_applied());
    }

    #[test]
    fn append_none_command() {
        let (mut l, _) = setup();
        assert_eq!(Ok(1), l.append(Entry { term: 3, command: None }));
        assert_eq!(Ok(Some(Entry { term: 3, command: None })), l.get(1));
    }

    #[test]
    fn append_persistence() {
        let (mut l, store) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: None }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x03]) }).unwrap();

        let l = Log::new(store).unwrap();
        assert_eq!(Ok(Some(Entry { term: 1, command: Some(vec![0x01]) })), l.get(1));
        assert_eq!(Ok(Some(Entry { term: 2, command: None })), l.get(2));
        assert_eq!(Ok(Some(Entry { term: 2, command: Some(vec![0x03]) })), l.get(3));
    }

    #[test]
    fn apply() {
        let (mut l, store) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: None }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x03]) }).unwrap();
        l.commit(3).unwrap();

        let mut state = TestState::new();
        assert_eq!(Ok(Some((1, Ok(vec![0xff, 0x01])))), l.apply(&mut state));
        assert_eq!((1, 1), l.get_applied());
        assert_eq!(vec![vec![0x01],], state.list());

        assert_eq!(Ok(Some((2, Ok(vec![])))), l.apply(&mut state));
        assert_eq!((2, 2), l.get_applied());
        assert_eq!(vec![vec![0x01],], state.list());

        assert_eq!(Ok(Some((3, Ok(vec![0xff, 0x03])))), l.apply(&mut state));
        assert_eq!((3, 2), l.get_applied());
        assert_eq!(vec![vec![0x01], vec![0x03]], state.list());

        assert_eq!(Ok(None), l.apply(&mut state));
        assert_eq!((3, 2), l.get_applied());
        assert_eq!(vec![vec![0x01], vec![0x03]], state.list());

        // The last applied entry should be persisted, and also used for last committed
        let l = Log::new(store).unwrap();
        assert_eq!((3, 2), l.get_last());
        assert_eq!((3, 2), l.get_committed());
        assert_eq!((3, 2), l.get_applied());
    }

    #[test]
    fn apply_committed_only() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: None }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x03]) }).unwrap();
        l.commit(1).unwrap();

        let mut state = TestState::new();
        assert_eq!(Ok(Some((1, Ok(vec![0xff, 0x01])))), l.apply(&mut state));
        assert_eq!((1, 1), l.get_applied());
        assert_eq!(vec![vec![0x01],], state.list());

        assert_eq!(Ok(None), l.apply(&mut state));
        assert_eq!((1, 1), l.get_applied());
        assert_eq!(vec![vec![0x01],], state.list());
    }

    #[test]
    fn apply_errors() -> Result<(), Error> {
        let (mut l, store) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) })?;
        l.append(Entry { term: 1, command: Some(vec![0xff]) })?; // Error::Value
        l.append(Entry { term: 1, command: Some(vec![0x03]) })?;
        l.append(Entry { term: 1, command: Some(vec![0x04, 0x04]) })?; // Error::Internal
        l.append(Entry { term: 1, command: Some(vec![0x05]) })?;
        l.commit(5)?;
        assert_eq!((5, 1), l.get_committed(), "committed entry");

        let mut state = TestState::new();
        assert_eq!(Ok(Some((1, Ok(vec![0xff, 0x01])))), l.apply(&mut state));
        assert_eq!((1, 1), l.get_applied());
        assert_eq!(vec![vec![0x01],], state.list());

        assert_eq!(
            Ok(Some((2, Err(Error::Value("Command cannot be 0xff".into()))))),
            l.apply(&mut state)
        );
        assert_eq!((2, 1), l.get_applied());
        assert_eq!(vec![vec![0x01],], state.list());

        assert_eq!(Ok(Some((3, Ok(vec![0xff, 0x03])))), l.apply(&mut state));
        assert_eq!((3, 1), l.get_applied());
        assert_eq!(vec![vec![0x01], vec![0x03]], state.list());

        assert_eq!(Err(Error::Internal("Command must be 1 byte".into())), l.apply(&mut state));
        assert_eq!((3, 1), l.get_applied());
        assert_eq!(vec![vec![0x01], vec![0x03]], state.list());

        assert_eq!(Err(Error::Internal("Command must be 1 byte".into())), l.apply(&mut state));
        assert_eq!((3, 1), l.get_applied());
        assert_eq!(vec![vec![0x01], vec![0x03]], state.list());

        // The last applied entry should be persisted, and also used for last committed,
        // and the erroring log entry should still be there.
        let mut l = Log::new(store).unwrap();
        assert_eq!((5, 1), l.get_last(), "last entry");
        assert_eq!((3, 1), l.get_committed(), "committed entry");
        assert_eq!((3, 1), l.get_applied(), "applied entry");
        l.commit(4)?;
        assert_eq!(Err(Error::Internal("Command must be 1 byte".into())), l.apply(&mut state));
        Ok(())
    }

    #[test]
    fn commit() {
        let (mut l, store) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: None }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x03]) }).unwrap();
        assert_eq!(Ok(3), l.commit(3));
        assert_eq!((3, 2), l.get_committed());

        // The last committed entry should not be persisted
        let l = Log::new(store).unwrap();
        assert_eq!((0, 0), l.get_committed());
    }

    #[test]
    fn commit_beyond() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: None }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x03]) }).unwrap();
        assert_eq!(Ok(3), l.commit(4));
        assert_eq!((3, 2), l.get_committed());
    }

    #[test]
    fn commit_partial() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: None }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x03]) }).unwrap();
        assert_eq!(Ok(2), l.commit(2));
        assert_eq!((2, 2), l.get_committed());
    }

    #[test]
    fn commit_reduce() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: None }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x03]) }).unwrap();
        assert_eq!(Ok(2), l.commit(2));
        assert_eq!((2, 2), l.get_committed());

        assert_eq!(Ok(2), l.commit(1));
        assert_eq!((2, 2), l.get_committed());
    }

    #[test]
    fn get() {
        let (mut l, _) = setup();
        assert_eq!(Ok(None), l.get(1));

        l.append(Entry { term: 3, command: Some(vec![0x01]) }).unwrap();
        assert_eq!(Ok(Some(Entry { term: 3, command: Some(vec![0x01]) })), l.get(1));
        assert_eq!(Ok(None), l.get(2));
    }

    #[test]
    fn has() {
        let (mut l, _) = setup();
        l.append(Entry { term: 2, command: Some(vec![0x01]) }).unwrap();

        assert_eq!(true, l.has(1, 2).unwrap());
        assert_eq!(true, l.has(0, 0).unwrap());
        assert_eq!(false, l.has(0, 1).unwrap());
        assert_eq!(false, l.has(1, 0).unwrap());
        assert_eq!(false, l.has(1, 3).unwrap());
        assert_eq!(false, l.has(2, 0).unwrap());
        assert_eq!(false, l.has(2, 1).unwrap());
    }

    #[test]
    fn range() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 1, command: Some(vec![0x02]) }).unwrap();
        l.append(Entry { term: 1, command: Some(vec![0x03]) }).unwrap();

        assert_eq!(
            Ok(vec![
                Entry { term: 1, command: Some(vec![0x01]) },
                Entry { term: 1, command: Some(vec![0x02]) },
                Entry { term: 1, command: Some(vec![0x03]) },
            ]),
            l.range(0..)
        );
        assert_eq!(
            Ok(vec![
                Entry { term: 1, command: Some(vec![0x02]) },
                Entry { term: 1, command: Some(vec![0x03]) },
            ]),
            l.range(2..)
        );
        assert_eq!(Ok(vec![]), l.range(4..));
    }

    #[test]
    fn load_save_term() {
        // Test loading empty term
        let (l, _) = setup();
        assert_eq!(Ok((0, None)), l.load_term());

        // Test loading saved term
        let (mut l, store) = setup();
        assert_eq!(Ok(()), l.save_term(1, Some("a")));
        let l = Log::new(store).unwrap();
        assert_eq!(Ok((1, Some("a".into()))), l.load_term());

        // Test replacing saved term with none
        let (mut l, _) = setup();
        assert_eq!(Ok(()), l.save_term(1, Some("a")));
        assert_eq!(Ok((1, Some("a".into()))), l.load_term());
        assert_eq!(Ok(()), l.save_term(0, None));
        assert_eq!(Ok((0, None)), l.load_term());
    }

    #[test]
    fn splice() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x02]) }).unwrap();
        l.append(Entry { term: 3, command: Some(vec![0x03]) }).unwrap();

        assert_eq!(
            Ok(4),
            l.splice(
                2,
                vec![
                    Entry { term: 3, command: Some(vec![0x03]) },
                    Entry { term: 4, command: Some(vec![0x04]) },
                ]
            )
        );
        assert_eq!(Ok(Some(Entry { term: 1, command: Some(vec![0x01]) })), l.get(1));
        assert_eq!(Ok(Some(Entry { term: 2, command: Some(vec![0x02]) })), l.get(2));
        assert_eq!(Ok(Some(Entry { term: 3, command: Some(vec![0x03]) })), l.get(3));
        assert_eq!(Ok(Some(Entry { term: 4, command: Some(vec![0x04]) })), l.get(4));
        assert_eq!((4, 4), l.get_last());
    }

    #[test]
    fn splice_all() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x02]) }).unwrap();
        l.append(Entry { term: 3, command: Some(vec![0x03]) }).unwrap();

        assert_eq!(
            Ok(2),
            l.splice(
                0,
                vec![
                    Entry { term: 4, command: Some(vec![0x0a]) },
                    Entry { term: 4, command: Some(vec![0x0b]) },
                ]
            )
        );
        assert_eq!(Ok(Some(Entry { term: 4, command: Some(vec![0x0a]) })), l.get(1));
        assert_eq!(Ok(Some(Entry { term: 4, command: Some(vec![0x0b]) })), l.get(2));
        assert_eq!((2, 4), l.get_last());
    }

    #[test]
    fn splice_append() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x02]) }).unwrap();

        assert_eq!(
            Ok(4),
            l.splice(
                2,
                vec![
                    Entry { term: 3, command: Some(vec![0x03]) },
                    Entry { term: 4, command: Some(vec![0x04]) },
                ]
            )
        );
        assert_eq!(Ok(Some(Entry { term: 1, command: Some(vec![0x01]) })), l.get(1));
        assert_eq!(Ok(Some(Entry { term: 2, command: Some(vec![0x02]) })), l.get(2));
        assert_eq!(Ok(Some(Entry { term: 3, command: Some(vec![0x03]) })), l.get(3));
        assert_eq!(Ok(Some(Entry { term: 4, command: Some(vec![0x04]) })), l.get(4));
        assert_eq!((4, 4), l.get_last());
    }

    #[test]
    fn splice_conflict_term() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x02]) }).unwrap();
        l.append(Entry { term: 3, command: Some(vec![0x03]) }).unwrap();
        l.append(Entry { term: 4, command: Some(vec![0x04]) }).unwrap();

        assert_eq!(
            Ok(3),
            l.splice(
                1,
                vec![
                    Entry { term: 3, command: Some(vec![0x0b]) },
                    Entry { term: 3, command: Some(vec![0x0c]) }
                ]
            )
        );
        assert_eq!(Ok(Some(Entry { term: 1, command: Some(vec![0x01]) })), l.get(1));
        assert_eq!(Ok(Some(Entry { term: 3, command: Some(vec![0x0b]) })), l.get(2));
        assert_eq!(Ok(Some(Entry { term: 3, command: Some(vec![0x0c]) })), l.get(3));
        assert_eq!((3, 3), l.get_last());
    }

    #[test]
    fn splice_overlap_inside() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x02]) }).unwrap();
        l.append(Entry { term: 3, command: Some(vec![0x03]) }).unwrap();

        assert_eq!(Ok(3), l.splice(1, vec![Entry { term: 2, command: Some(vec![0x02]) },]));
        assert_eq!(Ok(Some(Entry { term: 1, command: Some(vec![0x01]) })), l.get(1));
        assert_eq!(Ok(Some(Entry { term: 2, command: Some(vec![0x02]) })), l.get(2));
        assert_eq!(Ok(Some(Entry { term: 3, command: Some(vec![0x03]) })), l.get(3));
        assert_eq!((3, 3), l.get_last());
    }

    #[test]
    fn truncate() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x02]) }).unwrap();
        l.append(Entry { term: 3, command: Some(vec![0x03]) }).unwrap();

        assert_eq!(Ok(2), l.truncate(2));
        assert_eq!(Ok(Some(Entry { term: 1, command: Some(vec![0x01]) })), l.get(1));
        assert_eq!(Ok(Some(Entry { term: 2, command: Some(vec![0x02]) })), l.get(2));
        assert_eq!(Ok(None), l.get(3));
        assert_eq!((2, 2), l.get_last());
    }

    #[test]
    fn truncate_beyond() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x02]) }).unwrap();
        l.append(Entry { term: 3, command: Some(vec![0x03]) }).unwrap();

        assert_eq!(Ok(3), l.truncate(4));
        assert_eq!(Ok(Some(Entry { term: 1, command: Some(vec![0x01]) })), l.get(1));
        assert_eq!(Ok(Some(Entry { term: 2, command: Some(vec![0x02]) })), l.get(2));
        assert_eq!(Ok(Some(Entry { term: 3, command: Some(vec![0x03]) })), l.get(3));
        assert_eq!(Ok(None), l.get(4));
        assert_eq!((3, 3), l.get_last());
    }

    #[test]
    fn truncate_committed() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x02]) }).unwrap();
        l.append(Entry { term: 3, command: Some(vec![0x03]) }).unwrap();
        l.commit(2).unwrap();

        assert_matches!(l.truncate(1), Err(Error::Value(_)));
        assert_eq!(l.truncate(2), Ok(2));
    }

    #[test]
    fn truncate_zero() {
        let (mut l, _) = setup();
        l.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        l.append(Entry { term: 2, command: Some(vec![0x02]) }).unwrap();
        l.append(Entry { term: 3, command: Some(vec![0x03]) }).unwrap();

        assert_eq!(Ok(0), l.truncate(0));
        assert_eq!(Ok(None), l.get(1));
        assert_eq!(Ok(None), l.get(2));
        assert_eq!(Ok(None), l.get(3));
        assert_eq!((0, 0), l.get_last());
    }
}
