use super::{Scan, Store};
use crate::Error;

use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek as _, SeekFrom, Write};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

/// A hybrid log store, storing committed entries in an append-only file, uncommitted entries
/// in memory, and metadata in a separate file (should be an on-disk key-value store).
///
/// The log file contains sequential binary log entries, length-prefixed with a big-endian u64.
/// Entries are only flushed to disk when they are committed and permanent, thus the file is
/// written append-only.
///
/// An index of entry positions and sizes is maintained in memory. This is rebuilt on startup by
/// scanning the file, since maintaining the index in a separate file requires additional fsyncing
/// which is expensive. Since datasets are expected to be small, scanning the file on startup is
/// reasonably cheap.
pub struct Hybrid {
    /// The append-only log file. Protected by a mutex for interior mutability (i.e. read seeks).
    file: Mutex<File>,
    /// Index of entry locations and sizes in the log file.
    index: BTreeMap<u64, (u64, u64)>,
    /// Uncommitted log entries.
    uncommitted: VecDeque<Vec<u8>>,
    /// Metadata cache. Flushed to disk on changes.
    metadata: HashMap<Vec<u8>, Vec<u8>>,
    /// The file used to store metadata.
    /// FIXME Should be an on-disk B-tree key-value store.
    metadata_file: File,
    /// If true, fsync writes.
    sync: bool,
}

impl Hybrid {
    /// Creates or opens a new hybrid log, with files in the given directory.
    pub fn new(dir: &Path, sync: bool) -> Result<Self, Error> {
        let file =
            OpenOptions::new().read(true).write(true).create(true).open(dir.join("raft-log"))?;

        let metadata_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("raft-metadata"))?;

        Ok(Self {
            index: Self::build_index(&file)?,
            file: Mutex::new(file),
            uncommitted: VecDeque::new(),
            metadata: Self::load_metadata(&metadata_file)?,
            metadata_file,
            sync,
        })
    }

    /// Builds the index by scanning the log file.
    fn build_index(file: &File) -> Result<BTreeMap<u64, (u64, u64)>, Error> {
        let filesize = file.metadata()?.len();
        let mut bufreader = BufReader::new(file);
        let mut index = BTreeMap::new();
        let mut sizebuf = [0; 8];
        let mut pos = 0;
        let mut i = 1;
        while pos < filesize {
            bufreader.read_exact(&mut sizebuf)?;
            pos += 8;
            let size = u64::from_be_bytes(sizebuf);
            index.insert(i, (pos, size));
            let mut buf = vec![0; size as usize];
            bufreader.read_exact(&mut buf)?;
            pos += size;
            i += 1;
        }
        Ok(index)
    }

    /// Loads metadata from a file.
    fn load_metadata(file: &File) -> Result<HashMap<Vec<u8>, Vec<u8>>, Error> {
        match serde_cbor::from_reader(file) {
            Ok(metadata) => Ok(metadata),
            Err(err) if err.is_eof() => Ok(HashMap::new()),
            Err(err) => Err(err.into()),
        }
    }
}

impl Store for Hybrid {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64, Error> {
        self.uncommitted.push_back(entry);
        Ok(self.len())
    }

    fn commit(&mut self, index: u64) -> Result<(), Error> {
        if index > self.len() {
            return Err(Error::Internal(format!("Cannot commit non-existant index {}", index)));
        }
        if index < self.index.len() as u64 {
            return Err(Error::Internal(format!(
                "Cannot commit below current committed index {}",
                self.index.len() as u64
            )));
        }
        if index == self.index.len() as u64 {
            return Ok(());
        }

        let mut file = self.file.lock()?;
        let mut pos = file.seek(SeekFrom::End(0))?;
        let mut bufwriter = BufWriter::new(&mut *file);
        for i in (self.index.len() as u64 + 1)..=index {
            let entry = self
                .uncommitted
                .pop_front()
                .ok_or_else(|| Error::Internal("Unexpected end of uncommitted entries".into()))?;
            bufwriter.write_all(&entry.len().to_be_bytes())?;
            pos += 8;
            self.index.insert(i, (pos, entry.len() as u64));
            bufwriter.write_all(&entry)?;
            pos += entry.len() as u64;
        }
        bufwriter.flush()?;
        drop(bufwriter);
        if self.sync {
            file.sync_data()?;
        }
        Ok(())
    }

    fn committed(&self) -> u64 {
        self.index.len() as u64
    }

    fn get(&self, index: u64) -> Result<Option<Vec<u8>>, Error> {
        match index {
            0 => Ok(None),
            i if i <= self.index.len() as u64 => {
                let (pos, size) = self.index.get(&i).copied().ok_or_else(|| {
                    Error::Internal(format!("Indexed position not found for entry {}", i))
                })?;
                let mut entry = vec![0; size as usize];
                let mut file = self.file.lock()?;
                file.seek(SeekFrom::Start(pos))?;
                file.read_exact(&mut entry)?;
                Ok(Some(entry))
            }
            i => Ok(self.uncommitted.get(i as usize - self.index.len() - 1).cloned()),
        }
    }

    fn len(&self) -> u64 {
        self.index.len() as u64 + self.uncommitted.len() as u64
    }

    fn scan(&self, range: impl RangeBounds<u64>) -> Scan {
        let start = match range.start_bound() {
            Bound::Included(0) => 1,
            Bound::Included(n) => *n,
            Bound::Excluded(n) => *n + 1,
            Bound::Unbounded => 1,
        };
        let end = match range.end_bound() {
            Bound::Included(n) => *n,
            Bound::Excluded(0) => 0,
            Bound::Excluded(n) => *n - 1,
            Bound::Unbounded => self.len(),
        };

        let mut scan: Scan = Box::new(std::iter::empty());
        if start > end {
            return scan;
        }

        // Scan committed entries in file
        if let Some((offset, _)) = self.index.get(&start) {
            let mut file = self.file.lock().unwrap();
            file.seek(SeekFrom::Start(*offset - 8)).unwrap(); // seek to length prefix
            let mut bufreader = BufReader::new(MutexReader(file)); // FIXME Avoid MutexReader
            scan =
                Box::new(scan.chain(self.index.range(start..=end).map(move |(_, (_, size))| {
                    let mut sizebuf = vec![0; 8];
                    bufreader.read_exact(&mut sizebuf)?;
                    let mut entry = vec![0; *size as usize];
                    bufreader.read_exact(&mut entry)?;
                    Ok(entry)
                })));
        }

        // Scan uncommitted entries in memory
        if end > self.index.len() as u64 {
            scan = Box::new(
                scan.chain(
                    self.uncommitted
                        .iter()
                        .skip(start as usize - min(start as usize, self.index.len() + 1))
                        .take(end as usize - max(start as usize, self.index.len()) + 1)
                        .cloned()
                        .map(Ok),
                ),
            )
        }

        scan
    }

    fn truncate(&mut self, index: u64) -> Result<u64, Error> {
        if index < self.index.len() as u64 {
            return Err(Error::Internal(format!(
                "Cannot truncate below committed index {}",
                self.index.len() as u64
            )));
        }
        self.uncommitted.truncate(index as usize - self.index.len());
        Ok(self.len())
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.metadata.get(key).cloned())
    }

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        self.metadata.insert(key.to_vec(), value);
        self.metadata_file.set_len(0)?;
        self.metadata_file.seek(SeekFrom::Start(0))?;
        serde_cbor::to_writer(&mut self.metadata_file, &self.metadata)?;
        if self.sync {
            self.metadata_file.sync_data()?;
        }
        Ok(())
    }
}

impl Drop for Hybrid {
    /// Attempt to fsync data on drop, in case we're running without sync.
    fn drop(&mut self) {
        self.metadata_file.sync_all().ok();
        self.file.lock().map(|f| f.sync_all()).ok();
    }
}

struct MutexReader<'a>(MutexGuard<'a, File>);

impl<'a> Read for MutexReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

#[cfg(test)]
impl super::TestSuite<Hybrid> for Hybrid {
    fn setup() -> Result<Self, Error> {
        let dir = tempdir::TempDir::new("toydb")?;
        Hybrid::new(dir.as_ref(), false)
    }
}

#[test]
fn tests() -> Result<(), Error> {
    use super::TestSuite;
    Hybrid::test()
}

#[test]
fn test_persistent() -> Result<(), Error> {
    let dir = tempdir::TempDir::new("toydb")?;
    let mut l = Hybrid::new(dir.as_ref(), true)?;

    l.append(vec![0x01])?;
    l.append(vec![0x02])?;
    l.append(vec![0x03])?;
    l.append(vec![0x04])?;
    l.append(vec![0x05])?;
    l.commit(3)?;

    let l = Hybrid::new(dir.as_ref(), true)?;

    assert_eq!(vec![vec![1], vec![2], vec![3]], l.scan(..).collect::<Result<Vec<_>, Error>>()?);

    Ok(())
}
