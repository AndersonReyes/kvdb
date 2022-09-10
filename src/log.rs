use crate::defs::KvdbError;
use crate::defs::Result;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const FILENAME: &str = "kvdb.db";

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum LogEntry {
    Set { key: String, value: String },
    Remove { key: String },
}

pub struct Log {
    write_handle: File,
}

/// implements db log. Stores writes and removals to disk.
/// TODO: Right now we store ndjson records to write the log, explore other serialization formats?
impl Log {
    /// Open db log file
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        let filepath: PathBuf = if path.is_dir() {
            path.join(Path::new(FILENAME))
        } else {
            // assume is a full file path
            path
        };

        let write_handle = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&filepath)
            .map_err(|e| KvdbError::IO(e))?;

        Ok(Self { write_handle })
    }

    fn commit(&mut self, entry: LogEntry) -> Result<()> {
        serde_json::to_string(&entry)
            .map(|mut data| {
                data.push('\n');
                data.into_bytes()
            })
            .map_err(|e| KvdbError::SerializationError(e))
            .and_then(|bytes| {
                self.write_handle
                    .write_all(&bytes)
                    .map_err(|e| KvdbError::IO(e))
            })
            .map(|_| ())
    }

    pub fn commit_set(&mut self, key: &String, value: &String) -> Result<()> {
        self.commit(LogEntry::Set {
            key: key.clone(),
            value: value.clone(),
        })
    }

    pub fn commit_rm(&mut self, key: &String) -> Result<()> {
        self.commit(LogEntry::Remove { key: key.clone() })
    }

    pub fn replay(&self) -> Result<impl Iterator<Item = LogEntry> + '_> {
        Ok(BufReader::new(&self.write_handle)
            .lines()
            .map(|l| l.expect("Failed to read db file"))
            .map(|l| serde_json::from_str::<LogEntry>(l.as_str()).expect("corrupted db file")))
    }

    pub fn reset_seek(&mut self) -> Result<()> {
        self.write_handle
            .seek(SeekFrom::End(0))
            .map_err(KvdbError::from)?;

        Ok(())
    }
}
