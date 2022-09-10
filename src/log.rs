use crate::defs::KvdbError;
use crate::defs::{LogOffset, Result};
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

pub struct LogFileIterator<'a> {
    reader: BufReader<&'a File>,
}

impl Iterator for LogFileIterator<'_> {
    type Item = (LogEntry, LogOffset);

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.reader.seek(SeekFrom::Current(0)).expect("seek()");
        let mut data: Vec<u8> = Vec::new();

        self.reader.seek(SeekFrom::Start(offset)).expect("seek()");
        let num_bytes = self
            .reader
            .read_until(b'\n', &mut data)
            .expect("failed to read log record");

        match num_bytes {
            0 => None,
            _ => {
                let as_str = String::from_utf8(data).expect("failed to parse bytes to string");
                let entry =
                    serde_json::from_str::<LogEntry>(&as_str).expect("failed to parse log record");

                Some((entry, offset))
            }
        }
    }
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

    /// Write the log entry to disk, returning the bytes offset of the
    /// write.
    fn commit(&mut self, entry: LogEntry) -> Result<u64> {
        self.write_handle.seek(SeekFrom::End(0))?;

        serde_json::to_string(&entry)
            .map(|mut data| {
                data.push('\n');
                data.into_bytes()
            })
            .map_err(|e| KvdbError::SerializationError(e))
            .and_then(|bytes| {
                let n: u64 = bytes.len() as u64;
                self.write_handle
                    .write_all(&bytes)
                    .map(|_| n)
                    .map_err(|e| KvdbError::IO(e))
            })
            .map(|bytes_written| {
                self.write_handle
                    .seek(SeekFrom::Current(0))
                    .expect("Failed to get log position")
                    - bytes_written
            })
    }

    pub fn commit_set(&mut self, key: &String, value: &String) -> Result<u64> {
        self.commit(LogEntry::Set {
            key: key.clone(),
            value: value.clone(),
        })
    }

    pub fn commit_rm(&mut self, key: &String) -> Result<u64> {
        self.commit(LogEntry::Remove { key: key.clone() })
    }

    /// Iterate log from start to end
    pub fn iter(&self) -> LogFileIterator {
        let mut reader = BufReader::new(&self.write_handle);
        reader
            .seek(SeekFrom::Start(0))
            .expect("Failed to set reader seek to beginning of file");
        LogFileIterator { reader }
    }

    pub fn read_at(&mut self, offset: LogOffset) -> Result<LogEntry> {
        let mut reader = BufReader::new(&self.write_handle);
        reader.seek(SeekFrom::Start(offset))?;

        LogFileIterator { reader }
            .next()
            .map(|(entry, _)| entry)
            .ok_or(KvdbError::KvdbError(format!(
                "Failed to read log at offset {}",
                offset
            )))
    }
}
