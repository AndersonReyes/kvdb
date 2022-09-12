use crate::defs::KvdbError;
use crate::defs::{LogOffset, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const FILENAME: &str = "log.ndjson";
const LOG_START_BYTE: LogOffset = 0;

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum LogEntry {
    Set { key: String, value: String },
    Remove { key: String },
}

/// Commit log, writes every remove or insert to disk
pub struct Log {
    write_handle: File,
    /// Maps key to latest mutation start byte(line) in file.
    key_to_offset_cache: HashMap<String, LogOffset>,
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
        let filepath = path.into().join(Path::new(FILENAME));
        let mut write_handle = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&filepath)
            .map_err(|e| KvdbError::IO(e))?;

        write_handle.seek(SeekFrom::Start(LOG_START_BYTE))?;

        Ok(Self {
            write_handle,
            key_to_offset_cache: HashMap::new(),
        })
    }

    /// Write the log entry to disk, returning the bytes offset of the
    /// write.
    fn commit(&mut self, entry: LogEntry) -> Result<LogOffset> {
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

    pub fn commit_set(&mut self, key: &String, value: &String) -> Result<()> {
        let offset = self.commit(LogEntry::Set {
            key: key.clone(),
            value: value.clone(),
        })?;
        self.key_to_offset_cache.insert(key.to_owned(), offset);

        Ok(())
    }

    pub fn commit_rm(&mut self, key: &String) -> Result<()> {
        self.commit(LogEntry::Remove { key: key.clone() })?;
        Ok(())
    }

    /// Iterate log from start to end
    pub fn iter(&self) -> LogFileIterator {
        let mut reader = BufReader::new(&self.write_handle);
        reader
            .seek(SeekFrom::Start(LOG_START_BYTE))
            .expect("Failed to set reader seek to beginning of file");
        LogFileIterator { reader }
    }

    pub fn get(&mut self, key: &String) -> Result<Option<LogEntry>> {
        match self.key_to_offset_cache.get(key) {
            None => Ok(None),
            Some(&offset) => {
                let mut reader = BufReader::new(&self.write_handle);
                reader.seek(SeekFrom::Start(offset))?;

                Ok(LogFileIterator { reader }.next().map(|(entry, _)| entry))
            }
        }
    }
}
