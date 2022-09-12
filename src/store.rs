use crate::defs::Result;
use crate::log::{Log, LogEntry, LogFileIterator};
use crate::KvdbError;
use std::collections::HashMap;
use std::path::PathBuf;

/// In memory key value store using hashmap
pub struct KvStore {
    memory: HashMap<String, String>,
    commit_log: Log,
}

impl KvStore {
    /// Open db
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let commit_log = Log::open(path)?;
        let mut memory: HashMap<String, String> = HashMap::new();
        KvStore::hydrate_memory(&mut memory, commit_log.iter())?;
        Ok(Self { memory, commit_log })
    }

    /// Replay commit log to Update memory state
    fn hydrate_memory(
        memory: &mut HashMap<String, String>,
        entries: LogFileIterator,
    ) -> Result<()> {
        for (entry, _) in entries {
            match entry {
                LogEntry::Remove { key: k } => {
                    if memory.contains_key(&k) {
                        memory.remove(&k);
                    }
                }
                LogEntry::Set { key: k, value: v } => {
                    memory.insert(k, v);
                }
            };
        }

        Ok(())
    }

    /// remove value at key
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.memory.contains_key(&key) {
            return Err(KvdbError::KvdbError(String::from("Key not found")));
        }

        self.commit_log.commit_rm(&key)?;
        self.memory.remove(key.as_str());
        Ok(())
    }

    /// set value at key, overwrites existing
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.commit_log.commit_set(&key, &value)?;
        self.memory.insert(key, value);
        Ok(())
    }

    /// get value at key, returns None if it doesn't exist
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.memory.get(&key).map(|s| s.clone()))
    }
}
