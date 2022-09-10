use crate::defs::Result;
use crate::log::{Log, LogEntry};
use crate::KvdbError;
use std::collections::HashMap;
use std::path::{PathBuf};

/// In memory key value store using hashmap
pub struct KvStore {
    memory: HashMap<String, String>,
    commit_log: Log,
}

impl KvStore {
    /// Open db
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut store = Log::open(path).map(|log| Self {
            memory: HashMap::new(),
            commit_log: log,
        })?;

        store.hydrate_db()?;

        Ok(store)
    }

    /// Replay commit log to Update db state
    fn hydrate_db(&mut self) -> Result<()> {
        let entries = self.commit_log.replay()?;

        for entry in entries {
            match entry {
                LogEntry::Remove { key: k } => {
                    self.remove(k).expect("Failed to replay log");
                }
                LogEntry::Set { key: k, value: v } => {
                    self.set(k, v).expect("Failed to replay log");
                }
            };
        }

        Ok(())
    }

    /// remove value at key
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.memory.contains_key(&key) {
            return Err(KvdbError::KvdbError(String::from(
                "Key not found",
            )));
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
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.memory.get(key.as_str()).cloned())
    }
}
