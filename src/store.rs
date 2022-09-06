use std::collections::HashMap;

/// In memory key value store using hashmap
pub struct KvStore {
    memory: HashMap<String, String>,
}

impl KvStore {
    /// Create a new KvStore
    pub fn new() -> Self {
        Self {
            memory: HashMap::new(),
        }
    }

    /// remove value at key
    pub fn remove(&mut self, key: String) {
        self.memory.remove(key.as_str());
    }

    /// set value at key, overwrites existing
    pub fn set(&mut self, key: String, value: String) {
        self.memory.insert(key, value);
    }

    /// get value at key, returns None if it doesn't exist
    pub fn get(&self, key: String) -> Option<String> {
        self.memory.get(key.as_str()).cloned()
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}
