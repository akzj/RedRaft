//! Shard data structure with metadata
//!
//! Contains both data and metadata (like Raft apply index) for each shard
//!
//! ShardData contains all data structures (List, Set, ZSet) in a unified HashMap

use std::collections::HashMap;
use bytes::Bytes;

use crate::{
    memory::bitmap::BitmapData,
    memory::{ListData, SetData, ZSetData},
};

/// Unified data type enum (no COW, simple clone)
///
/// Wraps all Redis data types (List, Set, ZSet, Bitmap) in a single enum.
#[derive(Debug, Clone)]
pub enum Data {
    /// List data
    List(ListData),
    /// Set data
    Set(SetData),
    /// ZSet data
    ZSet(ZSetData),
    /// Bitmap data
    Bitmap(BitmapData),
}

// Keep DataCow as alias for backward compatibility during migration
pub type DataCow = Data;

impl Data {
    /// Get the type name of the data
    pub fn type_name(&self) -> &'static str {
        match self {
            Data::List(_) => "list",
            Data::Set(_) => "set",
            Data::ZSet(_) => "zset",
            Data::Bitmap(_) => "bitmap",
        }
    }

    /// Check if the data is empty
    pub fn is_empty(&self) -> bool {
        match self {
            Data::List(l) => l.is_empty(),
            Data::Set(s) => s.is_empty(),
            Data::ZSet(z) => z.is_empty(),
            Data::Bitmap(b) => b.is_empty(),
        }
    }

    /// Get length/size of the data
    pub fn len(&self) -> usize {
        match self {
            Data::List(l) => l.len(),
            Data::Set(s) => s.len(),
            Data::ZSet(z) => z.len(),
            Data::Bitmap(b) => b.len(),
        }
    }

    /// Serialize data for snapshot
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        use bincode::config::standard;
        use bincode::serde::encode_to_vec;

        match self {
            Data::List(list) => encode_to_vec(list, standard())
                .map_err(|e| format!("Failed to serialize ListData: {}", e)),
            Data::Set(set) => encode_to_vec(set, standard())
                .map_err(|e| format!("Failed to serialize SetData: {}", e)),
            Data::ZSet(zset) => encode_to_vec(zset, standard())
                .map_err(|e| format!("Failed to serialize ZSetData: {}", e)),
            Data::Bitmap(bitmap) => encode_to_vec(bitmap, standard())
                .map_err(|e| format!("Failed to serialize BitmapData: {}", e)),
        }
    }
}

/// Unified Memory Store (simple HashMap, no COW)
///
/// Combines all data types (List, Set, ZSet, Bitmap) in a single store.
/// Simple clone-based approach for snapshots.
#[derive(Debug, Clone)]
pub struct MemStore {
    /// Unified storage for all data types: key -> Data
    pub(crate) data: HashMap<Vec<u8>, Data>,
}

// Keep MemStoreCow as alias for backward compatibility during migration
pub type MemStoreCow = MemStore;

impl MemStore {
    /// Create a new empty unified store
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Get key type (O(1) lookup)
    pub fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        self.data.get(key).map(|data| data.type_name())
    }

    /// Check if key exists (O(1) lookup)
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    /// Delete key from any data type
    pub fn del(&mut self, key: &[u8]) -> bool {
        self.data.remove(key).is_some()
    }

    /// Get Data for key (returns cloned value)
    pub fn get(&self, key: &[u8]) -> Option<Data> {
        self.data.get(key).cloned()
    }

    /// Get mutable Data for key
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Data> {
        self.data.get_mut(key)
    }

    /// Insert or update Data for key
    ///
    /// If key exists with different type, it will be replaced.
    pub fn insert(&mut self, key: Vec<u8>, data: Data) {
        self.data.insert(key, data);
    }

    /// Get total key count across all data types
    pub fn key_count(&self) -> usize {
        self.data.len()
    }

    /// Check if store is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Iterate over all key-value pairs
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Data)> {
        self.data.iter()
    }

    // Set operations (simplified, no COW)
    /// Add a member to a Set
    pub fn add(&mut self, key: Vec<u8>, member: Bytes) -> crate::traits::StoreResult<bool> {
        use crate::traits::{StoreError, StoreResult};
        
        // Get or create Set
        let set = match self.data.get_mut(&key) {
            Some(Data::Set(set)) => set,
            Some(_) => return Err(StoreError::WrongType),
            None => {
                let new_set = SetData::new();
                self.data.insert(key.clone(), Data::Set(new_set));
                match self.data.get_mut(&key) {
                    Some(Data::Set(set)) => set,
                    _ => return Err(StoreError::Internal("Internal error".to_string())),
                }
            }
        };
        
        Ok(set.add(member))
    }

    /// Remove a member from a Set
    pub fn remove(&mut self, key: &[u8], member: &[u8]) -> crate::traits::StoreResult<bool> {
        use crate::traits::{StoreError, StoreResult};
        
        match self.data.get_mut(key) {
            Some(Data::Set(set)) => Ok(set.remove(member)),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(false),
        }
    }

    /// Check if member exists in a Set
    pub fn contains(&self, key: &[u8], member: &[u8]) -> bool {
        match self.data.get(key) {
            Some(Data::Set(set)) => set.contains(member),
            _ => false,
        }
    }

    /// Get member count for a Set
    pub fn len(&self, key: &[u8]) -> Option<usize> {
        match self.data.get(key) {
            Some(Data::Set(set)) => Some(set.len()),
            _ => None,
        }
    }

    /// Clear Set for key
    pub fn clear(&mut self, key: &[u8]) -> bool {
        match self.data.get(key) {
            Some(Data::Set(_)) => {
                self.data.remove(key).is_some()
            }
            _ => false,
        }
    }

    // List operations (simplified, no COW)
    /// Push elements to the left of a List
    pub fn lpush(&mut self, key: &[u8], values: Vec<Bytes>) -> crate::traits::StoreResult<usize> {
        use crate::traits::{StoreError, StoreResult};
        use crate::memory::ListData;
        
        // Get or create List
        let list = match self.data.get_mut(key) {
            Some(Data::List(list)) => list,
            Some(_) => return Err(StoreError::WrongType),
            None => {
                let new_list = ListData::new();
                self.data.insert(key.to_vec(), Data::List(new_list));
                match self.data.get_mut(key) {
                    Some(Data::List(list)) => list,
                    _ => return Err(StoreError::Internal("Internal error".to_string())),
                }
            }
        };
        
        let len_before = list.len();
        for value in values {
            list.push_front(value);
        }
        Ok(list.len() - len_before)
    }

    /// Push elements to the right of a List
    pub fn rpush(&mut self, key: &[u8], values: Vec<Bytes>) -> crate::traits::StoreResult<usize> {
        use crate::traits::{StoreError, StoreResult};
        use crate::memory::ListData;
        
        // Get or create List
        let list = match self.data.get_mut(key) {
            Some(Data::List(list)) => list,
            Some(_) => return Err(StoreError::WrongType),
            None => {
                let new_list = ListData::new();
                self.data.insert(key.to_vec(), Data::List(new_list));
                match self.data.get_mut(key) {
                    Some(Data::List(list)) => list,
                    _ => return Err(StoreError::Internal("Internal error".to_string())),
                }
            }
        };
        
        let len_before = list.len();
        for value in values {
            list.push_back(value);
        }
        Ok(list.len() - len_before)
    }

    /// Pop element from the left of a List
    pub fn lpop(&mut self, key: &[u8]) -> crate::traits::StoreResult<Option<Bytes>> {
        use crate::traits::{StoreError, StoreResult};
        
        match self.data.get_mut(key) {
            Some(Data::List(list)) => Ok(list.pop_front()),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(None),
        }
    }

    /// Pop element from the right of a List
    pub fn rpop(&mut self, key: &[u8]) -> crate::traits::StoreResult<Option<Bytes>> {
        use crate::traits::{StoreError, StoreResult};
        
        match self.data.get_mut(key) {
            Some(Data::List(list)) => Ok(list.pop_back()),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(None),
        }
    }

    /// Get a range of elements from a List
    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> crate::traits::StoreResult<Vec<Bytes>> {
        use crate::traits::{StoreError, StoreResult};
        
        match self.data.get(key) {
            Some(Data::List(list)) => {
                let len = list.len() as i64;
                let start_idx = if start < 0 { len + start } else { start }.max(0) as usize;
                let stop_idx = if stop < 0 { len + stop + 1 } else { stop + 1 }.min(len) as usize;
                
                if start_idx >= stop_idx || start_idx >= list.len() {
                    return Ok(Vec::new());
                }
                
                Ok(list.iter().skip(start_idx).take(stop_idx - start_idx).cloned().collect())
            }
            Some(_) => Err(StoreError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    /// Get the length of a List
    pub fn llen(&self, key: &[u8]) -> crate::traits::StoreResult<usize> {
        use crate::traits::{StoreError, StoreResult};
        
        match self.data.get(key) {
            Some(Data::List(list)) => Ok(list.len()),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(0),
        }
    }

    /// Get element at index in a List
    pub fn lindex(&self, key: &[u8], index: i64) -> crate::traits::StoreResult<Option<Bytes>> {
        use crate::traits::{StoreError, StoreResult};
        
        match self.data.get(key) {
            Some(Data::List(list)) => {
                let len = list.len() as i64;
                let idx = if index < 0 { len + index } else { index };
                if idx < 0 || idx >= len {
                    Ok(None)
                } else {
                    Ok(list.get(idx as usize).cloned())
                }
            }
            Some(_) => Err(StoreError::WrongType),
            None => Ok(None),
        }
    }

    /// Set element at index in a List
    pub fn lset(&mut self, key: &[u8], index: i64, value: Bytes) -> crate::traits::StoreResult<()> {
        use crate::traits::{StoreError, StoreResult};
        
        match self.data.get_mut(key) {
            Some(Data::List(list)) => {
                let len = list.len() as i64;
                let idx = if index < 0 { len + index } else { index };
                if idx < 0 || idx >= len {
                    Err(StoreError::Internal("Index out of range".to_string()))
                } else {
                    if let Some(elem) = list.get_mut(idx as usize) {
                        *elem = value;
                        Ok(())
                    } else {
                        Err(StoreError::Internal("Index out of range".to_string()))
                    }
                }
            }
            Some(_) => Err(StoreError::WrongType),
            None => Err(StoreError::Internal("Key not found".to_string())),
        }
    }
}
