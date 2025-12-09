//! List data structure implementation
//!
//! Uses VecDeque for efficient head/tail operations

use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// List data structure (VecDeque for O(1) head/tail operations)
pub type ListData = VecDeque<Vec<u8>>;

/// List storage implementation
#[derive(Clone)]
pub struct ListStore {
    data: Arc<RwLock<HashMap<Vec<u8>, ListData>>>,
}

impl ListStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// LPUSH: Insert elements from left (head)
    pub fn lpush(&self, key: Vec<u8>, values: Vec<Vec<u8>>) -> usize {
        let mut data = self.data.write();
        let list = data.entry(key).or_insert_with(VecDeque::new);
        
        // Insert in reverse order to maintain correct sequence
        for value in values.into_iter().rev() {
            list.push_front(value);
        }
        
        list.len()
    }

    /// RPUSH: Insert elements from right (tail)
    pub fn rpush(&self, key: Vec<u8>, values: Vec<Vec<u8>>) -> usize {
        let mut data = self.data.write();
        let list = data.entry(key).or_insert_with(VecDeque::new);
        
        for value in values {
            list.push_back(value);
        }
        
        list.len()
    }

    /// LPOP: Pop element from left (head)
    pub fn lpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut data = self.data.write();
        if let Some(list) = data.get_mut(key) {
            let value = list.pop_front();
            if list.is_empty() {
                data.remove(key);
            }
            value
        } else {
            None
        }
    }

    /// RPOP: Pop element from right (tail)
    pub fn rpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut data = self.data.write();
        if let Some(list) = data.get_mut(key) {
            let value = list.pop_back();
            if list.is_empty() {
                data.remove(key);
            }
            value
        } else {
            None
        }
    }

    /// LLEN: Get list length
    pub fn llen(&self, key: &[u8]) -> usize {
        let data = self.data.read();
        data.get(key).map(|list| list.len()).unwrap_or(0)
    }

    /// LINDEX: Get element at index
    pub fn lindex(&self, key: &[u8], index: i64) -> Option<Vec<u8>> {
        let data = self.data.read();
        if let Some(list) = data.get(key) {
            let len = list.len() as i64;
            let idx = if index < 0 {
                len + index
            } else {
                index
            };
            
            if idx >= 0 && idx < len {
                list.get(idx as usize).cloned()
            } else {
                None
            }
        } else {
            None
        }
    }

    /// LRANGE: Get elements in range [start, end]
    pub fn lrange(&self, key: &[u8], start: i64, end: i64) -> Vec<Vec<u8>> {
        let data = self.data.read();
        if let Some(list) = data.get(key) {
            let len = list.len() as i64;
            let start_idx = if start < 0 {
                (len + start).max(0)
            } else {
                start.min(len - 1)
            };
            
            let end_idx = if end < 0 {
                (len + end).max(0)
            } else {
                end.min(len - 1)
            };
            
            if start_idx > end_idx {
                return Vec::new();
            }
            
            list.iter()
                .skip(start_idx as usize)
                .take((end_idx - start_idx + 1) as usize)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// LSET: Set element at index
    pub fn lset(&self, key: &[u8], index: i64, value: Vec<u8>) -> Result<(), String> {
        let mut data = self.data.write();
        if let Some(list) = data.get_mut(key) {
            let len = list.len() as i64;
            let idx = if index < 0 {
                len + index
            } else {
                index
            };
            
            if idx < 0 || idx >= len {
                return Err("index out of range".to_string());
            }
            
            list[idx as usize] = value;
            Ok(())
        } else {
            Err("no such key".to_string())
        }
    }

    /// LTRIM: Trim list to range [start, end]
    pub fn ltrim(&self, key: &[u8], start: i64, end: i64) -> Result<(), String> {
        let mut data = self.data.write();
        if let Some(list) = data.get_mut(key) {
            let len = list.len() as i64;
            let start_idx = if start < 0 {
                (len + start).max(0) as usize
            } else {
                start.min(len - 1) as usize
            };
            
            let end_idx = if end < 0 {
                (len + end).max(0) as usize
            } else {
                end.min(len - 1) as usize
            };
            
            if start_idx > end_idx {
                data.remove(key);
                return Ok(());
            }
            
            // Keep only elements in range [start_idx, end_idx]
            let mut new_list = VecDeque::new();
            for (i, item) in list.iter().enumerate() {
                if i >= start_idx && i <= end_idx {
                    new_list.push_back(item.clone());
                }
            }
            *list = new_list;
            
            if list.is_empty() {
                data.remove(key);
            }
            
            Ok(())
        } else {
            Ok(()) // Key doesn't exist, nothing to trim
        }
    }

    /// LREM: Remove elements matching value
    pub fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> usize {
        let mut data = self.data.write();
        if let Some(list) = data.get_mut(key) {
            let mut removed = 0;
            
            if count == 0 {
                // Remove all occurrences
                list.retain(|item| {
                    if item == value {
                        removed += 1;
                        false
                    } else {
                        true
                    }
                });
            } else if count > 0 {
                // Remove from left (head)
                let mut i = 0;
                while i < list.len() && removed < count as usize {
                    if list[i] == value {
                        list.remove(i);
                        removed += 1;
                    } else {
                        i += 1;
                    }
                }
            } else {
                // Remove from right (tail), count is negative
                let count = (-count) as usize;
                let mut i = list.len();
                while i > 0 && removed < count {
                    i -= 1;
                    if list[i] == value {
                        list.remove(i);
                        removed += 1;
                    }
                }
            }
            
            if list.is_empty() {
                data.remove(key);
            }
            
            removed
        } else {
            0
        }
    }

    /// RPOPLPUSH: Pop from right of source, push to left of destination
    pub fn rpoplpush(&self, source: &[u8], dest: &[u8]) -> Option<Vec<u8>> {
        let mut data = self.data.write();
        
        if let Some(value) = data.get_mut(source).and_then(|list| list.pop_back()) {
            if data.get(source).map(|l| l.is_empty()).unwrap_or(false) {
                data.remove(source);
            }
            
            let dest_list = data.entry(dest.to_vec()).or_insert_with(VecDeque::new);
            dest_list.push_front(value.clone());
            
            Some(value)
        } else {
            None
        }
    }
}

impl Default for ListStore {
    fn default() -> Self {
        Self::new()
    }
}


