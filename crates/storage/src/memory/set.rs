//! Set data structure implementation
//!
//! Uses HashSet for O(1) member operations

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Set data structure (HashSet for O(1) operations)
pub type SetData = HashSet<Vec<u8>>;

/// Set storage implementation
#[derive(Clone)]
pub struct SetStore {
    data: Arc<RwLock<HashMap<Vec<u8>, SetData>>>,
}

impl SetStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// SADD: Add members to set
    pub fn sadd(&self, key: Vec<u8>, members: Vec<Vec<u8>>) -> usize {
        let mut data = self.data.write();
        let set = data.entry(key).or_insert_with(HashSet::new);
        
        let mut added = 0;
        for member in members {
            if set.insert(member) {
                added += 1;
            }
        }
        
        added
    }

    /// SREM: Remove members from set
    pub fn srem(&self, key: &[u8], members: &[Vec<u8>]) -> usize {
        let mut data = self.data.write();
        if let Some(set) = data.get_mut(key) {
            let mut removed = 0;
            for member in members {
                if set.remove(member) {
                    removed += 1;
                }
            }
            
            if set.is_empty() {
                data.remove(key);
            }
            
            removed
        } else {
            0
        }
    }

    /// SISMEMBER: Check if member exists in set
    pub fn sismember(&self, key: &[u8], member: &[u8]) -> bool {
        let data = self.data.read();
        data.get(key)
            .map(|set| set.contains(member))
            .unwrap_or(false)
    }

    /// SMEMBERS: Get all members
    pub fn smembers(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let data = self.data.read();
        data.get(key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// SCARD: Get set cardinality (size)
    pub fn scard(&self, key: &[u8]) -> usize {
        let data = self.data.read();
        data.get(key).map(|set| set.len()).unwrap_or(0)
    }

    /// SPOP: Pop random member(s)
    pub fn spop(&self, key: &[u8], count: usize) -> Vec<Vec<u8>> {
        let mut data = self.data.write();
        if let Some(set) = data.get_mut(key) {
            let mut popped = Vec::new();
            let mut to_remove = Vec::new();
            
            // Collect random members
            for member in set.iter().take(count) {
                to_remove.push(member.clone());
                popped.push(member.clone());
            }
            
            // Remove them
            for member in to_remove {
                set.remove(&member);
            }
            
            if set.is_empty() {
                data.remove(key);
            }
            
            popped
        } else {
            Vec::new()
        }
    }

    /// SRANDMEMBER: Get random member(s) without removing
    pub fn srandmember(&self, key: &[u8], count: i64) -> Vec<Vec<u8>> {
        let data = self.data.read();
        if let Some(set) = data.get(key) {
            let count = if count < 0 {
                (-count) as usize
            } else {
                count as usize
            };
            
            let members: Vec<_> = set.iter().take(count).cloned().collect();
            members
        } else {
            Vec::new()
        }
    }

    /// SMOVE: Move member from source to destination
    pub fn smove(&self, source: &[u8], dest: &[u8], member: Vec<u8>) -> bool {
        let mut data = self.data.write();
        
        let source_key = source.to_vec();
        let dest_key = dest.to_vec();
        
        let should_remove_source = {
            if let Some(source_set) = data.get_mut(&source_key) {
                if source_set.remove(&member) {
                    let is_empty = source_set.is_empty();
                    if is_empty {
                        drop(source_set);
                        data.remove(&source_key);
                    }
                    
                    let dest_set = data.entry(dest_key).or_insert_with(HashSet::new);
                    dest_set.insert(member);
                    return true;
                }
                false
            } else {
                false
            }
        };
        
        should_remove_source
    }

    /// SINTER: Intersection of multiple sets
    pub fn sinter(&self, keys: &[&[u8]]) -> Vec<Vec<u8>> {
        if keys.is_empty() {
            return Vec::new();
        }
        
        let data = self.data.read();
        
        // Get first set
        let first_set = match data.get(keys[0].to_vec().as_slice()) {
            Some(set) => set,
            None => return Vec::new(),
        };
        
        // Find intersection with other sets
        let mut result: HashSet<Vec<u8>> = first_set.iter().cloned().collect();
        
        for key in keys.iter().skip(1) {
            if let Some(set) = data.get(&key.to_vec()) {
                result.retain(|member| set.contains(member));
            } else {
                // If any set doesn't exist, intersection is empty
                return Vec::new();
            }
        }
        
        result.into_iter().collect()
    }

    /// SUNION: Union of multiple sets
    pub fn sunion(&self, keys: &[&[u8]]) -> Vec<Vec<u8>> {
        let data = self.data.read();
        let mut result = HashSet::new();
        
        for key in keys {
            if let Some(set) = data.get(&key.to_vec()) {
                for member in set {
                    result.insert(member.clone());
                }
            }
        }
        
        result.into_iter().collect()
    }

    /// SDIFF: Difference of sets (first - others)
    pub fn sdiff(&self, keys: &[&[u8]]) -> Vec<Vec<u8>> {
        if keys.is_empty() {
            return Vec::new();
        }
        
        let data = self.data.read();
        
        // Get first set
        let first_set = match data.get(keys[0]) {
            Some(set) => set.clone(),
            None => return Vec::new(),
        };
        
        // Remove members that exist in other sets
        let mut result: HashSet<Vec<u8>> = first_set;
        
        for key in keys.iter().skip(1) {
            if let Some(set) = data.get(&key.to_vec()) {
                for member in set {
                    result.remove(member);
                }
            }
        }
        
        result.into_iter().collect()
    }
}

impl Default for SetStore {
    fn default() -> Self {
        Self::new()
    }
}

