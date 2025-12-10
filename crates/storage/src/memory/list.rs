//! List Store with Incremental Copy-on-Write (COW)
//!
//! True incremental COW semantics - only changed lists are recorded, NOT full data copy:
//! - Snapshot: Only clones Arc (increases ref count), NO data copy
//! - Write: Records changes in small COW cache (only changed lists), NO full copy
//! - Read: Merges COW cache + base data (O(1) lookup)
//! - Merge: Applies only changed lists to base (O(M) where M = changes, not total data)
//!
//! Note: ListData is directly copied when modified (required for consistency).
//! This is acceptable because individual lists are typically small.
//!
//! Example: 1000 billion lists, modify 3 lists
//! - Old approach (Arc::make_mut): Copies all 1000 billion lists ❌
//! - This approach: Only records 3 changed lists in small HashMap ✅
//!
//! This module provides the core data structure for lists,
//! without implementing Redis API traits.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use crate::memory::shard_data::{DataCow, UnifiedStoreCow};
use crate::traits::{StoreError, StoreResult};

/// List data structure (VecDeque for O(1) head/tail operations)
pub type ListData = VecDeque<Vec<u8>>;

impl UnifiedStoreCow {
    /// LLEN: Get list length for key (read operation, no copy)
    ///
    /// Returns StoreResult<usize> where the usize is the length of the list.
    /// Returns StoreError::WrongType if key exists with different type.
    /// Returns Ok(0) if key doesn't exist (Redis behavior: LLEN returns 0 for non-existent key).
    pub fn llen(&self, key: &[u8]) -> StoreResult<usize> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.updated {
                if let Some(data) = updated.get(key) {
                    match data {
                        DataCow::List(list) => return Ok(list.len()),
                        _ => return Err(StoreError::WrongType),
                    }
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.removed {
                if removed.contains(key) {
                    // Key was removed, return 0 (Redis behavior)
                    return Ok(0);
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        match base.get(key) {
            Some(DataCow::List(list)) => Ok(list.len()),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(0), // Key doesn't exist, return 0 (Redis behavior)
        }
    }

    /// LPUSH: Insert elements from left (incremental COW: only records change)
    ///
    /// Returns StoreResult<usize> where usize is the new length of the list.
    /// Returns StoreError::WrongType if key exists with different type.
    pub fn lpush(&mut self, key: &[u8], values: Vec<Vec<u8>>) -> StoreResult<usize> {
        if values.is_empty() {
            // If no values to push, just return current length
            return self.llen(key);
        }

        if self.is_cow_mode() {
            let updated = self.updated.as_mut().unwrap();
            let removed = self.removed.as_mut().unwrap();
            let key_vec = key.to_vec();

            // Check if already in COW cache
            if let Some(data) = updated.get_mut(&key_vec) {
                match data {
                    DataCow::List(list) => {
                        // Already copied: directly modify (no additional copy!)
                        for value in values.into_iter().rev() {
                            list.push_front(value);
                        }
                        return Ok(list.len());
                    }
                    _ => return Err(StoreError::WrongType),
                }
            }

            // Not in COW cache: copy from base once, then modify
            let mut list = {
                // Check if removed
                if removed.contains(key) {
                    VecDeque::new()
                } else {
                    // Copy from base (first modification)
                    let base = self.base.read();
                    match base.get(key) {
                        Some(DataCow::List(base_list)) => base_list.clone(),
                        Some(_) => return Err(StoreError::WrongType),
                        None => VecDeque::new(),
                    }
                }
            };

            // Push all values in reverse order
            for value in values.into_iter().rev() {
                list.push_front(value);
            }
            updated.insert(key_vec, DataCow::List(list.clone()));
            removed.remove(key);

            Ok(list.len())
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            let entry = base.entry(key.to_vec());
            match entry {
                std::collections::hash_map::Entry::Occupied(mut occupied) => {
                    match occupied.get_mut() {
                        DataCow::List(list) => {
                            for value in values.into_iter().rev() {
                                list.push_front(value);
                            }
                            Ok(list.len())
                        }
                        _ => Err(StoreError::WrongType),
                    }
                }
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    let mut list = VecDeque::new();
                    for value in values.into_iter().rev() {
                        list.push_front(value);
                    }
                    let len = list.len();
                    vacant.insert(DataCow::List(list));
                    Ok(len)
                }
            }
        }
    }

    /// RPUSH: Insert elements from right (incremental COW: only records change)
    ///
    /// Returns StoreResult<usize> where usize is the new length of the list.
    /// Returns StoreError::WrongType if key exists with different type.
    pub fn rpush(&mut self, key: &[u8], values: Vec<Vec<u8>>) -> StoreResult<usize> {
        if values.is_empty() {
            // If no values to push, just return current length
            return self.llen(key);
        }

        if self.is_cow_mode() {
            let updated = self.updated.as_mut().unwrap();
            let removed = self.removed.as_mut().unwrap();
            let key_vec = key.to_vec();

            // Check if already in COW cache
            if let Some(data) = updated.get_mut(&key_vec) {
                match data {
                    DataCow::List(list) => {
                        // Already copied: directly modify (no additional copy!)
                        for value in values {
                            list.push_back(value);
                        }
                        return Ok(list.len());
                    }
                    _ => return Err(StoreError::WrongType),
                }
            }

            // Not in COW cache: copy from base once, then modify
            let mut list = {
                // Check if removed
                if removed.contains(key) {
                    VecDeque::new()
                } else {
                    // Copy from base (first modification)
                    let base = self.base.read();
                    match base.get(key) {
                        Some(DataCow::List(base_list)) => base_list.clone(),
                        Some(_) => return Err(StoreError::WrongType),
                        None => VecDeque::new(),
                    }
                }
            };

            // Push all values
            for value in values {
                list.push_back(value);
            }
            updated.insert(key_vec, DataCow::List(list.clone()));
            removed.remove(key);

            Ok(list.len())
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            let entry = base.entry(key.to_vec());
            match entry {
                std::collections::hash_map::Entry::Occupied(mut occupied) => {
                    match occupied.get_mut() {
                        DataCow::List(list) => {
                            for value in values {
                                list.push_back(value);
                            }
                            Ok(list.len())
                        }
                        _ => Err(StoreError::WrongType),
                    }
                }
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    let mut list = VecDeque::new();
                    for value in values {
                        list.push_back(value);
                    }
                    let len = list.len();
                    vacant.insert(DataCow::List(list));
                    Ok(len)
                }
            }
        }
    }

    /// LPOP: Pop element from front of list (incremental COW: only records change)
    ///
    /// Returns StoreResult<Option<Vec<u8>>> where Some(value) is the popped element.
    /// Returns StoreError::WrongType if key exists with different type.
    pub fn lpop(&mut self, key: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        if self.is_cow_mode() {
            let updated = self.updated.as_mut().unwrap();
            let removed = self.removed.as_mut().unwrap();

            // Check if already in COW cache
            if let Some(data) = updated.get_mut(key) {
                match data {
                    DataCow::List(list) => {
                        let result = list.pop_front();
                        if list.is_empty() {
                            removed.insert(key.to_vec());
                            updated.remove(key);
                        }
                        return Ok(result);
                    }
                    _ => return Err(StoreError::WrongType),
                }
            }

            // Not in COW cache: copy from base once, then modify
            if removed.contains(key) {
                return Ok(None);
            }

            let mut list = {
                let base = self.base.read();
                match base.get(key) {
                    Some(DataCow::List(base_list)) => base_list.clone(),
                    Some(_) => return Err(StoreError::WrongType),
                    None => return Ok(None),
                }
            };

            let result = list.pop_front();
            if list.is_empty() {
                removed.insert(key.to_vec());
            } else {
                updated.insert(key.to_vec(), DataCow::List(list));
            }
            Ok(result)
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            match base.get_mut(key) {
                Some(DataCow::List(list)) => Ok(list.pop_front()),
                Some(_) => Err(StoreError::WrongType),
                None => Ok(None),
            }
        }
    }

    /// RPOP: Pop element from back of list (incremental COW: only records change)
    ///
    /// Returns StoreResult<Option<Vec<u8>>> where Some(value) is the popped element.
    /// Returns StoreError::WrongType if key exists with different type.
    pub fn rpop(&mut self, key: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        if self.is_cow_mode() {
            let updated = self.updated.as_mut().unwrap();
            let removed = self.removed.as_mut().unwrap();

            // Check if already in COW cache
            if let Some(data) = updated.get_mut(key) {
                match data {
                    DataCow::List(list) => {
                        let result = list.pop_back();
                        if list.is_empty() {
                            removed.insert(key.to_vec());
                            updated.remove(key);
                        }
                        return Ok(result);
                    }
                    _ => return Err(StoreError::WrongType),
                }
            }

            // Not in COW cache: copy from base once, then modify
            if removed.contains(key) {
                return Ok(None);
            }

            let mut list = {
                let base = self.base.read();
                match base.get(key) {
                    Some(DataCow::List(base_list)) => base_list.clone(),
                    Some(_) => return Err(StoreError::WrongType),
                    None => return Ok(None),
                }
            };

            let result = list.pop_back();
            if list.is_empty() {
                removed.insert(key.to_vec());
            } else {
                updated.insert(key.to_vec(), DataCow::List(list));
            }
            Ok(result)
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            match base.get_mut(key) {
                Some(DataCow::List(list)) => Ok(list.pop_back()),
                Some(_) => Err(StoreError::WrongType),
                None => Ok(None),
            }
        }
    }

    /// LINDEX: Get element at specified index (read operation, no copy)
    ///
    /// Returns StoreResult<Option<Vec<u8>>> where Some(value) is the element at index.
    /// Returns StoreError::WrongType if key exists with different type.
    /// Index can be negative (counts from end).
    pub fn lindex(&self, key: &[u8], index: i64) -> StoreResult<Option<Vec<u8>>> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.updated {
                if let Some(data) = updated.get(key) {
                    match data {
                        DataCow::List(list) => {
                            let len = list.len() as i64;
                            let actual_index = if index < 0 { len + index } else { index };
                            if actual_index >= 0 && actual_index < len {
                                return Ok(list.get(actual_index as usize).cloned());
                            }
                            return Ok(None);
                        }
                        _ => return Err(StoreError::WrongType),
                    }
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.removed {
                if removed.contains(key) {
                    return Ok(None);
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        match base.get(key) {
            Some(DataCow::List(list)) => {
                let len = list.len() as i64;
                let actual_index = if index < 0 { len + index } else { index };
                if actual_index >= 0 && actual_index < len {
                    Ok(list.get(actual_index as usize).cloned())
                } else {
                    Ok(None)
                }
            }
            Some(_) => Err(StoreError::WrongType),
            None => Ok(None),
        }
    }

    /// LSET: Set element at specified index (incremental COW: only records change)
    ///
    /// Returns StoreResult<()> on success.
    /// Returns StoreError::WrongType if key exists with different type.
    /// Returns StoreError::IndexOutOfRange if index is out of range.
    /// Returns StoreError::KeyNotFound if key doesn't exist.
    /// Index can be negative (counts from end).
    pub fn lset(&mut self, key: &[u8], index: i64, value: Vec<u8>) -> StoreResult<()> {
        if self.is_cow_mode() {
            let updated = self.updated.as_mut().unwrap();
            let removed = self.removed.as_mut().unwrap();
            let key_vec = key.to_vec();

            // Check if already in COW cache
            if let Some(data) = updated.get_mut(&key_vec) {
                match data {
                    DataCow::List(list) => {
                        let len = list.len() as i64;
                        let actual_index = if index < 0 { len + index } else { index };
                        if actual_index >= 0 && actual_index < len {
                            list[actual_index as usize] = value;
                            return Ok(());
                        }
                        return Err(StoreError::IndexOutOfRange);
                    }
                    _ => return Err(StoreError::WrongType),
                }
            }

            // Not in COW cache: copy from base once, then modify
            if removed.contains(key) {
                return Err(StoreError::KeyNotFound);
            }

            let mut list = {
                let base = self.base.read();
                match base.get(key) {
                    Some(DataCow::List(base_list)) => base_list.clone(),
                    Some(_) => return Err(StoreError::WrongType),
                    None => return Err(StoreError::KeyNotFound),
                }
            };

            let len = list.len() as i64;
            let actual_index = if index < 0 { len + index } else { index };
            if actual_index >= 0 && actual_index < len {
                list[actual_index as usize] = value;
                updated.insert(key_vec, DataCow::List(list));
                removed.remove(key);
                Ok(())
            } else {
                Err(StoreError::IndexOutOfRange)
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            match base.get_mut(key) {
                Some(DataCow::List(list)) => {
                    let len = list.len() as i64;
                    let actual_index = if index < 0 { len + index } else { index };
                    if actual_index >= 0 && actual_index < len {
                        list[actual_index as usize] = value;
                        Ok(())
                    } else {
                        Err(StoreError::IndexOutOfRange)
                    }
                }
                Some(_) => Err(StoreError::WrongType),
                None => Err(StoreError::KeyNotFound),
            }
        }
    }

    /// LRANGE: Get range of elements [start, end] (read operation, no copy)
    ///
    /// Returns StoreResult<Vec<Vec<u8>>> containing elements in the range.
    /// Returns StoreError::WrongType if key exists with different type.
    /// Start and end can be negative (counts from end).
    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> StoreResult<Vec<Vec<u8>>> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.updated {
                if let Some(data) = updated.get(key) {
                    match data {
                        DataCow::List(list) => {
                            let len = list.len() as i64;
                            let start_idx = if start < 0 {
                                (len + start).max(0) as usize
                            } else {
                                start.min(len - 1) as usize
                            };
                            let end_idx = if stop < 0 {
                                (len + stop).max(0) as usize
                            } else {
                                stop.min(len - 1) as usize
                            };
                            if start_idx > end_idx {
                                return Ok(Vec::new());
                            }
                            return Ok(list
                                .iter()
                                .skip(start_idx)
                                .take(end_idx - start_idx + 1)
                                .cloned()
                                .collect());
                        }
                        _ => return Err(StoreError::WrongType),
                    }
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.removed {
                if removed.contains(key) {
                    return Ok(Vec::new());
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        match base.get(key) {
            Some(DataCow::List(list)) => {
                let len = list.len() as i64;
                let start_idx = if start < 0 {
                    (len + start).max(0) as usize
                } else {
                    start.min(len - 1) as usize
                };
                let end_idx = if stop < 0 {
                    (len + stop).max(0) as usize
                } else {
                    stop.min(len - 1) as usize
                };
                if start_idx > end_idx {
                    Ok(Vec::new())
                } else {
                    Ok(list
                        .iter()
                        .skip(start_idx)
                        .take(end_idx - start_idx + 1)
                        .cloned()
                        .collect())
                }
            }
            Some(_) => Err(StoreError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    /// Clear all lists (incremental COW: only records change)
    pub fn clear_all(&mut self) {
        if self.is_cow_mode() {
            // COW mode: mark all existing keys as removed
            let base = self.base.read();
            let keys: Vec<Vec<u8>> = base.keys().cloned().collect();
            drop(base);
            for key in keys {
                self.removed.as_mut().unwrap().insert(key);
            }
            self.updated.as_mut().unwrap().clear();
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_store_basic_operations() {
        let mut store = UnifiedStoreCow::new();

        // Push elements
        store.rpush(b"list1", vec![b"a".to_vec()]);
        store.rpush(b"list1", vec![b"b".to_vec()]);
        store.lpush(b"list1", vec![b"c".to_vec()]);

        assert_eq!(store.llen(b"list1").unwrap(), 3);
        assert_eq!(store.lindex(b"list1", 0).unwrap(), Some(b"c".to_vec()));
        assert_eq!(store.lindex(b"list1", 1).unwrap(), Some(b"a".to_vec()));
        assert_eq!(store.lindex(b"list1", 2).unwrap(), Some(b"b".to_vec()));

        // Pop elements
        assert_eq!(store.lpop(b"list1").unwrap(), Some(b"c".to_vec()));
        assert_eq!(store.rpop(b"list1").unwrap(), Some(b"b".to_vec()));
        assert_eq!(store.llen(b"list1").unwrap(), 1);

        // Set element
        store.lset(b"list1", 0, b"x".to_vec()).unwrap();
        assert_eq!(store.lindex(b"list1", 0).unwrap(), Some(b"x".to_vec()));
    }

    #[test]
    fn test_list_store_snapshot_no_copy() {
        let mut store = UnifiedStoreCow::new();
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"c".to_vec()]).unwrap();

        // Before snapshot: ref count should be 1
        assert_eq!(store.ref_count(), 1);

        // Create snapshot (only increases ref count, no copy)
        let snapshot = store.make_snapshot();

        // After snapshot: ref count should be 2
        assert_eq!(store.ref_count(), 2);
        assert_eq!(Arc::strong_count(&snapshot), 2);

        // Snapshot should have same data
        let snapshot_data = snapshot.read();
        assert_eq!(snapshot_data.len(), 1);
        if let Some(DataCow::List(list)) = snapshot_data.get(b"list1" as &[u8]) {
            assert_eq!(list.len(), 3);
        }
        drop(snapshot_data);

        // Original should still work
        assert_eq!(store.llen(b"list1").unwrap(), 3);
        assert_eq!(store.lindex(b"list1", 0).unwrap(), Some(b"a".to_vec()));
    }

    #[test]
    fn test_list_store_write_after_snapshot_no_copy() {
        let mut store = UnifiedStoreCow::new();
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();

        // Create snapshot
        let snapshot = store.make_snapshot();
        assert_eq!(store.ref_count(), 2);
        assert!(store.is_in_cow_mode());

        // Write operation records change in COW cache (NO data copy)
        store.rpush(b"list1", vec![b"c".to_vec()]).unwrap();
        assert_eq!(store.ref_count(), 2); // Ref count unchanged (no copy!)
        assert_eq!(Arc::strong_count(&snapshot), 2);
        assert!(store.is_in_cow_mode());

        // Original should have new data (via COW cache)
        assert_eq!(store.llen(b"list1").unwrap(), 3);
        assert_eq!(store.lindex(b"list1", 2).unwrap(), Some(b"c".to_vec()));

        // Snapshot should have old data (unchanged, from base)
        let snapshot_data = snapshot.read();
        if let Some(DataCow::List(list)) = snapshot_data.get(b"list1" as &[u8]) {
            assert_eq!(list.len(), 2);
            assert_eq!(list.get(2), None);
        }
        drop(snapshot_data);
    }

    #[test]
    fn test_list_store_write_without_snapshot_no_copy() {
        let mut store = UnifiedStoreCow::new();
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();

        // No snapshot, ref count is 1
        assert_eq!(store.ref_count(), 1);

        // Write operation should NOT copy (ref count stays 1)
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();
        assert_eq!(store.ref_count(), 1); // No copy happened

        // Another write
        store.rpush(b"list1", vec![b"c".to_vec()]).unwrap();
        assert_eq!(store.ref_count(), 1); // Still no copy
    }

    #[test]
    fn test_list_store_merge_applies_only_changes() {
        let mut store = UnifiedStoreCow::new();
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"c".to_vec()]).unwrap();

        // Create snapshot
        let snapshot = store.make_snapshot();
        assert_eq!(store.ref_count(), 2);

        // Make changes (only 3 operations, not full copy)
        store.rpush(b"list1", vec![b"d".to_vec()]).unwrap(); // Add new
        store.lset(b"list1", 0, b"x".to_vec()).unwrap(); // Update existing
        store.lpop(b"list1").unwrap(); // Remove first

        // Before merge: changes are in COW cache
        assert!(store.is_in_cow_mode());
        assert_eq!(store.llen(b"list1").unwrap(), 3); // x, b, c, d -> after pop_front: b, c, d
        assert_eq!(store.lindex(b"list1", 0).unwrap(), Some(b"b".to_vec()));
        assert_eq!(store.lindex(b"list1", 1).unwrap(), Some(b"c".to_vec()));
        assert_eq!(store.lindex(b"list1", 2).unwrap(), Some(b"d".to_vec()));

        // Snapshot still has old data
        let snapshot_data = snapshot.read();
        if let Some(DataCow::List(list)) = snapshot_data.get(b"list1" as &[u8]) {
            assert_eq!(list.len(), 3);
            assert_eq!(list.get(0), Some(&b"a".to_vec()));
        }
        drop(snapshot_data);

        // Merge: applies only changes to base (O(M) where M=3, not O(N))
        store.merge_cow();
        assert!(!store.is_in_cow_mode());
        // Ref count is still 2 because snapshot still exists (this is correct)
        assert_eq!(store.ref_count(), 2);

        // After merge: changes are in base
        assert_eq!(store.llen(b"list1").unwrap(), 3);
        assert_eq!(store.lindex(b"list1", 0).unwrap(), Some(b"b".to_vec()));
        assert_eq!(store.lindex(b"list1", 1).unwrap(), Some(b"c".to_vec()));
        assert_eq!(store.lindex(b"list1", 2).unwrap(), Some(b"d".to_vec()));
    }

    #[test]
    fn test_list_store_range() {
        let mut store = UnifiedStoreCow::new();
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"c".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"d".to_vec()]).unwrap();

        // Get range
        let range = store.lrange(b"list1", 1, 2).unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], b"b".to_vec());
        assert_eq!(range[1], b"c".to_vec());
    }

    #[test]
    fn test_list_store_multiple_keys() {
        let mut store = UnifiedStoreCow::new();
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store.rpush(b"list2", vec![b"x".to_vec()]).unwrap();
        store.rpush(b"list2", vec![b"y".to_vec()]).unwrap();

        assert_eq!(store.key_count(), 2);
        assert_eq!(store.llen(b"list1").unwrap(), 1);
        assert_eq!(store.llen(b"list2").unwrap(), 2);

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Modify only list1
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();

        // list1 should be updated
        assert_eq!(store.llen(b"list1").unwrap(), 2);
        // list2 should be unchanged (from base)
        assert_eq!(store.len(b"list2"), Some(2));
    }

    #[test]
    fn test_list_store_updated_overrides_removed() {
        let mut store = UnifiedStoreCow::new();
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Remove list1 (adds to removed cache)
        assert!(store.clear(b"list1"));
        assert!(!store.contains_key(b"list1"));
        assert_eq!(store.llen(b"list1").unwrap(), 0);

        // Re-add list1 (adds to updated cache, should override removed)
        store.rpush(b"list1", vec![b"c".to_vec()]).unwrap();
        assert!(store.contains_key(b"list1")); // Should exist (updated overrides removed)
        assert_eq!(store.llen(b"list1").unwrap(), 1);
        assert_eq!(store.lindex(b"list1", 0).unwrap(), Some(b"c".to_vec()));

        // Merge: updated should take precedence
        store.merge_cow();
        assert!(store.contains_key(b"list1")); // Should still exist after merge
        assert_eq!(store.llen(b"list1").unwrap(), 1);
        assert_eq!(store.lindex(b"list1", 0).unwrap(), Some(b"c".to_vec()));
    }

    #[test]
    fn test_list_store_merge_updated_overrides_removed() {
        let mut store = UnifiedStoreCow::new();
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store.rpush(b"list2", vec![b"x".to_vec()]).unwrap();

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Remove list1
        assert!(store.del(b"list1"));
        assert!(!store.contains_key(b"list1"));

        // Re-add list1 (should override removed)
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();
        assert!(store.contains_key(b"list1"));

        // Merge: updated should take precedence over removed
        store.merge_cow();
        assert!(store.contains_key(b"list1")); // Should exist (updated overrides removed)
        assert_eq!(store.len(b"list1"), Some(1));
        assert_eq!(store.lindex(b"list1", 0).unwrap(), Some(b"b".to_vec()));

        // list2 should still exist
        assert!(store.contains_key(b"list2"));
        assert_eq!(store.len(b"list2"), Some(1));
    }

    // ========== Additional COW Tests (similar to set.rs patterns) ==========

    #[test]
    fn test_list_data_cow_clear_in_cow_mode() {
        let mut store = UnifiedStoreCow::new();

        // Add initial data
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();
        store.rpush(b"list2", vec![b"x".to_vec()]).unwrap();

        // Create snapshot
        store.make_snapshot();
        assert!(store.is_cow_mode());

        // Clear lists in COW mode
        assert!(store.clear(b"list1"));
        assert!(store.clear(b"list2"));

        // Verify cleared state
        assert!(!store.contains_key(b"list1"));
        assert!(!store.contains_key(b"list2"));
        assert_eq!(store.llen(b"list1").unwrap(), 0);
        assert_eq!(store.len(b"list2"), None);

        // Verify keys() returns empty
        let keys = store.keys();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_list_data_cow_multiple_snapshots() {
        let mut store1 = UnifiedStoreCow::new();

        // Add data to store1
        store1.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store1.rpush(b"list1", vec![b"b".to_vec()]).unwrap();
        store1.rpush(b"list2", vec![b"x".to_vec()]).unwrap();

        // Create snapshot and get reference
        let _snapshot = store1.make_snapshot();
        assert_eq!(store1.ref_count(), 2);

        // Create store2 from same base (simulating multiple snapshots)
        let mut store2 = UnifiedStoreCow {
            base: Arc::clone(&store1.base),
            updated: Some(HashMap::new()),
            removed: Some(HashSet::new()),
        };

        // Both should share the same base
        assert_eq!(store1.ref_count(), 3); // store1.base + snapshot + store2
        assert!(store2.is_cow_mode());

        // Make independent changes
        store1.rpush(b"list1", vec![b"c".to_vec()]).unwrap(); // Add to list1
        store2.rpush(b"list2", vec![b"y".to_vec()]).unwrap(); // Add to list2
        store2.del(b"list1"); // Remove list1 in store2

        // Verify independent states
        assert_eq!(store1.llen(b"list1").unwrap(), 3); // a, b, c
        assert_eq!(store1.llen(b"list2").unwrap(), 1); // x
        assert!(store1.contains_key(b"list1"));

        assert_eq!(store2.llen(b"list1").unwrap(), 0); // cleared
        assert_eq!(store2.llen(b"list2").unwrap(), 2); // x, y
        assert!(!store2.contains_key(b"list1"));
    }

    #[test]
    fn test_list_data_cow_edge_cases() {
        let mut store = UnifiedStoreCow::new();

        // Test empty data
        store.rpush(&[], vec![vec![]]).unwrap();
        assert!(store.contains_key(&[]));
        assert_eq!(store.llen(&[]).unwrap(), 1);
        assert_eq!(store.lindex(&[], 0).unwrap(), Some(vec![]));

        // Test large data
        let large_data = vec![b'x'; 1024]; // 1KB element
        store.rpush(b"large", vec![large_data.clone()]).unwrap();
        assert!(store.contains_key(b"large"));
        assert_eq!(store.llen(b"large").unwrap(), 1);
        assert_eq!(store.lindex(b"large", 0).unwrap(), Some(large_data));

        // Test binary data
        let binary_data = vec![0, 1, 2, 255, 254, 253];
        store.rpush(b"binary", vec![binary_data.clone()]).unwrap();
        assert!(store.contains_key(b"binary"));
        assert_eq!(store.lindex(b"binary", 0).unwrap(), Some(binary_data));

        // Test with snapshot
        store.make_snapshot();
        store.rpush(b"binary", vec![vec![128, 129, 130]]).unwrap();
        assert_eq!(store.llen(b"binary").unwrap(), 2);
    }

    #[test]
    fn test_list_data_cow_concurrent_read_write() {
        use std::sync::Arc;
        use std::thread;

        let mut store = UnifiedStoreCow::new();

        // Add initial data
        for i in 0..10 {
            store.rpush(b"shared", vec![format!("item{}", i).into_bytes()]).unwrap();
        }

        // Create snapshot
        store.make_snapshot();
        let store_arc = Arc::new(parking_lot::RwLock::new(store));

        // Spawn reader threads
        let mut handles = vec![];

        for _ in 0..3 {
            let store_clone = Arc::clone(&store_arc);
            let handle = thread::spawn(move || {
                for i in 0..10 {
                    let store = store_clone.read();
                    let _len = store.llen(b"shared");
                    let _item = store.lindex(b"shared", (i % 10) as i64);
                    let _contains = store.contains_key(b"shared");
                }
            });
            handles.push(handle);
        }

        // Spawn writer thread
        let store_clone = Arc::clone(&store_arc);
        let writer_handle = thread::spawn(move || {
            let mut store = store_clone.write();
            for i in 10..15 {
                store.rpush(b"shared", vec![format!("item{}", i).into_bytes()]).unwrap();
            }
        });

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
        writer_handle.join().unwrap();

        // Verify final state
        let store = store_arc.read();
        assert_eq!(store.llen(b"shared").unwrap(), 15);
    }

    #[test]
    fn test_list_data_cow_multiple_merges() {
        let mut store = UnifiedStoreCow::new();

        // First cycle: add data and merge
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store.make_snapshot();
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();
        store.merge_cow();

        // Second cycle: modify and merge again
        store.make_snapshot();
        store.rpush(b"list1", vec![b"c".to_vec()]).unwrap();
        store.lpop(b"list1").unwrap();
        store.merge_cow();

        // Verify final state
        assert_eq!(store.llen(b"list1").unwrap(), 2); // b, c
        assert_eq!(store.lindex(b"list1", 0).unwrap(), Some(b"b".to_vec()));
        assert_eq!(store.lindex(b"list1", 1).unwrap(), Some(b"c".to_vec()));
    }

    #[test]
    fn test_list_store_cow_from_data() {
        let mut data = HashMap::new();
        let mut list1 = VecDeque::new();
        list1.push_back(b"a".to_vec());
        list1.push_back(b"b".to_vec());
        data.insert(b"list1".to_vec(), list1);

        let mut store = UnifiedStoreCow::new();
        // Insert data manually
        for (key, list) in data {
            store.insert(key, DataCow::List(list));
        }

        // Verify initial data
        assert_eq!(store.llen(b"list1").unwrap(), 2);
        assert_eq!(store.lindex(b"list1", 0).unwrap(), Some(b"a".to_vec()));
        assert_eq!(store.lindex(b"list1", 1).unwrap(), Some(b"b".to_vec()));

        // Create snapshot and modify
        store.make_snapshot();
        store.rpush(b"list1", vec![b"c".to_vec()]).unwrap();

        // Verify changes
        assert_eq!(store.llen(b"list1").unwrap(), 3);
        assert_eq!(store.lindex(b"list1", 2).unwrap(), Some(b"c".to_vec()));
    }

    #[test]
    fn test_list_store_cow_clear_operations() {
        let mut store = UnifiedStoreCow::new();

        // Add some data
        store.rpush(b"list1", vec![b"a".to_vec()]).unwrap();
        store.rpush(b"list1", vec![b"b".to_vec()]).unwrap();
        store.rpush(b"list2", vec![b"x".to_vec()]).unwrap();

        // Create snapshot
        store.make_snapshot();

        // Test clear operations
        assert!(store.del(b"list1"));
        assert!(!store.del(b"nonexistent"));

        // Verify state
        assert!(!store.contains_key(b"list1"));
        assert!(store.contains_key(b"list2"));
        assert_eq!(store.llen(b"list1").unwrap(), 0);
        assert_eq!(store.len(b"list2"), Some(1));

        // Test merge
        store.merge_cow();
        assert!(!store.contains_key(b"list1"));
        assert!(store.contains_key(b"list2"));
    }
}
