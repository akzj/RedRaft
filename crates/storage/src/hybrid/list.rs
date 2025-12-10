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

use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

/// List data structure (VecDeque for O(1) head/tail operations)
pub type ListData = VecDeque<Vec<u8>>;

/// List Store with Incremental Copy-on-Write (COW) support
///
/// True incremental COW semantics:
/// - `make_snapshot()`: Only clones Arc (increases ref count), NO data copy
/// - `push_*()`/`pop_*()`/`set()`: Records changes in small COW cache (only changed lists), NO full copy
/// - Read operations: Merges COW cache + base data (O(1) lookup)
/// - `merge_cow()`: Applies only changed lists to base via RwLock (O(M) where M = changes, not total data)
///
/// Note: ListData is copied when modified (required for consistency), but only for changed lists.
/// This avoids full HashMap copy even for 1000 billion lists when only 3 lists change.
#[derive(Debug, Clone)]
pub struct ListStoreCow {
    /// Base data (shared via Arc<RwLock<>>, can be directly modified without clone)
    base: Arc<RwLock<HashMap<Vec<u8>, ListData>>>,

    /// COW cache: Updated/added lists (only changed lists)
    /// ListData is copied here (required for consistency)
    lists_updated: Option<HashMap<Vec<u8>, ListData>>,

    /// COW cache: Removed list keys
    lists_removed: Option<HashSet<Vec<u8>>>,
}

impl ListStoreCow {
    /// Create a new empty ListStore with COW support
    pub fn new() -> Self {
        Self {
            base: Arc::new(RwLock::new(HashMap::new())),
            lists_updated: None,
            lists_removed: None,
        }
    }

    /// Create from existing HashMap
    pub fn from_data(data: HashMap<Vec<u8>, ListData>) -> Self {
        Self {
            base: Arc::new(RwLock::new(data)),
            lists_updated: None,
            lists_removed: None,
        }
    }

    /// Check if in COW mode (has snapshot)
    fn is_cow_mode(&self) -> bool {
        self.lists_updated.is_some()
    }

    /// Create a snapshot (only increases reference count, NO data copy)
    ///
    /// Returns a cloned Arc that shares the same base data.
    /// Write operations will record changes in COW cache instead of copying data.
    pub fn make_snapshot(&mut self) -> Arc<RwLock<HashMap<Vec<u8>, ListData>>> {
        if self.is_cow_mode() {
            // Already in COW mode, just return existing base
            return Arc::clone(&self.base);
        }

        // Enter COW mode: initialize caches
        self.lists_updated = Some(HashMap::new());
        self.lists_removed = Some(HashSet::new());

        // ✅ Only clone Arc (O(1)), NO data copy
        Arc::clone(&self.base)
    }

    /// Merge COW changes back to base (applies only changed lists)
    ///
    /// This is called when snapshot is no longer needed.
    /// Only changed lists are applied via RwLock, not full data copy.
    pub fn merge_cow(&mut self) {
        if !self.is_cow_mode() {
            return;
        }

        let updated = self.lists_updated.take();
        let removed = self.lists_removed.take();

        // ✅ Get write lock and directly modify base (NO clone!)
        let mut base = self.base.write();

        // Apply removals (only if not in updated - updated takes precedence)
        if let Some(ref removed) = removed {
            for key in removed {
                // Only remove if not being updated
                if updated.as_ref().map_or(true, |u| !u.contains_key(key)) {
                    base.remove(key);
                }
            }
        }

        // Apply updates/additions (this handles both new and updated lists)
        if let Some(ref updated) = updated {
            for (key, list_data) in updated {
                // ListData is copied here (required for consistency)
                base.insert(key.clone(), list_data.clone());
            }
        }

        // Write lock is released here, no need to replace base
    }

    /// Get list for key (returns a copy - only use when modification is needed)
    ///
    /// ⚠️ WARNING: This method copies the entire list!
    /// For read-only operations, use optimized methods instead:
    /// - `len(key)` - get length without copy
    /// - `get(key, index)` - get element without copy
    /// - `range(key, start, end)` - get range without copy
    ///
    /// This method should only be used when you need to modify the list
    /// (e.g., in push/pop/set operations).
    pub fn get_list(&self, key: &[u8]) -> Option<ListData> {
        if self.is_cow_mode() {
            // Check if removed
            if let Some(ref removed) = self.lists_removed {
                if removed.contains(key) {
                    return None;
                }
            }

            // Check COW cache first (if exists)
            if let Some(ref updated) = self.lists_updated {
                if let Some(list) = updated.get(key) {
                    return Some(list.clone());
                }
            }
            // If not in updated, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).cloned()
    }

    /// Check if key exists (read operation, merges COW cache + base)
    pub fn contains_key(&self, key: &[u8]) -> bool {
        if self.is_cow_mode() {
            // Check if removed
            if let Some(ref removed) = self.lists_removed {
                if removed.contains(key) {
                    return false;
                }
            }

            // Check COW cache first (if exists)
            if let Some(ref updated) = self.lists_updated {
                if updated.contains_key(key) {
                    return true;
                }
            }
            // If not in updated, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.contains_key(key)
    }

    /// Get list length for key (read operation, no copy)
    pub fn len(&self, key: &[u8]) -> Option<usize> {
        if self.is_cow_mode() {
            // Check if removed
            if let Some(ref removed) = self.lists_removed {
                if removed.contains(key) {
                    return None;
                }
            }

            // Check COW cache first (if exists)
            if let Some(ref updated) = self.lists_updated {
                if let Some(list) = updated.get(key) {
                    return Some(list.len());
                }
            }
            // If not in updated, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map(|list| list.len())
    }

    /// Push element to front of list (incremental COW: only records change)
    pub fn push_front(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if self.is_cow_mode() {
            // COW mode: get current list, modify, and record
            let key_clone = key.clone();
            let current = self.get_list(&key).unwrap_or_default();
            let mut new_list = current;
            new_list.push_front(value);
            self.lists_updated.as_mut().unwrap().insert(key, new_list);
            // Remove from removed cache if present
            if let Some(ref mut removed) = self.lists_removed {
                removed.remove(&key_clone);
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.entry(key).or_default().push_front(value);
        }
    }

    /// Push element to back of list (incremental COW: only records change)
    pub fn push_back(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if self.is_cow_mode() {
            // COW mode: get current list, modify, and record
            let key_clone = key.clone();
            let current = self.get_list(&key).unwrap_or_default();
            let mut new_list = current;
            new_list.push_back(value);
            self.lists_updated.as_mut().unwrap().insert(key, new_list);
            // Remove from removed cache if present
            if let Some(ref mut removed) = self.lists_removed {
                removed.remove(&key_clone);
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.entry(key).or_default().push_back(value);
        }
    }

    /// Pop element from front of list (incremental COW: only records change)
    pub fn pop_front(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if self.is_cow_mode() {
            // COW mode: get current list, modify, and record
            let current = self.get_list(key)?;
            let mut new_list = current;
            let result = new_list.pop_front();
            if new_list.is_empty() {
                // If list becomes empty, remove it
                self.lists_removed.as_mut().unwrap().insert(key.to_vec());
                self.lists_updated.as_mut().unwrap().remove(key);
            } else {
                self.lists_updated
                    .as_mut()
                    .unwrap()
                    .insert(key.to_vec(), new_list);
            }
            result
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.get_mut(key)?.pop_front()
        }
    }

    /// Pop element from back of list (incremental COW: only records change)
    pub fn pop_back(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if self.is_cow_mode() {
            // COW mode: get current list, modify, and record
            let current = self.get_list(key)?;
            let mut new_list = current;
            let result = new_list.pop_back();
            if new_list.is_empty() {
                // If list becomes empty, remove it
                self.lists_removed.as_mut().unwrap().insert(key.to_vec());
                self.lists_updated.as_mut().unwrap().remove(key);
            } else {
                self.lists_updated
                    .as_mut()
                    .unwrap()
                    .insert(key.to_vec(), new_list);
            }
            result
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.get_mut(key)?.pop_back()
        }
    }

    /// Get element at index (read operation, no copy)
    pub fn get(&self, key: &[u8], index: usize) -> Option<Vec<u8>> {
        if self.is_cow_mode() {
            // Check if removed
            if let Some(ref removed) = self.lists_removed {
                if removed.contains(key) {
                    return None;
                }
            }

            // Check COW cache first (if exists)
            if let Some(ref updated) = self.lists_updated {
                if let Some(list) = updated.get(key) {
                    return list.get(index).cloned();
                }
            }
            // If not in updated, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key)?.get(index).cloned()
    }

    /// Set element at index (incremental COW: only records change)
    pub fn set(&mut self, key: Vec<u8>, index: usize, value: Vec<u8>) -> bool {
        if self.is_cow_mode() {
            // COW mode: get current list, modify, and record
            if let Some(current) = self.get_list(&key) {
                if index >= current.len() {
                    return false;
                }
                let mut new_list = current;
                new_list[index] = value;
                self.lists_updated
                    .as_mut()
                    .unwrap()
                    .insert(key.clone(), new_list);
                // Remove from removed cache if present
                if let Some(ref mut removed) = self.lists_removed {
                    removed.remove(&key);
                }
                true
            } else {
                false
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            if let Some(list) = base.get_mut(&key) {
                if let Some(elem) = list.get_mut(index) {
                    *elem = value;
                    return true;
                }
            }
            false
        }
    }

    /// Get range of elements [start, end] (read operation, no copy)
    pub fn range(&self, key: &[u8], start: usize, end: usize) -> Option<Vec<Vec<u8>>> {
        if self.is_cow_mode() {
            // Check if removed
            if let Some(ref removed) = self.lists_removed {
                if removed.contains(key) {
                    return None;
                }
            }

            // Check COW cache first (if exists)
            if let Some(ref updated) = self.lists_updated {
                if let Some(list) = updated.get(key) {
                    return Some(
                        list.iter()
                            .skip(start)
                            .take(end.saturating_sub(start).saturating_add(1))
                            .cloned()
                            .collect(),
                    );
                }
            }
            // If not in updated, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map(|list| {
            list.iter()
                .skip(start)
                .take(end.saturating_sub(start).saturating_add(1))
                .cloned()
                .collect()
        })
    }

    /// Clear list for key (incremental COW: only records change)
    pub fn clear(&mut self, key: &[u8]) -> bool {
        if self.is_cow_mode() {
            // COW mode: mark as removed
            if self.contains_key(key) {
                self.lists_removed.as_mut().unwrap().insert(key.to_vec());
                self.lists_updated.as_mut().unwrap().remove(key);
                true
            } else {
                false
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.remove(key).is_some()
        }
    }

    /// Remove list for key (incremental COW: only records change)
    pub fn remove(&mut self, key: &[u8]) -> bool {
        self.clear(key)
    }

    /// Get all keys (read operation)
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys = HashSet::new();

        if self.is_cow_mode() {
            // Add keys from base (excluding removed)
            let base = self.base.read();
            if let Some(ref removed) = self.lists_removed {
                for key in base.keys() {
                    if !removed.contains(key) {
                        keys.insert(key.clone());
                    }
                }
            } else {
                for key in base.keys() {
                    keys.insert(key.clone());
                }
            }
            drop(base);

            // Add keys from COW cache
            if let Some(ref updated) = self.lists_updated {
                for key in updated.keys() {
                    keys.insert(key.clone());
                }
            }
        } else {
            // Fall back to base (read lock)
            let base = self.base.read();
            keys = base.keys().cloned().collect();
        }

        keys.into_iter().collect()
    }

    /// Get total key count (read operation)
    pub fn key_count(&self) -> usize {
        self.keys().len()
    }

    /// Clear all lists (incremental COW: only records change)
    pub fn clear_all(&mut self) {
        if self.is_cow_mode() {
            // COW mode: mark all existing keys as removed
            let base = self.base.read();
            let keys: Vec<Vec<u8>> = base.keys().cloned().collect();
            drop(base);
            for key in keys {
                self.lists_removed.as_mut().unwrap().insert(key);
            }
            self.lists_updated.as_mut().unwrap().clear();
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.clear();
        }
    }

    /// Get reference count (for debugging)
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.base)
    }

    /// Check if in COW mode (for debugging)
    pub fn is_in_cow_mode(&self) -> bool {
        self.is_cow_mode()
    }
}

impl Default for ListStoreCow {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_store_basic_operations() {
        let mut store = ListStoreCow::new();

        // Push elements
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.push_back(b"list1".to_vec(), b"b".to_vec());
        store.push_front(b"list1".to_vec(), b"c".to_vec());

        assert_eq!(store.len(b"list1"), Some(3));
        assert_eq!(store.get(b"list1", 0), Some(b"c".to_vec()));
        assert_eq!(store.get(b"list1", 1), Some(b"a".to_vec()));
        assert_eq!(store.get(b"list1", 2), Some(b"b".to_vec()));

        // Pop elements
        assert_eq!(store.pop_front(b"list1"), Some(b"c".to_vec()));
        assert_eq!(store.pop_back(b"list1"), Some(b"b".to_vec()));
        assert_eq!(store.len(b"list1"), Some(1));

        // Set element
        assert!(store.set(b"list1".to_vec(), 0, b"x".to_vec()));
        assert_eq!(store.get(b"list1", 0), Some(b"x".to_vec()));
    }

    #[test]
    fn test_list_store_snapshot_no_copy() {
        let mut store = ListStoreCow::new();
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.push_back(b"list1".to_vec(), b"b".to_vec());
        store.push_back(b"list1".to_vec(), b"c".to_vec());

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
        assert_eq!(snapshot_data.get(b"list1".as_slice()).unwrap().len(), 3);
        drop(snapshot_data);

        // Original should still work
        assert_eq!(store.len(b"list1"), Some(3));
        assert_eq!(store.get(b"list1", 0), Some(b"a".to_vec()));
    }

    #[test]
    fn test_list_store_write_after_snapshot_no_copy() {
        let mut store = ListStoreCow::new();
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.push_back(b"list1".to_vec(), b"b".to_vec());

        // Create snapshot
        let snapshot = store.make_snapshot();
        assert_eq!(store.ref_count(), 2);
        assert!(store.is_in_cow_mode());

        // Write operation records change in COW cache (NO data copy)
        store.push_back(b"list1".to_vec(), b"c".to_vec());
        assert_eq!(store.ref_count(), 2); // Ref count unchanged (no copy!)
        assert_eq!(Arc::strong_count(&snapshot), 2);
        assert!(store.is_in_cow_mode());

        // Original should have new data (via COW cache)
        assert_eq!(store.len(b"list1"), Some(3));
        assert_eq!(store.get(b"list1", 2), Some(b"c".to_vec()));

        // Snapshot should have old data (unchanged, from base)
        let snapshot_data = snapshot.read();
        assert_eq!(snapshot_data.get(b"list1".as_slice()).unwrap().len(), 2);
        assert_eq!(snapshot_data.get(b"list1".as_slice()).unwrap().get(2), None);
        drop(snapshot_data);
    }

    #[test]
    fn test_list_store_write_without_snapshot_no_copy() {
        let mut store = ListStoreCow::new();
        store.push_back(b"list1".to_vec(), b"a".to_vec());

        // No snapshot, ref count is 1
        assert_eq!(store.ref_count(), 1);

        // Write operation should NOT copy (ref count stays 1)
        store.push_back(b"list1".to_vec(), b"b".to_vec());
        assert_eq!(store.ref_count(), 1); // No copy happened

        // Another write
        store.push_back(b"list1".to_vec(), b"c".to_vec());
        assert_eq!(store.ref_count(), 1); // Still no copy
    }

    #[test]
    fn test_list_store_merge_applies_only_changes() {
        let mut store = ListStoreCow::new();
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.push_back(b"list1".to_vec(), b"b".to_vec());
        store.push_back(b"list1".to_vec(), b"c".to_vec());

        // Create snapshot
        let snapshot = store.make_snapshot();
        assert_eq!(store.ref_count(), 2);

        // Make changes (only 3 operations, not full copy)
        store.push_back(b"list1".to_vec(), b"d".to_vec()); // Add new
        store.set(b"list1".to_vec(), 0, b"x".to_vec()); // Update existing
        store.pop_front(b"list1"); // Remove first

        // Before merge: changes are in COW cache
        assert!(store.is_in_cow_mode());
        assert_eq!(store.len(b"list1"), Some(3)); // x, b, c, d -> after pop_front: b, c, d
        assert_eq!(store.get(b"list1", 0), Some(b"b".to_vec()));
        assert_eq!(store.get(b"list1", 1), Some(b"c".to_vec()));
        assert_eq!(store.get(b"list1", 2), Some(b"d".to_vec()));

        // Snapshot still has old data
        let snapshot_data = snapshot.read();
        assert_eq!(snapshot_data.get(b"list1".as_slice()).unwrap().len(), 3);
        assert_eq!(
            snapshot_data.get(b"list1".as_slice()).unwrap().get(0),
            Some(&b"a".to_vec())
        );
        drop(snapshot_data);

        // Merge: applies only changes to base (O(M) where M=3, not O(N))
        store.merge_cow();
        assert!(!store.is_in_cow_mode());
        // Ref count is still 2 because snapshot still exists (this is correct)
        assert_eq!(store.ref_count(), 2);

        // After merge: changes are in base
        assert_eq!(store.len(b"list1"), Some(3));
        assert_eq!(store.get(b"list1", 0), Some(b"b".to_vec()));
        assert_eq!(store.get(b"list1", 1), Some(b"c".to_vec()));
        assert_eq!(store.get(b"list1", 2), Some(b"d".to_vec()));
    }

    #[test]
    fn test_list_store_range() {
        let mut store = ListStoreCow::new();
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.push_back(b"list1".to_vec(), b"b".to_vec());
        store.push_back(b"list1".to_vec(), b"c".to_vec());
        store.push_back(b"list1".to_vec(), b"d".to_vec());

        // Get range
        let range = store.range(b"list1", 1, 2).unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], b"b".to_vec());
        assert_eq!(range[1], b"c".to_vec());
    }

    #[test]
    fn test_list_store_multiple_keys() {
        let mut store = ListStoreCow::new();
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.push_back(b"list2".to_vec(), b"x".to_vec());
        store.push_back(b"list2".to_vec(), b"y".to_vec());

        assert_eq!(store.key_count(), 2);
        assert_eq!(store.len(b"list1"), Some(1));
        assert_eq!(store.len(b"list2"), Some(2));

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Modify only list1
        store.push_back(b"list1".to_vec(), b"b".to_vec());

        // list1 should be updated
        assert_eq!(store.len(b"list1"), Some(2));
        // list2 should be unchanged (from base)
        assert_eq!(store.len(b"list2"), Some(2));
    }
}
