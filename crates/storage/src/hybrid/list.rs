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

        // Apply updates/additions first (updated takes precedence over removed)
        // This handles both new and updated lists
        if let Some(ref updated) = updated {
            for (key, list_data) in updated {
                // ListData is copied here (required for consistency)
                base.insert(key.clone(), list_data.clone());
            }
        }

        // Apply removals (only if not in updated - updated takes precedence)
        if let Some(ref removed) = removed {
            for key in removed {
                // Only remove if not being updated (updated already applied above)
                if updated.as_ref().map_or(true, |u| !u.contains_key(key)) {
                    base.remove(key);
                }
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
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.lists_updated {
                if let Some(list) = updated.get(key) {
                    return Some(list.clone());
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.lists_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).cloned()
    }

    /// Check if key exists (read operation, merges COW cache + base)
    pub fn contains_key(&self, key: &[u8]) -> bool {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.lists_updated {
                if updated.contains_key(key) {
                    return true;
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.lists_removed {
                if removed.contains(key) {
                    return false;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.contains_key(key)
    }

    /// Get list length for key (read operation, no copy)
    pub fn len(&self, key: &[u8]) -> Option<usize> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.lists_updated {
                if let Some(list) = updated.get(key) {
                    return Some(list.len());
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.lists_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map(|list| list.len())
    }

    /// Push element to front of list (incremental COW: only records change)
    pub fn push_front(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if self.is_cow_mode() {
            let updated = self.lists_updated.as_mut().unwrap();
            let removed = self.lists_removed.as_mut().unwrap();

            // Check if already in COW cache (already copied)
            if let Some(list) = updated.get_mut(&key) {
                // Already copied: directly modify (no additional copy!)
                list.push_front(value);
            } else {
                // Not in COW cache: copy from base once, then modify
                let mut list = {
                    // Check if removed
                    if removed.contains(&key) {
                        VecDeque::new()
                    } else {
                        // Copy from base (first modification)
                        let base = self.base.read();
                        base.get(&key).cloned().unwrap_or_default()
                    }
                };
                list.push_front(value);
                updated.insert(key.clone(), list);
            }

            // Remove from removed cache if present
            removed.remove(&key);
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.entry(key).or_default().push_front(value);
        }
    }

    /// Push element to back of list (incremental COW: only records change)
    pub fn push_back(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if self.is_cow_mode() {
            let updated = self.lists_updated.as_mut().unwrap();
            let removed = self.lists_removed.as_mut().unwrap();

            // Check if already in COW cache (already copied)
            if let Some(list) = updated.get_mut(&key) {
                // Already copied: directly modify (no additional copy!)
                list.push_back(value);
            } else {
                // Not in COW cache: copy from base once, then modify
                let mut list = {
                    // Check if removed
                    if removed.contains(&key) {
                        VecDeque::new()
                    } else {
                        // Copy from base (first modification)
                        let base = self.base.read();
                        base.get(&key).cloned().unwrap_or_default()
                    }
                };
                list.push_back(value);
                updated.insert(key.clone(), list);
            }

            // Remove from removed cache if present
            removed.remove(&key);
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.entry(key).or_default().push_back(value);
        }
    }

    /// Pop element from front of list (incremental COW: only records change)
    pub fn pop_front(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if self.is_cow_mode() {
            let updated = self.lists_updated.as_mut().unwrap();
            let removed = self.lists_removed.as_mut().unwrap();

            // Check if already in COW cache (already copied)
            if let Some(list) = updated.get_mut(key) {
                // Already copied: directly modify (no additional copy!)
                let result = list.pop_front();
                if list.is_empty() {
                    // If list becomes empty, remove it
                    removed.insert(key.to_vec());
                    updated.remove(key);
                }
                result
            } else {
                // Not in COW cache: copy from base once, then modify
                let mut list = {
                    // Check if removed
                    if removed.contains(key) {
                        return None;
                    }
                    // Copy from base (first modification)
                    let base = self.base.read();
                    base.get(key)?.clone()
                };
                let result = list.pop_front();
                if list.is_empty() {
                    // If list becomes empty, remove it
                    removed.insert(key.to_vec());
                    updated.remove(key);
                } else {
                    updated.insert(key.to_vec(), list);
                }
                result
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.get_mut(key)?.pop_front()
        }
    }

    /// Pop element from back of list (incremental COW: only records change)
    pub fn pop_back(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if self.is_cow_mode() {
            let updated = self.lists_updated.as_mut().unwrap();
            let removed = self.lists_removed.as_mut().unwrap();

            // Check if already in COW cache (already copied)
            if let Some(list) = updated.get_mut(key) {
                // Already copied: directly modify (no additional copy!)
                let result = list.pop_back();
                if list.is_empty() {
                    // If list becomes empty, remove it
                    removed.insert(key.to_vec());
                    updated.remove(key);
                }
                result
            } else {
                // Not in COW cache: copy from base once, then modify
                let mut list = {
                    // Check if removed
                    if removed.contains(key) {
                        return None;
                    }
                    // Copy from base (first modification)
                    let base = self.base.read();
                    base.get(key)?.clone()
                };
                let result = list.pop_back();
                if list.is_empty() {
                    // If list becomes empty, remove it
                    removed.insert(key.to_vec());
                } else {
                    updated.insert(key.to_vec(), list);
                }
                result
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.get_mut(key)?.pop_back()
        }
    }

    /// Get element at index (read operation, no copy)
    pub fn get(&self, key: &[u8], index: usize) -> Option<Vec<u8>> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.lists_updated {
                if let Some(list) = updated.get(key) {
                    return list.get(index).cloned();
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.lists_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key)?.get(index).cloned()
    }

    /// Set element at index (incremental COW: only records change)
    pub fn set(&mut self, key: Vec<u8>, index: usize, value: Vec<u8>) -> bool {
        if self.is_cow_mode() {
            let updated = self.lists_updated.as_mut().unwrap();
            let removed = self.lists_removed.as_mut().unwrap();

            // Check if already in COW cache (already copied)
            if let Some(list) = updated.get_mut(&key) {
                // Already copied: directly modify (no additional copy!)
                if index >= list.len() {
                    return false;
                }
                list[index] = value;
                true
            } else {
                // Not in COW cache: copy from base once, then modify
                // Check if removed
                if removed.contains(&key) {
                    return false;
                }
                // Copy from base (first modification)
                let mut list = {
                    let base = self.base.read();
                    if let Some(list) = base.get(&key).cloned() {
                        list
                    } else {
                        return false;
                    }
                };
                if index >= list.len() {
                    return false;
                }
                list[index] = value;
                updated.insert(key.clone(), list);
                // Remove from removed cache if present
                removed.remove(&key);
                true
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
            // Check COW cache first (updated takes precedence over removed)
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

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.lists_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
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
            let updated = self.lists_updated.as_mut().unwrap();
            let removed = self.lists_removed.as_mut().unwrap();

            // Check if already removed
            if removed.contains(key) {
                return true; // Already removed
            }

            // Check if exists in updated or base
            let exists = updated.contains_key(key) || {
                let base = self.base.read();
                base.contains_key(key)
            };

            if exists {
                // Mark as removed
                removed.insert(key.to_vec());
                updated.remove(key); // Remove from updated if present
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

    #[test]
    fn test_list_store_updated_overrides_removed() {
        let mut store = ListStoreCow::new();
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.push_back(b"list1".to_vec(), b"b".to_vec());

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Remove list1 (adds to removed cache)
        assert!(store.clear(b"list1"));
        assert!(!store.contains_key(b"list1"));
        assert_eq!(store.len(b"list1"), None);

        // Re-add list1 (adds to updated cache, should override removed)
        store.push_back(b"list1".to_vec(), b"c".to_vec());
        assert!(store.contains_key(b"list1")); // Should exist (updated overrides removed)
        assert_eq!(store.len(b"list1"), Some(1));
        assert_eq!(store.get(b"list1", 0), Some(b"c".to_vec()));

        // Merge: updated should take precedence
        store.merge_cow();
        assert!(store.contains_key(b"list1")); // Should still exist after merge
        assert_eq!(store.len(b"list1"), Some(1));
        assert_eq!(store.get(b"list1", 0), Some(b"c".to_vec()));
    }

    #[test]
    fn test_list_store_merge_updated_overrides_removed() {
        let mut store = ListStoreCow::new();
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.push_back(b"list2".to_vec(), b"x".to_vec());

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Remove list1
        assert!(store.clear(b"list1"));
        assert!(!store.contains_key(b"list1"));

        // Re-add list1 (should override removed)
        store.push_back(b"list1".to_vec(), b"b".to_vec());
        assert!(store.contains_key(b"list1"));

        // Merge: updated should take precedence over removed
        store.merge_cow();
        assert!(store.contains_key(b"list1")); // Should exist (updated overrides removed)
        assert_eq!(store.len(b"list1"), Some(1));
        assert_eq!(store.get(b"list1", 0), Some(b"b".to_vec()));

        // list2 should still exist
        assert!(store.contains_key(b"list2"));
        assert_eq!(store.len(b"list2"), Some(1));
    }

    // ========== Additional COW Tests (similar to set.rs patterns) ==========

    #[test]
    fn test_list_data_cow_clear_in_cow_mode() {
        let mut store = ListStoreCow::new();
        
        // Add initial data
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.push_back(b"list1".to_vec(), b"b".to_vec());
        store.push_back(b"list2".to_vec(), b"x".to_vec());
        
        // Create snapshot
        store.make_snapshot();
        assert!(store.is_cow_mode());
        
        // Clear lists in COW mode
        assert!(store.clear(b"list1"));
        assert!(store.clear(b"list2"));
        
        // Verify cleared state
        assert!(!store.contains_key(b"list1"));
        assert!(!store.contains_key(b"list2"));
        assert_eq!(store.len(b"list1"), None);
        assert_eq!(store.len(b"list2"), None);
        
        // Verify keys() returns empty
        let keys = store.keys();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_list_data_cow_multiple_snapshots() {
        let mut store1 = ListStoreCow::new();
        
        // Add data to store1
        store1.push_back(b"list1".to_vec(), b"a".to_vec());
        store1.push_back(b"list1".to_vec(), b"b".to_vec());
        store1.push_back(b"list2".to_vec(), b"x".to_vec());
        
        // Create snapshot and get reference
        let snapshot = store1.make_snapshot();
        assert_eq!(store1.ref_count(), 2);
        
        // Create store2 from same base (simulating multiple snapshots)
        let mut store2 = ListStoreCow {
            base: Arc::clone(&store1.base),
            lists_updated: Some(HashMap::new()),
            lists_removed: Some(HashSet::new()),
        };
        
        // Both should share the same base
        assert_eq!(store1.ref_count(), 3); // store1.base + snapshot + store2
        assert!(store2.is_cow_mode());
        
        // Make independent changes
        store1.push_back(b"list1".to_vec(), b"c".to_vec()); // Add to list1
        store2.push_back(b"list2".to_vec(), b"y".to_vec()); // Add to list2
        store2.clear(b"list1"); // Remove list1 in store2
        
        // Verify independent states
        assert_eq!(store1.len(b"list1"), Some(3)); // a, b, c
        assert_eq!(store1.len(b"list2"), Some(1)); // x
        assert!(store1.contains_key(b"list1"));
        
        assert_eq!(store2.len(b"list1"), None); // cleared
        assert_eq!(store2.len(b"list2"), Some(2)); // x, y
        assert!(!store2.contains_key(b"list1"));
    }

    #[test]
    fn test_list_data_cow_edge_cases() {
        let mut store = ListStoreCow::new();
        
        // Test empty data
        store.push_back(vec![], vec![]);
        assert!(store.contains_key(&[]));
        assert_eq!(store.len(&[]), Some(1));
        assert_eq!(store.get(&[], 0), Some(vec![]));
        
        // Test large data
        let large_data = vec![b'x'; 1024]; // 1KB element
        store.push_back(b"large".to_vec(), large_data.clone());
        assert!(store.contains_key(b"large"));
        assert_eq!(store.len(b"large"), Some(1));
        assert_eq!(store.get(b"large", 0), Some(large_data));
        
        // Test binary data
        let binary_data = vec![0, 1, 2, 255, 254, 253];
        store.push_back(b"binary".to_vec(), binary_data.clone());
        assert!(store.contains_key(b"binary"));
        assert_eq!(store.get(b"binary", 0), Some(binary_data));
        
        // Test with snapshot
        store.make_snapshot();
        store.push_back(b"binary".to_vec(), vec![128, 129, 130]);
        assert_eq!(store.len(b"binary"), Some(2));
    }

    #[test]
    fn test_list_data_cow_concurrent_read_write() {
        use std::sync::Arc;
        use std::thread;
        
        let mut store = ListStoreCow::new();
        
        // Add initial data
        for i in 0..10 {
            store.push_back(b"shared".to_vec(), format!("item{}", i).into_bytes());
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
                    let _len = store.len(b"shared");
                    let _item = store.get(b"shared", i % 10);
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
                store.push_back(b"shared".to_vec(), format!("item{}", i).into_bytes());
            }
        });
        
        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
        writer_handle.join().unwrap();
        
        // Verify final state
        let store = store_arc.read();
        assert_eq!(store.len(b"shared"), Some(15));
    }

    #[test]
    fn test_list_data_cow_multiple_merges() {
        let mut store = ListStoreCow::new();
        
        // First cycle: add data and merge
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.make_snapshot();
        store.push_back(b"list1".to_vec(), b"b".to_vec());
        store.merge_cow();
        
        // Second cycle: modify and merge again
        store.make_snapshot();
        store.push_back(b"list1".to_vec(), b"c".to_vec());
        store.pop_front(b"list1");
        store.merge_cow();
        
        // Verify final state
        assert_eq!(store.len(b"list1"), Some(2)); // b, c
        assert_eq!(store.get(b"list1", 0), Some(b"b".to_vec()));
        assert_eq!(store.get(b"list1", 1), Some(b"c".to_vec()));
    }

    #[test]
    fn test_list_store_cow_from_data() {
        let mut data = HashMap::new();
        let mut list1 = VecDeque::new();
        list1.push_back(b"a".to_vec());
        list1.push_back(b"b".to_vec());
        data.insert(b"list1".to_vec(), list1);
        
        let mut store = ListStoreCow::from_data(data);
        
        // Verify initial data
        assert_eq!(store.len(b"list1"), Some(2));
        assert_eq!(store.get(b"list1", 0), Some(b"a".to_vec()));
        assert_eq!(store.get(b"list1", 1), Some(b"b".to_vec()));
        
        // Create snapshot and modify
        store.make_snapshot();
        store.push_back(b"list1".to_vec(), b"c".to_vec());
        
        // Verify changes
        assert_eq!(store.len(b"list1"), Some(3));
        assert_eq!(store.get(b"list1", 2), Some(b"c".to_vec()));
    }

    #[test]
    fn test_list_store_cow_clear_operations() {
        let mut store = ListStoreCow::new();
        
        // Add some data
        store.push_back(b"list1".to_vec(), b"a".to_vec());
        store.push_back(b"list1".to_vec(), b"b".to_vec());
        store.push_back(b"list2".to_vec(), b"x".to_vec());
        
        // Create snapshot
        store.make_snapshot();
        
        // Test clear operations
        assert!(store.clear(b"list1"));
        assert!(!store.clear(b"nonexistent"));
        
        // Verify state
        assert!(!store.contains_key(b"list1"));
        assert!(store.contains_key(b"list2"));
        assert_eq!(store.len(b"list1"), None);
        assert_eq!(store.len(b"list2"), Some(1));
        
        // Test merge
        store.merge_cow();
        assert!(!store.contains_key(b"list1"));
        assert!(store.contains_key(b"list2"));
    }
}
