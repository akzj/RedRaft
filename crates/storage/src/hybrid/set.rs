//! Set Data Structure with Incremental Copy-on-Write (COW)
//!
//! True incremental COW semantics - only changed items are recorded, NOT full data copy:
//! - Snapshot: Only clones Arc (increases ref count), NO data copy
//! - Write: Records changes in small COW cache (only changed items), NO full copy
//! - Read: Merges COW cache + base data (O(1) lookup)
//! - Merge: Applies only changed items to base (O(M) where M = changes, not total data)
//!
//! Example: 1000 billion items, modify 3 items
//! - Old approach (Arc::make_mut): Copies all 1000 billion items ❌
//! - This approach: Only records 3 changes in small HashSet ✅
//!
//! This module provides the core data structure for sets,
//! without implementing Redis API traits.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Set data structure
///
/// A set maintains a collection of unique members.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetData {
    pub members: HashSet<Vec<u8>>,
}

impl SetData {
    /// Create a new empty Set
    pub fn new() -> Self {
        Self {
            members: HashSet::new(),
        }
    }

    /// Add a member
    pub fn add(&mut self, member: Vec<u8>) -> bool {
        self.members.insert(member)
    }

    /// Remove a member
    pub fn remove(&mut self, member: &[u8]) -> bool {
        self.members.remove(member)
    }

    /// Check if member exists
    pub fn contains(&self, member: &[u8]) -> bool {
        self.members.contains(member)
    }

    /// Get member count
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.members.clear();
    }

    /// Get all members
    pub fn members(&self) -> Vec<Vec<u8>> {
        self.members.iter().cloned().collect()
    }

    /// Get intersection with another set
    pub fn intersect(&self, other: &SetData) -> SetData {
        let mut result = SetData::new();
        for member in &self.members {
            if other.members.contains(member) {
                result.members.insert(member.clone());
            }
        }
        result
    }

    /// Get union with another set
    pub fn union(&self, other: &SetData) -> SetData {
        let mut result = SetData::new();
        for member in &self.members {
            result.members.insert(member.clone());
        }
        for member in &other.members {
            result.members.insert(member.clone());
        }
        result
    }

    /// Get difference (self - other)
    pub fn difference(&self, other: &SetData) -> SetData {
        let mut result = SetData::new();
        for member in &self.members {
            if !other.members.contains(member) {
                result.members.insert(member.clone());
            }
        }
        result
    }
}

impl Default for SetData {
    fn default() -> Self {
        Self::new()
    }
}

/// Set with Incremental Copy-on-Write (COW) support
///
/// True incremental COW semantics:
/// - `make_snapshot()`: Only clones Arc (increases ref count), NO data copy
/// - `add()`/`remove()`: Records changes in small COW cache (only changed items), NO full copy
/// - Read operations: Merges COW cache + base data (O(1) lookup)
/// - `merge_cow()`: Applies only changed items to base via RwLock (O(M) where M = changes, not total data)
///
/// This avoids full data copy even for 1000 billion items when only 3 items change.
#[derive(Debug, Clone)]
pub struct SetDataCow {
    /// Base data (shared via Arc<RwLock<>>, can be directly modified without clone)
    base: Arc<RwLock<SetData>>,

    /// COW cache: Updated/added members (only changed items)
    members_updated: Option<HashSet<Vec<u8>>>,

    /// COW cache: Removed members
    members_removed: Option<HashSet<Vec<u8>>>,
}

impl SetDataCow {
    /// Create a new empty Set with COW support
    pub fn new() -> Self {
        Self {
            base: Arc::new(RwLock::new(SetData::new())),
            members_updated: None,
            members_removed: None,
        }
    }

    /// Create from existing SetData
    pub fn from_data(data: SetData) -> Self {
        Self {
            base: Arc::new(RwLock::new(data)),
            members_updated: None,
            members_removed: None,
        }
    }

    /// Check if in COW mode (has snapshot)
    fn is_cow_mode(&self) -> bool {
        self.members_updated.is_some()
    }

    /// Create a snapshot (only increases reference count, NO data copy)
    ///
    /// Returns a cloned Arc that shares the same base data.
    /// Write operations will record changes in COW cache instead of copying data.
    pub fn make_snapshot(&mut self) -> Arc<RwLock<SetData>> {
        if self.is_cow_mode() {
            // Already in COW mode, just return existing base
            return Arc::clone(&self.base);
        }

        // Enter COW mode: initialize caches
        self.members_updated = Some(HashSet::new());
        self.members_removed = Some(HashSet::new());

        // ✅ Only clone Arc (O(1)), NO data copy
        Arc::clone(&self.base)
    }

    /// Create a COW instance (only increases reference count, NO data copy)
    ///
    /// Returns a new SetDataCow that shares the same base data.
    /// Write operations will record changes in COW cache instead of copying data.
    pub fn make_cow(&mut self) -> SetDataCow {
        SetDataCow {
            base: Arc::clone(&self.base),
            members_updated: Some(HashSet::new()),
            members_removed: Some(HashSet::new()),
        }
    }

    /// Merge COW changes back to base (applies only changed items)
    ///
    /// This is called when snapshot is no longer needed.
    /// Only changed items are applied via RwLock, not full data copy.
    pub fn merge_cow(&mut self) {
        if !self.is_cow_mode() {
            return;
        }

        let updated = self.members_updated.take();
        let removed = self.members_removed.take();

        // ✅ Get write lock and directly modify base (NO clone!)
        let mut base = self.base.write();

        // Apply removals first (only if not in updated - updated takes precedence)
        if let Some(ref removed) = removed {
            for member in removed {
                // Only remove if not being updated
                if updated.as_ref().map_or(true, |u| !u.contains(member)) {
                    base.remove(member);
                }
            }
        }

        // Apply updates/additions (this handles both new and updated members)
        if let Some(ref updated) = updated {
            for member in updated {
                base.add(member.clone());
            }
        }

        // Write lock is released here, no need to replace base
    }

    /// Add a member (incremental COW: only records change)
    pub fn add(&mut self, member: Vec<u8>) -> bool {
        if self.is_cow_mode() {
            // COW mode: record change in cache, don't modify base
            let updated = self.members_updated.as_mut().unwrap();
            let removed = self.members_removed.as_mut().unwrap();

            // Check if member exists in base (read lock)
            let exists_in_base = {
                let base = self.base.read();
                base.contains(&member)
            }; // Read lock released here

            // Check if already in updated cache
            if updated.contains(&member) {
                return false; // Already in updated, no change
            }

            if exists_in_base {
                // Member already exists in base
                // If it was removed, we need to re-add it
                if removed.contains(&member) {
                    removed.remove(&member);
                    updated.insert(member);
                    true // Re-added after removal
                } else {
                    false // Already exists, no change
                }
            } else {
                // New member: add to updated, remove from removed if present
                removed.remove(&member);
                updated.insert(member)
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.add(member)
        }
    }

    /// Remove a member (incremental COW: only records change)
    pub fn remove(&mut self, member: &[u8]) -> bool {
        if self.is_cow_mode() {
            // COW mode: record change in cache
            let updated = self.members_updated.as_mut().unwrap();
            let removed = self.members_removed.as_mut().unwrap();

            // Check if member exists in base (read lock)
            let exists_in_base = {
                let base = self.base.read();
                base.contains(member)
            }; // Read lock released here

            if exists_in_base {
                // Remove from updated cache if present
                updated.remove(member);
                // Record removal
                removed.insert(member.to_vec());
                true
            } else if updated.contains(member) {
                // Member was added in COW cache, just remove it
                updated.remove(member);
                true
            } else {
                false
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.remove(member)
        }
    }

    /// Check if member exists (merges COW cache + base)
    pub fn contains(&self, member: &[u8]) -> bool {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.members_updated {
                if updated.contains(member) {
                    return true;
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.members_removed {
                if removed.contains(member) {
                    return false;
                }
            }
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.contains(member)
    }

    /// Get member count (approximate, includes COW changes)
    pub fn len(&self) -> usize {
        let base = self.base.read();
        let base_len = base.len();
        drop(base);

        if self.is_cow_mode() {
            let updated = self.members_updated.as_ref().unwrap();
            let removed = self.members_removed.as_ref().unwrap();
            // Base count - removed + updated (new items)
            let new_items = updated.len() - removed.len();
            base_len + new_items
        } else {
            base_len
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all data
    pub fn clear(&mut self) {
        if self.is_cow_mode() {
            // In COW mode: clear caches and mark all base members as removed
            self.members_updated = Some(HashSet::new());
            let base = self.base.read();
            let mut removed = HashSet::new();
            for member in &base.members {
                removed.insert(member.clone());
            }
            drop(base);
            self.members_removed = Some(removed);
        } else {
            let mut base = self.base.write();
            base.clear();
        }
    }

    /// Get all members (merges COW cache + base)
    pub fn members(&self) -> Vec<Vec<u8>> {
        let mut members = HashSet::new();

        // Add base members (excluding removed) - read lock
        let base = self.base.read();
        if let Some(ref removed) = self.members_removed {
            for member in &base.members {
                if !removed.contains(member) {
                    members.insert(member.clone());
                }
            }
        } else {
            for member in &base.members {
                members.insert(member.clone());
            }
        }
        drop(base);

        // Add updated/added members
        if let Some(ref updated) = self.members_updated {
            for member in updated {
                members.insert(member.clone());
            }
        }

        members.into_iter().collect()
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

impl Default for SetDataCow {
    fn default() -> Self {
        Self::new()
    }
}

/// Set Store with Incremental Copy-on-Write (COW) support
///
/// True incremental COW semantics at HashMap level:
/// - `make_snapshot()`: Only clones Arc (increases ref count), NO data copy
/// - Write operations: Use `SetDataCow::make_cow()` to create COW instances (NO full copy)
/// - Read operations: Merges COW cache + base data (O(1) lookup)
/// - `merge_cow()`: Applies only changed Sets to base (O(M) where M = changes, not total data)
///
/// Note: SetDataCow is created via `make_cow()` when modified (required for consistency),
/// but only for changed Sets. This avoids full HashMap copy even for 1000 billion Sets
/// when only 3 Sets change.
#[derive(Debug, Clone)]
pub struct SetStoreCow {
    /// Base data (shared via Arc<RwLock<>>, can be directly modified without clone)
    base: Arc<RwLock<HashMap<Vec<u8>, SetDataCow>>>,

    /// COW cache: Updated/added Sets (only changed Sets)
    /// SetDataCow is created via make_cow() here (required for consistency)
    sets_updated: Option<HashMap<Vec<u8>, SetDataCow>>,

    /// COW cache: Removed Set keys
    sets_removed: Option<HashSet<Vec<u8>>>,
}

impl SetStoreCow {
    /// Create a new empty SetStore with COW support
    pub fn new() -> Self {
        Self {
            base: Arc::new(RwLock::new(HashMap::new())),
            sets_updated: None,
            sets_removed: None,
        }
    }

    /// Create from existing HashMap
    pub fn from_data(data: HashMap<Vec<u8>, SetDataCow>) -> Self {
        Self {
            base: Arc::new(RwLock::new(data)),
            sets_updated: None,
            sets_removed: None,
        }
    }

    /// Check if in COW mode (has snapshot)
    fn is_cow_mode(&self) -> bool {
        self.sets_updated.is_some()
    }

    /// Create a snapshot (only increases reference count, NO data copy)
    ///
    /// Returns a cloned Arc that shares the same base data.
    /// Write operations will use `make_cow()` to create COW instances instead of copying data.
    pub fn make_snapshot(&mut self) -> Arc<RwLock<HashMap<Vec<u8>, SetDataCow>>> {
        if self.is_cow_mode() {
            // Already in COW mode, just return existing base
            return Arc::clone(&self.base);
        }

        // Enter COW mode: initialize caches
        self.sets_updated = Some(HashMap::new());
        self.sets_removed = Some(HashSet::new());

        // ✅ Only clone Arc (O(1)), NO data copy
        Arc::clone(&self.base)
    }

    /// Merge COW changes back to base (applies only changed Sets)
    ///
    /// This is called when snapshot is no longer needed.
    /// Only changed Sets are applied via RwLock, not full data copy.
    pub fn merge_cow(&mut self) {
        if !self.is_cow_mode() {
            return;
        }

        let updated = self.sets_updated.take();
        let removed = self.sets_removed.take();

        // Collect updated keys before processing (for removal check)
        let updated_keys: HashSet<Vec<u8>> = updated
            .as_ref()
            .map(|u| u.keys().cloned().collect())
            .unwrap_or_default();

        // ✅ Get write lock and directly modify base (NO clone!)
        let mut base = self.base.write();

        // Apply updates/additions first (updated takes precedence over removed)
        // This handles both new and updated Sets
        if let Some(updated) = updated {
            for (key, mut set_cow) in updated {
                // Merge the SetDataCow's changes to its base first
                if set_cow.is_in_cow_mode() {
                    set_cow.merge_cow();
                }

                // Now set_cow's base has the merged data
                // We need to extract the data from set_cow's base and create a new SetDataCow
                // Since we can't directly access the internal data, we'll replace the base entry
                // with a new SetDataCow that shares the same base Arc
                let merged_set = {
                    // Clone the base Arc (only increases ref count, no data copy)
                    let base_arc = Arc::clone(&set_cow.base);
                    // Create new SetDataCow with the merged base
                    SetDataCow {
                        base: base_arc,
                        members_updated: None,
                        members_removed: None,
                    }
                };

                // Replace or insert the merged SetDataCow
                base.insert(key, merged_set);
            }
        }

        // Apply removals (only if not in updated - updated takes precedence)
        if let Some(ref removed) = removed {
            for key in removed {
                // Only remove if not being updated (updated already applied above)
                if !updated_keys.contains(key) {
                    base.remove(key);
                }
            }
        }

        // Write lock is released here, no need to replace base
    }

    /// Get SetDataCow for key (returns reference - for read operations)
    ///
    /// Returns a reference to the SetDataCow if it exists.
    /// For modifications, use `get_set_mut()` instead.
    ///
    /// Note: This method can only return references from COW cache, not from base
    /// (due to lock lifetime constraints). For base lookups, use other methods.
    pub fn get_set(&self, key: &[u8]) -> Option<&SetDataCow> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.sets_updated {
                if let Some(set) = updated.get(key) {
                    return Some(set);
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.sets_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, we can't return a reference from base
            // (due to lock lifetime), so return None
            // Callers should use other methods like contains(), len(), etc.
            return None;
        }

        // No COW mode: can't return reference from base (lock lifetime)
        None
    }

    /// Get mutable SetDataCow for key (creates COW instance if needed)
    ///
    /// This method:
    /// 1. Checks if Set is already in COW cache
    /// 2. If yes, returns mutable reference (no additional copy!)
    /// 3. If no, uses `make_cow()` to create COW instance (no full copy!)
    pub fn get_set_mut(&mut self, key: &[u8]) -> Option<&mut SetDataCow> {
        if self.is_cow_mode() {
            let updated = self.sets_updated.as_mut().unwrap();
            let removed = self.sets_removed.as_mut().unwrap();

            // Check if already in COW cache (already has COW instance)
            if updated.contains_key(key) {
                // Already has COW instance: return mutable reference (no additional copy!)
                return updated.get_mut(key);
            }

            // Not in COW cache: create COW instance via make_cow() (no full copy!)
            let set_cow = {
                // Check if removed
                if removed.contains(key) {
                    // Create new SetDataCow for removed key
                    SetDataCow::new()
                } else {
                    // Get from base and create COW instance
                    let base = self.base.read();
                    if let Some(base_set) = base.get(key) {
                        // Clone the SetDataCow struct (only clones Arc, NO data copy!)
                        // Then call make_cow() to create COW instance (NO full copy!)
                        let mut base_set_clone = base_set.clone();
                        base_set_clone.make_cow()
                    } else {
                        // Create new SetDataCow
                        SetDataCow::new()
                    }
                }
            };
            updated.insert(key.to_vec(), set_cow);

            // Remove from removed cache if present
            removed.remove(key);

            // Return mutable reference to the newly inserted SetDataCow
            updated.get_mut(key)
        } else {
            // No snapshot: cannot return mutable reference from lock
            // Callers should use add(), remove(), etc. methods instead
            // Or create a snapshot first to enable COW mode
            None
        }
    }

    /// Check if key exists (read operation, merges COW cache + base)
    pub fn contains_key(&self, key: &[u8]) -> bool {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.sets_updated {
                if updated.contains_key(key) {
                    return true;
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.sets_removed {
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

    /// Add a member to a Set (incremental COW: only records change)
    pub fn add(&mut self, key: Vec<u8>, member: Vec<u8>) -> bool {
        if let Some(set) = self.get_set_mut(&key) {
            set.add(member)
        } else if !self.is_cow_mode() {
            // No snapshot and key doesn't exist: create new SetDataCow
            let mut base = self.base.write();
            let set = base.entry(key).or_insert_with(|| SetDataCow::new());
            set.add(member)
        } else {
            false
        }
    }

    /// Remove a member from a Set (incremental COW: only records change)
    pub fn remove(&mut self, key: &[u8], member: &[u8]) -> bool {
        if let Some(set) = self.get_set_mut(key) {
            set.remove(member)
        } else if !self.is_cow_mode() {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            if let Some(set) = base.get_mut(key) {
                set.remove(member)
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Check if member exists in a Set (read operation, no copy)
    pub fn contains(&self, key: &[u8], member: &[u8]) -> bool {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.sets_updated {
                if let Some(set) = updated.get(key) {
                    return set.contains(member);
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.sets_removed {
                if removed.contains(key) {
                    return false;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map_or(false, |set| set.contains(member))
    }

    /// Get member count for a Set (read operation, no copy)
    pub fn len(&self, key: &[u8]) -> Option<usize> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.sets_updated {
                if let Some(set) = updated.get(key) {
                    return Some(set.len());
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.sets_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map(|set| set.len())
    }

    /// Clear Set for key (incremental COW: only records change)
    pub fn clear(&mut self, key: &[u8]) -> bool {
        if self.is_cow_mode() {
            let updated = self.sets_updated.as_mut().unwrap();
            let removed = self.sets_removed.as_mut().unwrap();

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

    /// Remove Set for key (incremental COW: only records change)
    pub fn remove_set(&mut self, key: &[u8]) -> bool {
        self.clear(key)
    }

    /// Get all keys (read operation)
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys = HashSet::new();

        if self.is_cow_mode() {
            // Add keys from base (excluding removed)
            let base = self.base.read();
            if let Some(ref removed) = self.sets_removed {
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

            // Add keys from COW cache (updated takes precedence)
            if let Some(ref updated) = self.sets_updated {
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

    /// Get key count (read operation)
    pub fn key_count(&self) -> usize {
        self.keys().len()
    }

    /// Get reference count (for testing)
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.base)
    }

    /// Check if in COW mode (for testing)
    pub fn is_in_cow_mode(&self) -> bool {
        self.is_cow_mode()
    }
}

impl Default for SetStoreCow {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_basic_operations() {
        let mut set = SetData::new();

        // Add members
        assert!(set.add(b"member1".to_vec()));
        assert!(set.add(b"member2".to_vec()));
        assert!(!set.add(b"member1".to_vec())); // Duplicate

        assert_eq!(set.len(), 2);
        assert!(set.contains(b"member1"));
        assert!(set.contains(b"member2"));

        // Remove member
        assert!(set.remove(b"member1"));
        assert!(!set.contains(b"member1"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_set_operations() {
        let mut set1 = SetData::new();
        set1.add(b"a".to_vec());
        set1.add(b"b".to_vec());
        set1.add(b"c".to_vec());

        let mut set2 = SetData::new();
        set2.add(b"b".to_vec());
        set2.add(b"c".to_vec());
        set2.add(b"d".to_vec());

        // Intersection
        let intersection = set1.intersect(&set2);
        assert_eq!(intersection.len(), 2);
        assert!(intersection.contains(b"b"));
        assert!(intersection.contains(b"c"));

        // Union
        let union = set1.union(&set2);
        assert_eq!(union.len(), 4);
        assert!(union.contains(b"a"));
        assert!(union.contains(b"b"));
        assert!(union.contains(b"c"));
        assert!(union.contains(b"d"));

        // Difference
        let difference = set1.difference(&set2);
        assert_eq!(difference.len(), 1);
        assert!(difference.contains(b"a"));
    }

    #[test]
    fn test_set_cow_basic_operations() {
        let mut cow = SetDataCow::new();

        // Add members
        assert!(cow.add(b"member1".to_vec()));
        assert!(cow.add(b"member2".to_vec()));
        assert!(!cow.add(b"member1".to_vec())); // Duplicate

        assert_eq!(cow.len(), 2);
        assert!(cow.contains(b"member1"));
        assert!(cow.contains(b"member2"));

        // Remove member
        assert!(cow.remove(b"member1"));
        assert!(!cow.contains(b"member1"));
        assert_eq!(cow.len(), 1);
    }

    #[test]
    fn test_set_cow_snapshot_no_copy() {
        let mut cow = SetDataCow::new();
        cow.add(b"a".to_vec());
        cow.add(b"b".to_vec());
        cow.add(b"c".to_vec());

        // Before snapshot: ref count should be 1
        assert_eq!(cow.ref_count(), 1);

        // Create snapshot (only increases ref count, no copy)
        let snapshot = cow.make_snapshot();

        // After snapshot: ref count should be 2
        assert_eq!(cow.ref_count(), 2);
        assert_eq!(Arc::strong_count(&snapshot), 2);

        // Snapshot should have same data (via read lock)
        let snapshot_data = snapshot.read();
        assert_eq!(snapshot_data.len(), 3);
        assert!(snapshot_data.contains(b"a"));
        assert!(snapshot_data.contains(b"b"));
        assert!(snapshot_data.contains(b"c"));
        drop(snapshot_data);

        // Original should still work
        assert_eq!(cow.len(), 3);
        assert!(cow.contains(b"a"));
    }

    #[test]
    fn test_set_cow_write_after_snapshot_no_copy() {
        let mut cow = SetDataCow::new();
        cow.add(b"a".to_vec());
        cow.add(b"b".to_vec());

        // Create snapshot
        let snapshot = cow.make_snapshot();
        assert_eq!(cow.ref_count(), 2);
        assert!(cow.is_in_cow_mode());

        // Write operation records change in COW cache (NO data copy)
        cow.add(b"c".to_vec());
        assert_eq!(cow.ref_count(), 2); // Ref count unchanged (no copy!)
        assert_eq!(Arc::strong_count(&snapshot), 2);
        assert!(cow.is_in_cow_mode());

        // Original should have new data (via COW cache)
        assert_eq!(cow.len(), 3);
        assert!(cow.contains(b"c"));

        // Snapshot should have old data (unchanged, from base)
        let snapshot_data = snapshot.read();
        assert_eq!(snapshot_data.len(), 2);
        assert!(!snapshot_data.contains(b"c"));
        drop(snapshot_data);
    }

    #[test]
    fn test_set_cow_write_without_snapshot_no_copy() {
        let mut cow = SetDataCow::new();
        cow.add(b"a".to_vec());

        // No snapshot, ref count is 1
        assert_eq!(cow.ref_count(), 1);

        // Write operation should NOT copy (ref count stays 1)
        cow.add(b"b".to_vec());
        assert_eq!(cow.ref_count(), 1); // No copy happened

        // Another write
        cow.add(b"c".to_vec());
        assert_eq!(cow.ref_count(), 1); // Still no copy
    }

    #[test]
    fn test_set_cow_merge_applies_only_changes() {
        let mut cow = SetDataCow::new();
        cow.add(b"a".to_vec());
        cow.add(b"b".to_vec());
        cow.add(b"c".to_vec());

        // Create snapshot
        let snapshot = cow.make_snapshot();
        assert_eq!(cow.ref_count(), 2);

        // Make changes (only 3 changes, not full copy)
        cow.add(b"d".to_vec()); // Add new
        cow.remove(b"b"); // Remove existing

        // Before merge: changes are in COW cache
        assert!(cow.is_in_cow_mode());
        assert_eq!(cow.len(), 3); // a, c, d
        assert!(cow.contains(b"a"));
        assert!(!cow.contains(b"b"));
        assert!(cow.contains(b"c"));
        assert!(cow.contains(b"d"));

        // Snapshot still has old data
        let snapshot_data = snapshot.read();
        assert_eq!(snapshot_data.len(), 3);
        assert!(snapshot_data.contains(b"a"));
        assert!(snapshot_data.contains(b"b"));
        drop(snapshot_data);

        // Merge: applies only changes to base (O(M) where M=2, not O(N))
        cow.merge_cow();
        assert!(!cow.is_in_cow_mode());
        // Ref count is still 2 because snapshot still exists (this is correct)
        assert_eq!(cow.ref_count(), 2);

        // After merge: changes are in base
        assert_eq!(cow.len(), 3);
        assert!(cow.contains(b"a"));
        assert!(!cow.contains(b"b"));
        assert!(cow.contains(b"c"));
        assert!(cow.contains(b"d"));
    }

    // ========== SetStoreCow Tests ==========

    #[test]
    fn test_set_store_basic_operations() {
        let mut store = SetStoreCow::new();

        // Add members to Set
        assert!(store.add(b"set1".to_vec(), b"member1".to_vec()));
        assert!(store.add(b"set1".to_vec(), b"member2".to_vec()));
        assert!(!store.add(b"set1".to_vec(), b"member1".to_vec())); // Duplicate

        assert_eq!(store.len(b"set1"), Some(2));
        assert!(store.contains(b"set1", b"member1"));
        assert!(store.contains(b"set1", b"member2"));

        // Remove member
        assert!(store.remove(b"set1", b"member1"));
        assert!(!store.contains(b"set1", b"member1"));
        assert_eq!(store.len(b"set1"), Some(1));
    }

    #[test]
    fn test_set_store_snapshot_no_copy() {
        let mut store = SetStoreCow::new();
        store.add(b"set1".to_vec(), b"a".to_vec());
        store.add(b"set1".to_vec(), b"b".to_vec());
        store.add(b"set1".to_vec(), b"c".to_vec());

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
        assert!(snapshot_data
            .get(b"set1".as_slice())
            .unwrap()
            .contains(b"a"));
        drop(snapshot_data);

        // Original should still work
        assert_eq!(store.len(b"set1"), Some(3));
        assert!(store.contains(b"set1", b"a"));
    }

    #[test]
    fn test_set_store_write_after_snapshot_no_copy() {
        let mut store = SetStoreCow::new();
        store.add(b"set1".to_vec(), b"a".to_vec());
        store.add(b"set1".to_vec(), b"b".to_vec());

        // Create snapshot
        let snapshot = store.make_snapshot();
        assert_eq!(store.ref_count(), 2);
        assert!(store.is_in_cow_mode());

        // Write operation records change in COW cache (NO data copy)
        store.add(b"set1".to_vec(), b"c".to_vec());
        assert_eq!(store.ref_count(), 2); // Ref count unchanged (no copy!)
        assert_eq!(Arc::strong_count(&snapshot), 2);
        assert!(store.is_in_cow_mode());

        // Original should have new data (via COW cache)
        assert_eq!(store.len(b"set1"), Some(3));
        assert!(store.contains(b"set1", b"c"));

        // Snapshot should have old data (unchanged, from base)
        let snapshot_data = snapshot.read();
        let base_set = snapshot_data.get(b"set1".as_slice()).unwrap();
        assert_eq!(base_set.len(), 2);
        assert!(!base_set.contains(b"c"));
        drop(snapshot_data);
    }

    #[test]
    fn test_set_store_write_without_snapshot_no_copy() {
        let mut store = SetStoreCow::new();
        store.add(b"set1".to_vec(), b"a".to_vec());

        // No snapshot, ref count is 1
        assert_eq!(store.ref_count(), 1);

        // Write operation should NOT copy (ref count stays 1)
        store.add(b"set1".to_vec(), b"b".to_vec());
        assert_eq!(store.ref_count(), 1); // No copy happened

        // Another write
        store.add(b"set1".to_vec(), b"c".to_vec());
        assert_eq!(store.ref_count(), 1); // Still no copy
    }

    #[test]
    fn test_set_store_merge_applies_only_changes() {
        let mut store = SetStoreCow::new();
        store.add(b"set1".to_vec(), b"a".to_vec());
        store.add(b"set1".to_vec(), b"b".to_vec());
        store.add(b"set1".to_vec(), b"c".to_vec());

        // Create snapshot
        let snapshot = store.make_snapshot();
        assert_eq!(store.ref_count(), 2);

        // Make changes (only 2 operations, not full copy)
        store.add(b"set1".to_vec(), b"d".to_vec()); // Add new
        store.remove(b"set1", b"b"); // Remove existing

        // Before merge: changes are in COW cache
        assert!(store.is_in_cow_mode());
        assert_eq!(store.len(b"set1"), Some(3)); // a, c, d
        assert!(store.contains(b"set1", b"a"));
        assert!(!store.contains(b"set1", b"b"));
        assert!(store.contains(b"set1", b"c"));
        assert!(store.contains(b"set1", b"d"));

        // Snapshot still has old data
        let snapshot_data = snapshot.read();
        let base_set = snapshot_data.get(b"set1".as_slice()).unwrap();
        assert_eq!(base_set.len(), 3);
        assert!(base_set.contains(b"a"));
        assert!(base_set.contains(b"b"));
        drop(snapshot_data);

        // Merge: applies only changes to base (O(M) where M=2, not O(N))
        store.merge_cow();
        assert!(!store.is_in_cow_mode());
        // Ref count is still 2 because snapshot still exists (this is correct)
        assert_eq!(store.ref_count(), 2);

        // After merge: changes are in base
        assert_eq!(store.len(b"set1"), Some(3));
        assert!(store.contains(b"set1", b"a"));
        assert!(!store.contains(b"set1", b"b"));
        assert!(store.contains(b"set1", b"c"));
        assert!(store.contains(b"set1", b"d"));
    }

    #[test]
    fn test_set_store_multiple_keys() {
        let mut store = SetStoreCow::new();
        store.add(b"set1".to_vec(), b"a".to_vec());
        store.add(b"set2".to_vec(), b"x".to_vec());
        store.add(b"set2".to_vec(), b"y".to_vec());

        assert_eq!(store.key_count(), 2);
        assert_eq!(store.len(b"set1"), Some(1));
        assert_eq!(store.len(b"set2"), Some(2));

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Modify only set1
        store.add(b"set1".to_vec(), b"b".to_vec());

        // set1 should be updated
        assert_eq!(store.len(b"set1"), Some(2));
        // set2 should be unchanged (from base)
        assert_eq!(store.len(b"set2"), Some(2));
    }

    #[test]
    fn test_set_store_updated_overrides_removed() {
        let mut store = SetStoreCow::new();
        store.add(b"set1".to_vec(), b"a".to_vec());
        store.add(b"set1".to_vec(), b"b".to_vec());

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Remove set1 (adds to removed cache)
        assert!(store.clear(b"set1"));
        assert!(!store.contains_key(b"set1"));
        assert_eq!(store.len(b"set1"), None);

        // Re-add set1 (adds to updated cache, should override removed)
        store.add(b"set1".to_vec(), b"c".to_vec());
        assert!(store.contains_key(b"set1")); // Should exist (updated overrides removed)
        assert_eq!(store.len(b"set1"), Some(1));
        assert!(store.contains(b"set1", b"c"));

        // Merge: updated should take precedence
        store.merge_cow();
        assert!(store.contains_key(b"set1")); // Should still exist after merge
        assert_eq!(store.len(b"set1"), Some(1));
        assert!(store.contains(b"set1", b"c"));
    }

    #[test]
    fn test_set_store_merge_updated_overrides_removed() {
        let mut store = SetStoreCow::new();
        store.add(b"set1".to_vec(), b"a".to_vec());
        store.add(b"set2".to_vec(), b"x".to_vec());

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Remove set1
        assert!(store.clear(b"set1"));
        assert!(!store.contains_key(b"set1"));

        // Re-add set1 (should override removed)
        store.add(b"set1".to_vec(), b"b".to_vec());
        assert!(store.contains_key(b"set1"));

        // Merge: updated should take precedence over removed
        store.merge_cow();
        assert!(store.contains_key(b"set1")); // Should exist (updated overrides removed)
        assert_eq!(store.len(b"set1"), Some(1));
        assert!(store.contains(b"set1", b"b"));

        // set2 should still exist
        assert!(store.contains_key(b"set2"));
        assert_eq!(store.len(b"set2"), Some(1));
    }

    #[test]
    fn test_set_store_keys() {
        let mut store = SetStoreCow::new();
        store.add(b"set1".to_vec(), b"a".to_vec());
        store.add(b"set2".to_vec(), b"x".to_vec());
        store.add(b"set3".to_vec(), b"y".to_vec());

        let keys = store.keys();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&b"set1".to_vec()));
        assert!(keys.contains(&b"set2".to_vec()));
        assert!(keys.contains(&b"set3".to_vec()));

        // Create snapshot and remove set2
        let _snapshot = store.make_snapshot();
        assert!(store.clear(b"set2"));

        // Keys should exclude removed set2
        let keys = store.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&b"set1".to_vec()));
        assert!(!keys.contains(&b"set2".to_vec()));
        assert!(keys.contains(&b"set3".to_vec()));
    }

    #[test]
    fn test_set_store_clear() {
        let mut store = SetStoreCow::new();
        store.add(b"set1".to_vec(), b"a".to_vec());
        store.add(b"set1".to_vec(), b"b".to_vec());

        // Clear set1
        assert!(store.clear(b"set1"));
        assert!(!store.contains_key(b"set1"));
        assert_eq!(store.len(b"set1"), None);
        assert!(!store.contains(b"set1", b"a"));

        // Clear non-existent set
        assert!(!store.clear(b"nonexistent"));
    }

    #[test]
    fn test_set_store_from_data() {
        let mut data = HashMap::new();
        let mut set1 = SetDataCow::new();
        set1.add(b"a".to_vec());
        set1.add(b"b".to_vec());
        data.insert(b"set1".to_vec(), set1);

        let store = SetStoreCow::from_data(data);
        assert_eq!(store.key_count(), 1);
        assert_eq!(store.len(b"set1"), Some(2));
        assert!(store.contains(b"set1", b"a"));
        assert!(store.contains(b"set1", b"b"));
    }

    #[test]
    fn test_set_store_multiple_writes_same_set() {
        let mut store = SetStoreCow::new();
        store.add(b"set1".to_vec(), b"a".to_vec());

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Multiple writes to same set (should reuse COW instance)
        store.add(b"set1".to_vec(), b"b".to_vec());
        store.add(b"set1".to_vec(), b"c".to_vec());
        store.add(b"set1".to_vec(), b"a".to_vec()); // Duplicate (should return false)

        assert_eq!(store.len(b"set1"), Some(3));
        assert!(store.contains(b"set1", b"a"));
        assert!(store.contains(b"set1", b"b"));
        assert!(store.contains(b"set1", b"c"));

        // Ref count should still be 2 (no full copy!)
        assert_eq!(store.ref_count(), 2);
    }
}
