//! ZSet (Sorted Set) Data Structure with Incremental Copy-on-Write (COW)
//!
//! True incremental COW semantics - only changed items are recorded, NOT full data copy:
//! - Snapshot: Only clones Arc (increases ref count), NO data copy
//! - Write: Records changes in small COW cache (only changed items), NO full copy
//! - Read: Merges COW cache + base data (O(1) lookup)
//! - Merge: Applies only changed items to base (O(M) where M = changes, not total data)
//!
//! Example: 1000 billion items, modify 3 items
//! - Old approach (Arc::make_mut): Copies all 1000 billion items ❌
//! - This approach: Only records 3 changes in small HashMap ✅
//!
//! This module provides the core data structure for sorted sets,
//! without implementing Redis API traits.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

/// ZSet data structure
///
/// A sorted set maintains:
/// - member -> score mapping (for O(1) score lookup)
/// - score -> members mapping (for range queries)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZSetData {
    pub scores: HashMap<Vec<u8>, f64>,
    pub by_score: BTreeMap<OrderedFloat, HashSet<Vec<u8>>>,
}

impl ZSetData {
    /// Create a new empty ZSet
    pub fn new() -> Self {
        Self {
            scores: HashMap::new(),
            by_score: BTreeMap::new(),
        }
    }

    /// Add or update a member with score
    pub fn add(&mut self, member: Vec<u8>, score: f64) {
        // Remove old score if member exists
        if let Some(old_score) = self.scores.remove(&member) {
            let old_key = OrderedFloat(old_score);
            if let Some(members) = self.by_score.get_mut(&old_key) {
                members.remove(&member);
                if members.is_empty() {
                    self.by_score.remove(&old_key);
                }
            }
        }

        // Add new score
        self.scores.insert(member.clone(), score);
        self.by_score
            .entry(OrderedFloat(score))
            .or_default()
            .insert(member);
    }

    /// Remove a member
    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some(score) = self.scores.remove(member) {
            let key = OrderedFloat(score);
            if let Some(members) = self.by_score.get_mut(&key) {
                members.remove(member);
                if members.is_empty() {
                    self.by_score.remove(&key);
                }
            }
            true
        } else {
            false
        }
    }

    /// Get score for a member
    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        self.scores.get(member).copied()
    }

    /// Check if member exists
    pub fn contains(&self, member: &[u8]) -> bool {
        self.scores.contains_key(member)
    }

    /// Get member count
    pub fn len(&self) -> usize {
        self.scores.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.scores.clear();
        self.by_score.clear();
    }

    /// Get all members
    pub fn members(&self) -> Vec<Vec<u8>> {
        self.scores.keys().cloned().collect()
    }

    /// Get members in score range [min, max] (inclusive)
    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<(Vec<u8>, f64)> {
        let mut result = Vec::new();
        let min_key = OrderedFloat(min);
        let max_key = OrderedFloat(max);

        for (score_key, members) in self.by_score.range(min_key..=max_key) {
            let score = score_key.0;
            for member in members {
                result.push((member.clone(), score));
            }
        }

        result
    }

    /// Get members in rank range [start, end] (0-based)
    pub fn range_by_rank(&self, start: usize, end: usize) -> Vec<(Vec<u8>, f64)> {
        let mut result = Vec::new();
        let mut rank = 0;

        for (score_key, members) in self.by_score.iter() {
            let score = score_key.0;
            for member in members {
                if rank >= start && rank <= end {
                    result.push((member.clone(), score));
                }
                rank += 1;
                if rank > end {
                    return result;
                }
            }
        }

        result
    }

    /// Get rank of a member (0-based, returns None if not found)
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.scores.get(member)?;
        let score_key = OrderedFloat(*score);

        let mut rank = 0;
        for (key, members) in self.by_score.iter() {
            if *key < score_key {
                rank += members.len();
            } else if *key == score_key {
                for m in members {
                    if m == member {
                        return Some(rank);
                    }
                    rank += 1;
                }
            }
        }

        None
    }

    /// Get reverse rank of a member (0-based, returns None if not found)
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.scores.get(member)?;
        let score_key = OrderedFloat(*score);

        let mut higher_count = 0;
        for (key, members) in self.by_score.iter().rev() {
            if *key > score_key {
                higher_count += members.len();
            } else if *key == score_key {
                let mut same_score_before = 0;
                for m in members {
                    if m.as_slice() < member {
                        same_score_before += 1;
                    }
                }
                return Some(higher_count + same_score_before);
            }
        }

        None
    }
}

impl Default for ZSetData {
    fn default() -> Self {
        Self::new()
    }
}

/// ZSet with Incremental Copy-on-Write (COW) support
///
/// True incremental COW semantics:
/// - `make_snapshot()`: Only clones Arc (increases ref count), NO data copy
/// - `add()`/`remove()`: Records changes in small COW cache (only changed items), NO full copy
/// - Read operations: Merges COW cache + base data (O(1) lookup)
/// - `merge_cow()`: Applies only changed items to base via RwLock (O(M) where M = changes, not total data)
///
/// This avoids full data copy even for 1000 billion items when only 3 items change.
#[derive(Debug, Clone)]
pub struct ZSetDataCow {
    /// Base data (shared via Arc<RwLock<>>, can be directly modified without clone)
    base: Arc<RwLock<ZSetData>>,

    /// COW cache: Updated/added members (only changed items)
    scores_updated: Option<HashMap<Vec<u8>, f64>>,

    /// COW cache: Removed members with their old scores (for cleanup)
    scores_removed: Option<HashMap<Vec<u8>, f64>>,
}

impl ZSetDataCow {
    /// Create a new empty ZSet with COW support
    pub fn new() -> Self {
        Self {
            base: Arc::new(RwLock::new(ZSetData::new())),
            scores_updated: None,
            scores_removed: None,
        }
    }

    /// Create from existing ZSetData
    pub fn from_data(data: ZSetData) -> Self {
        Self {
            base: Arc::new(RwLock::new(data)),
            scores_updated: None,
            scores_removed: None,
        }
    }

    /// Check if in COW mode (has snapshot)
    fn is_cow_mode(&self) -> bool {
        self.scores_updated.is_some()
    }

    /// Create a snapshot (only increases reference count, NO data copy)
    ///
    /// Returns a cloned Arc that shares the same base data.
    /// Write operations will record changes in COW cache instead of copying data.
    pub fn make_snapshot(&mut self) -> Arc<RwLock<ZSetData>> {
        if self.is_cow_mode() {
            // Already in COW mode, just return existing base
            return Arc::clone(&self.base);
        }

        // Enter COW mode: initialize caches
        self.scores_updated = Some(HashMap::new());
        self.scores_removed = Some(HashMap::new());

        // ✅ Only clone Arc (O(1)), NO data copy
        Arc::clone(&self.base)
    }
    /// Create a snapshot (only increases reference count, NO data copy)
    ///
    /// Returns a cloned Arc that shares the same base data.
    /// Write operations will record changes in COW cache instead of copying data.
    pub fn make_cow(&mut self) -> ZSetDataCow {
        ZSetDataCow {
            base: Arc::clone(&self.base),
            scores_updated: Some(HashMap::new()),
            scores_removed: Some(HashMap::new()),
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

        let updated = self.scores_updated.take();
        let removed = self.scores_removed.take();

        // ✅ Get write lock and directly modify base (NO clone!)
        let mut base = self.base.write();

        // Apply removals (only if not in updated - updated takes precedence)
        if let Some(ref removed) = removed {
            for member in removed.keys() {
                // Only remove if not being updated
                if updated.as_ref().map_or(true, |u| !u.contains_key(member)) {
                    base.remove(member);
                }
            }
        }

        // Apply updates/additions (this handles both new and updated members)
        if let Some(ref updated) = updated {
            for (member, score) in updated {
                base.add(member.clone(), *score);
            }
        }

        // Write lock is released here, no need to replace base
    }

    /// Add or update a member with score (incremental COW: only records change)
    pub fn add(&mut self, member: Vec<u8>, score: f64) {
        if self.is_cow_mode() {
            // COW mode: record change in cache, don't modify base
            let updated = self.scores_updated.as_mut().unwrap();
            let removed = self.scores_removed.as_mut().unwrap();

            // Check if member exists in base (read lock)
            let old_score_opt = {
                let base = self.base.read();
                base.scores.get(&member).copied()
            }; // Read lock released here

            if let Some(old_score) = old_score_opt {
                // Member exists in base, record old score for removal tracking
                // (so reads know the old value is gone)
                removed.insert(member.clone(), old_score);
            }
            // If member was previously removed, we're re-adding it, so remove from removed cache
            else if removed.contains_key(&member) {
                removed.remove(&member);
            }

            // Record update (overwrites if already in updated)
            updated.insert(member, score);
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.add(member, score);
        }
    }

    /// Remove a member (incremental COW: only records change)
    pub fn remove(&mut self, member: &[u8]) -> bool {
        if self.is_cow_mode() {
            // COW mode: record change in cache
            let updated = self.scores_updated.as_mut().unwrap();
            let removed = self.scores_removed.as_mut().unwrap();

            // Check if member exists in base (read lock)
            let score_opt = {
                let base = self.base.read();
                base.scores.get(member).copied()
            }; // Read lock released here

            if let Some(score) = score_opt {
                // Remove from updated cache if present
                updated.remove(member);
                // Record removal
                removed.insert(member.to_vec(), score);
                true
            } else if updated.contains_key(member) {
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

    /// Get score for a member (merges COW cache + base)
    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.scores_updated {
                if let Some(&score) = updated.get(member) {
                    return Some(score);
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.scores_removed {
                if removed.contains_key(member) {
                    return None;
                }
            }
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get_score(member)
    }

    /// Check if member exists (merges COW cache + base)
    pub fn contains(&self, member: &[u8]) -> bool {
        self.get_score(member).is_some()
    }

    /// Get member count (approximate, includes COW changes)
    pub fn len(&self) -> usize {
        let base = self.base.read();
        let base_len = base.len();
        drop(base);

        if self.is_cow_mode() {
            let updated = self.scores_updated.as_ref().unwrap();
            let removed = self.scores_removed.as_ref().unwrap();
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
            self.scores_updated = Some(HashMap::new());
            let base = self.base.read();
            let mut removed = HashMap::new();
            for (member, score) in &base.scores {
                removed.insert(member.clone(), *score);
            }
            drop(base);
            self.scores_removed = Some(removed);
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
        if let Some(ref removed) = self.scores_removed {
            for member in base.scores.keys() {
                if !removed.contains_key(member) {
                    members.insert(member.clone());
                }
            }
        } else {
            for member in base.scores.keys() {
                members.insert(member.clone());
            }
        }
        drop(base);

        // Add updated/added members
        if let Some(ref updated) = self.scores_updated {
            for member in updated.keys() {
                members.insert(member.clone());
            }
        }

        members.into_iter().collect()
    }

    /// Get members in score range (merges COW cache + base)
    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<(Vec<u8>, f64)> {
        let mut result = Vec::new();
        let removed = self.scores_removed.as_ref();

        // Add from base (excluding removed) - read lock
        let base = self.base.read();
        for (member, score) in base.range_by_score(min, max) {
            if let Some(ref removed) = removed {
                if !removed.contains_key(&member) {
                    result.push((member, score));
                }
            } else {
                result.push((member, score));
            }
        }
        drop(base);

        // Add from COW cache
        if let Some(ref updated) = self.scores_updated {
            for (member, score) in updated {
                if *score >= min && *score <= max {
                    result.push((member.clone(), *score));
                }
            }
        }

        // Sort by score (for consistency)
        result.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        result
    }

    /// Get members in rank range (simplified, may not be exact due to COW)
    pub fn range_by_rank(&self, start: usize, end: usize) -> Vec<(Vec<u8>, f64)> {
        // Get all members and sort
        let mut all = self.range_by_score(f64::NEG_INFINITY, f64::INFINITY);
        all.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        if start >= all.len() {
            return Vec::new();
        }

        let end = end.min(all.len() - 1);
        all[start..=end].to_vec()
    }

    /// Get rank of a member
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.get_score(member)?;
        let all = self.range_by_score(f64::NEG_INFINITY, f64::INFINITY);
        let mut sorted = all;
        sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        sorted.iter().position(|(m, _)| m == member)
    }

    /// Get reverse rank of a member
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.get_score(member)?;
        let all = self.range_by_score(f64::NEG_INFINITY, f64::INFINITY);
        let mut sorted = all;
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        sorted.iter().position(|(m, _)| m == member)
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

impl Default for ZSetDataCow {
    fn default() -> Self {
        Self::new()
    }
}

/// Ordered float for BTreeMap key
///
/// Wraps f64 to make it usable as a BTreeMap key.
/// Handles NaN and infinity by treating them as equal.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OrderedFloat(pub f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        // Handle NaN: all NaNs are considered equal
        if self.0.is_nan() && other.0.is_nan() {
            return true;
        }
        self.0 == other.0
    }
}

impl Eq for OrderedFloat {}

impl std::hash::Hash for OrderedFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash NaN as a special value
        if self.0.is_nan() {
            state.write_u64(0x7ff8000000000000u64); // NaN bit pattern
        } else {
            self.0.to_bits().hash(state);
        }
    }
}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Handle NaN and infinity
        if self.0.is_nan() && other.0.is_nan() {
            return std::cmp::Ordering::Equal;
        }
        if self.0.is_nan() {
            return std::cmp::Ordering::Less;
        }
        if other.0.is_nan() {
            return std::cmp::Ordering::Greater;
        }

        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zset_basic_operations() {
        let mut zset = ZSetData::new();

        // Add members
        zset.add(b"member1".to_vec(), 10.0);
        zset.add(b"member2".to_vec(), 20.0);
        zset.add(b"member3".to_vec(), 15.0);

        assert_eq!(zset.len(), 3);
        assert!(zset.contains(b"member1"));
        assert_eq!(zset.get_score(b"member1"), Some(10.0));

        // Update score
        zset.add(b"member1".to_vec(), 25.0);
        assert_eq!(zset.get_score(b"member1"), Some(25.0));

        // Remove member
        assert!(zset.remove(b"member2"));
        assert!(!zset.contains(b"member2"));
        assert_eq!(zset.len(), 2);
    }

    #[test]
    fn test_zset_range_queries() {
        let mut zset = ZSetData::new();
        zset.add(b"a".to_vec(), 10.0);
        zset.add(b"b".to_vec(), 20.0);
        zset.add(b"c".to_vec(), 30.0);
        zset.add(b"d".to_vec(), 40.0);

        // Range by score
        let result = zset.range_by_score(15.0, 35.0);
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|(m, _)| m == b"b"));
        assert!(result.iter().any(|(m, _)| m == b"c"));

        // Range by rank
        let result = zset.range_by_rank(1, 2);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_ordered_float() {
        let mut map = BTreeMap::new();
        map.insert(OrderedFloat(10.0), "a");
        map.insert(OrderedFloat(20.0), "b");
        map.insert(OrderedFloat(15.0), "c");

        let keys: Vec<f64> = map.keys().map(|k| k.0).collect();
        assert_eq!(keys, vec![10.0, 15.0, 20.0]);
    }

    #[test]
    fn test_cow_basic_operations() {
        let mut cow = ZSetDataCow::new();

        // Add members
        cow.add(b"member1".to_vec(), 10.0);
        cow.add(b"member2".to_vec(), 20.0);
        cow.add(b"member3".to_vec(), 15.0);

        assert_eq!(cow.len(), 3);
        assert!(cow.contains(b"member1"));
        assert_eq!(cow.get_score(b"member1"), Some(10.0));

        // Update score
        cow.add(b"member1".to_vec(), 25.0);
        assert_eq!(cow.get_score(b"member1"), Some(25.0));

        // Remove member
        assert!(cow.remove(b"member2"));
        assert!(!cow.contains(b"member2"));
        assert_eq!(cow.len(), 2);
    }

    #[test]
    fn test_cow_snapshot_no_copy() {
        let mut cow = ZSetDataCow::new();
        cow.add(b"a".to_vec(), 10.0);
        cow.add(b"b".to_vec(), 20.0);
        cow.add(b"c".to_vec(), 30.0);

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
        assert_eq!(snapshot_data.get_score(b"a"), Some(10.0));
        assert_eq!(snapshot_data.get_score(b"b"), Some(20.0));
        assert_eq!(snapshot_data.get_score(b"c"), Some(30.0));
        drop(snapshot_data);

        // Original should still work
        assert_eq!(cow.len(), 3);
        assert_eq!(cow.get_score(b"a"), Some(10.0));
    }

    #[test]
    fn test_cow_write_after_snapshot_copies() {
        let mut cow = ZSetDataCow::new();
        cow.add(b"a".to_vec(), 10.0);
        cow.add(b"b".to_vec(), 20.0);

        // Create snapshot
        let snapshot = cow.make_snapshot();
        assert_eq!(cow.ref_count(), 2);
        assert!(cow.is_in_cow_mode()); // Should be in COW mode

        // Write operation records change in COW cache (NO data copy)
        cow.add(b"c".to_vec(), 30.0);
        assert_eq!(cow.ref_count(), 2); // Ref count unchanged (no copy!)
        assert_eq!(Arc::strong_count(&snapshot), 2); // Snapshot still shares base
        assert!(cow.is_in_cow_mode()); // Still in COW mode

        // Original should have new data (via COW cache)
        assert_eq!(cow.len(), 3);
        assert_eq!(cow.get_score(b"c"), Some(30.0));

        // Snapshot should have old data (unchanged, from base)
        let snapshot_data = snapshot.read();
        assert_eq!(snapshot_data.len(), 2);
        assert_eq!(snapshot_data.get_score(b"c"), None);
        drop(snapshot_data);
    }

    #[test]
    fn test_cow_write_without_snapshot_no_copy() {
        let mut cow = ZSetDataCow::new();
        cow.add(b"a".to_vec(), 10.0);

        // No snapshot, ref count is 1
        assert_eq!(cow.ref_count(), 1);

        // Write operation should NOT copy (ref count stays 1)
        cow.add(b"b".to_vec(), 20.0);
        assert_eq!(cow.ref_count(), 1); // No copy happened

        // Another write
        cow.add(b"c".to_vec(), 30.0);
        assert_eq!(cow.ref_count(), 1); // Still no copy
    }

    #[test]
    fn test_cow_multiple_snapshots() {
        let mut cow = ZSetDataCow::new();
        cow.add(b"a".to_vec(), 10.0);

        // Create multiple snapshots (they share the same Arc)
        let snapshot1 = cow.make_snapshot();
        let snapshot2 = cow.make_snapshot();
        assert_eq!(cow.ref_count(), 3); // cow + snapshot1 + snapshot2
        assert!(cow.is_in_cow_mode()); // Should be in COW mode

        // Write records change in COW cache (NO data copy)
        cow.add(b"b".to_vec(), 20.0);
        assert_eq!(cow.ref_count(), 3); // Ref count unchanged (no copy!)
        assert!(cow.is_in_cow_mode()); // Still in COW mode

        // Snapshots still share the same base Arc (ref count = 3)
        assert_eq!(Arc::strong_count(&snapshot1), 3); // cow + snapshot1 + snapshot2
        assert_eq!(Arc::strong_count(&snapshot2), 3); // cow + snapshot1 + snapshot2

        // All snapshots should have old data (from base)
        let snapshot1_data = snapshot1.read();
        let snapshot2_data = snapshot2.read();
        assert_eq!(snapshot1_data.len(), 1);
        assert_eq!(snapshot2_data.len(), 1);
        drop(snapshot1_data);
        drop(snapshot2_data);

        // Original should have new data (via COW cache)
        assert_eq!(cow.len(), 2);
        assert_eq!(cow.get_score(b"b"), Some(20.0));
    }

    #[test]
    fn test_cow_read_operations_no_overhead() {
        let mut cow = ZSetDataCow::new();
        cow.add(b"a".to_vec(), 10.0);
        cow.add(b"b".to_vec(), 20.0);

        // Create snapshot
        let _snapshot = cow.make_snapshot();

        // Read operations should work without copying
        assert_eq!(cow.get_score(b"a"), Some(10.0));
        assert_eq!(cow.get_score(b"b"), Some(20.0));
        assert!(cow.contains(b"a"));
        assert_eq!(cow.len(), 2);

        // Ref count should still be 2 (no copy happened)
        assert_eq!(cow.ref_count(), 2);
    }

    #[test]
    fn test_cow_merge_applies_only_changes() {
        let mut cow = ZSetDataCow::new();
        cow.add(b"a".to_vec(), 10.0);
        cow.add(b"b".to_vec(), 20.0);
        cow.add(b"c".to_vec(), 30.0);

        // Create snapshot
        let snapshot = cow.make_snapshot();
        assert_eq!(cow.ref_count(), 2);

        // Make changes (only 3 changes, not full copy)
        cow.add(b"d".to_vec(), 40.0); // Add new
        cow.add(b"a".to_vec(), 15.0); // Update existing
        cow.remove(b"b"); // Remove existing

        // Before merge: changes are in COW cache
        assert!(cow.is_in_cow_mode());
        assert_eq!(cow.len(), 3); // a(15), c(30), d(40)
        assert_eq!(cow.get_score(b"a"), Some(15.0));
        assert_eq!(cow.get_score(b"b"), None);
        assert_eq!(cow.get_score(b"d"), Some(40.0));

        // Snapshot still has old data
        let snapshot_data = snapshot.read();
        assert_eq!(snapshot_data.len(), 3);
        assert_eq!(snapshot_data.get_score(b"a"), Some(10.0));
        assert_eq!(snapshot_data.get_score(b"b"), Some(20.0));
        drop(snapshot_data);

        // Merge: applies only changes to base (O(M) where M=3, not O(N))
        cow.merge_cow();
        assert!(!cow.is_in_cow_mode());
        // Ref count is still 2 because snapshot still exists (this is correct)
        assert_eq!(cow.ref_count(), 2);

        // After merge: changes are in base
        assert_eq!(cow.len(), 3);
        assert_eq!(cow.get_score(b"a"), Some(15.0));
        assert_eq!(cow.get_score(b"b"), None);
        assert_eq!(cow.get_score(b"c"), Some(30.0));
        assert_eq!(cow.get_score(b"d"), Some(40.0));
    }
}

/// ZSet Store with Incremental Copy-on-Write (COW) support
///
/// True incremental COW semantics at HashMap level:
/// - `make_snapshot()`: Only clones Arc (increases ref count), NO data copy
/// - Write operations: Use `ZSetDataCow::make_cow()` to create COW instances (NO full copy)
/// - Read operations: Merges COW cache + base data (O(1) lookup)
/// - `merge_cow()`: Applies only changed ZSets to base (O(M) where M = changes, not total data)
///
/// Note: ZSetDataCow is created via `make_cow()` when modified (required for consistency),
/// but only for changed ZSets. This avoids full HashMap copy even for 1000 billion ZSets
/// when only 3 ZSets change.
#[derive(Debug, Clone)]
pub struct ZSetStoreCow {
    /// Base data (shared via Arc<RwLock<>>, can be directly modified without clone)
    base: Arc<RwLock<HashMap<Vec<u8>, ZSetDataCow>>>,

    /// COW cache: Updated/added ZSets (only changed ZSets)
    /// ZSetDataCow is created via make_cow() here (required for consistency)
    zsets_updated: Option<HashMap<Vec<u8>, ZSetDataCow>>,

    /// COW cache: Removed ZSet keys
    zsets_removed: Option<HashSet<Vec<u8>>>,
}

impl ZSetStoreCow {
    /// Create a new empty ZSetStore with COW support
    pub fn new() -> Self {
        Self {
            base: Arc::new(RwLock::new(HashMap::new())),
            zsets_updated: None,
            zsets_removed: None,
        }
    }

    /// Create from existing HashMap
    pub fn from_data(data: HashMap<Vec<u8>, ZSetDataCow>) -> Self {
        Self {
            base: Arc::new(RwLock::new(data)),
            zsets_updated: None,
            zsets_removed: None,
        }
    }

    /// Check if in COW mode (has snapshot)
    fn is_cow_mode(&self) -> bool {
        self.zsets_updated.is_some()
    }

    /// Create a snapshot (only increases reference count, NO data copy)
    ///
    /// Returns a cloned Arc that shares the same base data.
    /// Write operations will use `make_cow()` to create COW instances instead of copying data.
    pub fn make_snapshot(&mut self) -> Arc<RwLock<HashMap<Vec<u8>, ZSetDataCow>>> {
        if self.is_cow_mode() {
            // Already in COW mode, just return existing base
            return Arc::clone(&self.base);
        }

        // Enter COW mode: initialize caches
        self.zsets_updated = Some(HashMap::new());
        self.zsets_removed = Some(HashSet::new());

        // ✅ Only clone Arc (O(1)), NO data copy
        Arc::clone(&self.base)
    }

    /// Merge COW changes back to base (applies only changed ZSets)
    ///
    /// This is called when snapshot is no longer needed.
    /// Only changed ZSets are applied via RwLock, not full data copy.
    pub fn merge_cow(&mut self) {
        if !self.is_cow_mode() {
            return;
        }

        let updated = self.zsets_updated.take();
        let removed = self.zsets_removed.take();

        // Collect updated keys before processing (for removal check)
        let updated_keys: HashSet<Vec<u8>> = updated
            .as_ref()
            .map(|u| u.keys().cloned().collect())
            .unwrap_or_default();

        // ✅ Get write lock and directly modify base (NO clone!)
        let mut base = self.base.write();

        // Apply updates/additions first (updated takes precedence over removed)
        // This handles both new and updated ZSets
        if let Some(updated) = updated {
            for (key, mut zset_cow) in updated {
                // Merge the ZSetDataCow's changes to its base first
                if zset_cow.is_in_cow_mode() {
                    zset_cow.merge_cow();
                }
                
                // Now zset_cow's base has the merged data
                // We need to extract the data from zset_cow's base and create a new ZSetDataCow
                // Since we can't directly access the internal data, we'll replace the base entry
                // with a new ZSetDataCow that shares the same base Arc
                let merged_zset = {
                    // Clone the base Arc (only increases ref count, no data copy)
                    let base_arc = Arc::clone(&zset_cow.base);
                    // Create new ZSetDataCow with the merged base
                    ZSetDataCow {
                        base: base_arc,
                        scores_updated: None,
                        scores_removed: None,
                    }
                };
                
                // Replace or insert the merged ZSetDataCow
                base.insert(key, merged_zset);
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

    /// Get ZSetDataCow for key (returns reference - for read operations)
    ///
    /// Returns a reference to the ZSetDataCow if it exists.
    /// For modifications, use `get_zset_mut()` instead.
    ///
    /// Note: This method can only return references from COW cache, not from base
    /// (due to lock lifetime constraints). For base lookups, use other methods.
    pub fn get_zset(&self, key: &[u8]) -> Option<&ZSetDataCow> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.zsets_updated {
                if let Some(zset) = updated.get(key) {
                    return Some(zset);
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.zsets_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, we can't return a reference from base
            // (due to lock lifetime), so return None
            // Callers should use other methods like get_score(), contains(), etc.
            return None;
        }

        // No COW mode: can't return reference from base (lock lifetime)
        None
    }

    /// Get mutable ZSetDataCow for key (creates COW instance if needed)
    ///
    /// This method:
    /// 1. Checks if ZSet is already in COW cache
    /// 2. If yes, returns mutable reference (no additional copy!)
    /// 3. If no, uses `make_cow()` to create COW instance (no full copy!)
    pub fn get_zset_mut(&mut self, key: &[u8]) -> Option<&mut ZSetDataCow> {
        if self.is_cow_mode() {
            let updated = self.zsets_updated.as_mut().unwrap();
            let removed = self.zsets_removed.as_mut().unwrap();

            // Check if already in COW cache (already has COW instance)
            if updated.contains_key(key) {
                // Already has COW instance: return mutable reference (no additional copy!)
                return updated.get_mut(key);
            }

            // Not in COW cache: create COW instance via make_cow() (no full copy!)
            let zset_cow = {
                // Check if removed
                if removed.contains(key) {
                    // Create new ZSetDataCow for removed key
                    ZSetDataCow::new()
                } else {
                    // Get from base and create COW instance
                    let base = self.base.read();
                    if let Some(base_zset) = base.get(key) {
                        // Clone the ZSetDataCow struct (only clones Arc, NO data copy!)
                        // Then call make_cow() to create COW instance (NO full copy!)
                        let mut base_zset_clone = base_zset.clone();
                        base_zset_clone.make_cow()
                    } else {
                        // Create new ZSetDataCow
                        ZSetDataCow::new()
                    }
                }
            };
            updated.insert(key.to_vec(), zset_cow);
            
            // Remove from removed cache if present
            removed.remove(key);
            
            // Return mutable reference to the newly inserted ZSetDataCow
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
            if let Some(ref updated) = self.zsets_updated {
                if updated.contains_key(key) {
                    return true;
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.zsets_removed {
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

    /// Add or update a member with score in a ZSet (incremental COW: only records change)
    pub fn add(&mut self, key: Vec<u8>, member: Vec<u8>, score: f64) {
        if let Some(zset) = self.get_zset_mut(&key) {
            zset.add(member, score);
        } else if !self.is_cow_mode() {
            // No snapshot and key doesn't exist: create new ZSetDataCow
            let mut base = self.base.write();
            let zset = base.entry(key).or_insert_with(|| ZSetDataCow::new());
            zset.add(member, score);
        }
    }

    /// Remove a member from a ZSet (incremental COW: only records change)
    pub fn remove(&mut self, key: &[u8], member: &[u8]) -> bool {
        if let Some(zset) = self.get_zset_mut(key) {
            zset.remove(member)
        } else if !self.is_cow_mode() {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            if let Some(zset) = base.get_mut(key) {
                zset.remove(member)
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Get score for a member in a ZSet (read operation, no copy)
    pub fn get_score(&self, key: &[u8], member: &[u8]) -> Option<f64> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.zsets_updated {
                if let Some(zset) = updated.get(key) {
                    return zset.get_score(member);
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.zsets_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key)?.get_score(member)
    }

    /// Check if member exists in a ZSet (read operation, no copy)
    pub fn contains(&self, key: &[u8], member: &[u8]) -> bool {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.zsets_updated {
                if let Some(zset) = updated.get(key) {
                    return zset.contains(member);
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.zsets_removed {
                if removed.contains(key) {
                    return false;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map_or(false, |zset| zset.contains(member))
    }

    /// Get member count for a ZSet (read operation, no copy)
    pub fn len(&self, key: &[u8]) -> Option<usize> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.zsets_updated {
                if let Some(zset) = updated.get(key) {
                    return Some(zset.len());
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.zsets_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map(|zset| zset.len())
    }

    /// Clear ZSet for key (incremental COW: only records change)
    pub fn clear(&mut self, key: &[u8]) -> bool {
        if self.is_cow_mode() {
            let updated = self.zsets_updated.as_mut().unwrap();
            let removed = self.zsets_removed.as_mut().unwrap();

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

    /// Remove ZSet for key (incremental COW: only records change)
    pub fn remove_zset(&mut self, key: &[u8]) -> bool {
        self.clear(key)
    }

    /// Get all keys (read operation)
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys = HashSet::new();

        if self.is_cow_mode() {
            // Add keys from base (excluding removed)
            let base = self.base.read();
            if let Some(ref removed) = self.zsets_removed {
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
            if let Some(ref updated) = self.zsets_updated {
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

impl Default for ZSetStoreCow {
    fn default() -> Self {
        Self::new()
    }
}
