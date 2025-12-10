//! ZSet (Sorted Set) Data Structure with Copy-on-Write (COW)
//!
//! Independent ZSet storage implementation with true Copy-on-Write semantics.
//! - Snapshot: Only increases reference count, no data copy
//! - Write: Only copies data when snapshot exists (reference count > 1)
//! - Read: Direct access, no overhead
//!
//! This module provides the core data structure for sorted sets,
//! without implementing Redis API traits.

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

/// ZSet with Copy-on-Write (COW) support
///
/// True COW semantics:
/// - `make_snapshot()`: Only clones Arc (increases ref count), no data copy
/// - `add()`/`remove()`: Uses `Arc::make_mut()` to copy only when needed
/// - Read operations: Direct access, zero overhead
#[derive(Debug, Clone)]
pub struct ZSetDataCow {
    /// Shared data (Arc for COW)
    /// When reference count > 1, write operations will copy the data
    data: Arc<ZSetData>,
}

impl ZSetDataCow {
    /// Create a new empty ZSet with COW support
    pub fn new() -> Self {
        Self {
            data: Arc::new(ZSetData::new()),
        }
    }

    /// Create from existing ZSetData
    pub fn from_data(data: ZSetData) -> Self {
        Self {
            data: Arc::new(data),
        }
    }

    /// Create a snapshot (only increases reference count, no data copy)
    ///
    /// Returns a cloned Arc that shares the same data.
    /// The data will only be copied when a write operation occurs.
    pub fn make_snapshot(&self) -> Arc<ZSetData> {
        // ✅ Only clone Arc (cheap), no data copy
        Arc::clone(&self.data)
    }

    /// Get reference to inner data (for read operations)
    pub fn as_ref(&self) -> &ZSetData {
        &self.data
    }

    /// Add or update a member with score (COW: copies only if needed)
    pub fn add(&mut self, member: Vec<u8>, score: f64) {
        // ✅ Arc::make_mut() copies data only if reference count > 1
        // If no snapshot exists (ref count == 1), no copy happens
        Arc::make_mut(&mut self.data).add(member, score);
    }

    /// Remove a member (COW: copies only if needed)
    pub fn remove(&mut self, member: &[u8]) -> bool {
        Arc::make_mut(&mut self.data).remove(member)
    }

    /// Get score for a member (read operation, no copy)
    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        self.data.get_score(member)
    }

    /// Check if member exists (read operation, no copy)
    pub fn contains(&self, member: &[u8]) -> bool {
        self.data.contains(member)
    }

    /// Get member count (read operation, no copy)
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if empty (read operation, no copy)
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clear all data (COW: copies only if needed)
    pub fn clear(&mut self) {
        Arc::make_mut(&mut self.data).clear();
    }

    /// Get all members (read operation, no copy)
    pub fn members(&self) -> Vec<Vec<u8>> {
        self.data.members()
    }

    /// Get members in score range (read operation, no copy)
    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<(Vec<u8>, f64)> {
        self.data.range_by_score(min, max)
    }

    /// Get members in rank range (read operation, no copy)
    pub fn range_by_rank(&self, start: usize, end: usize) -> Vec<(Vec<u8>, f64)> {
        self.data.range_by_rank(start, end)
    }

    /// Get rank of a member (read operation, no copy)
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        self.data.rank(member)
    }

    /// Get reverse rank of a member (read operation, no copy)
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        self.data.rev_rank(member)
    }

    /// Get reference count (for debugging)
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.data)
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

        // Snapshot should have same data
        assert_eq!(snapshot.len(), 3);
        assert_eq!(snapshot.get_score(b"a"), Some(10.0));
        assert_eq!(snapshot.get_score(b"b"), Some(20.0));
        assert_eq!(snapshot.get_score(b"c"), Some(30.0));

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

        // Write operation should copy data (ref count back to 1)
        cow.add(b"c".to_vec(), 30.0);
        assert_eq!(cow.ref_count(), 1); // Data was copied
        assert_eq!(Arc::strong_count(&snapshot), 1); // Snapshot still has old data

        // Original should have new data
        assert_eq!(cow.len(), 3);
        assert_eq!(cow.get_score(b"c"), Some(30.0));

        // Snapshot should have old data (unchanged)
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get_score(b"c"), None);
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

        // Write should copy (cow gets new Arc, ref count back to 1)
        cow.add(b"b".to_vec(), 20.0);
        assert_eq!(cow.ref_count(), 1); // New Arc for cow
        
        // Snapshots still share the old Arc (ref count = 2)
        assert_eq!(Arc::strong_count(&snapshot1), 2); // snapshot1 + snapshot2
        assert_eq!(Arc::strong_count(&snapshot2), 2); // snapshot1 + snapshot2

        // All snapshots should have old data
        assert_eq!(snapshot1.len(), 1);
        assert_eq!(snapshot2.len(), 1);

        // Original should have new data
        assert_eq!(cow.len(), 2);
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
}
