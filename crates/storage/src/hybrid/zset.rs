//! ZSet (Sorted Set) Data Structure
//!
//! Independent ZSet storage implementation.
//! This module provides the core data structure for sorted sets,
//! without implementing Redis API traits.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

/// ZSet internal data structure
///
/// A sorted set maintains:
/// - member -> score mapping (for O(1) score lookup)
/// - score -> members mapping (for range queries)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ZSetData {
    /// member -> score mapping
    pub scores: HashMap<Vec<u8>, f64>,
    /// score -> members mapping (for range queries)
    pub by_score: BTreeMap<OrderedFloat, HashSet<Vec<u8>>>,
}

impl ZSetData {
    /// Create a new empty ZSet
    pub fn new() -> Self {
        Self::default()
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
    ///
    /// Reverse rank means: rank from highest score to lowest.
    /// Member with highest score has rev_rank 0.
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.scores.get(member)?;
        let score_key = OrderedFloat(*score);

        // Count members with higher scores
        let mut higher_count = 0;
        for (key, members) in self.by_score.iter().rev() {
            if *key > score_key {
                higher_count += members.len();
            } else if *key == score_key {
                // Count members with same score that come before this member
                // Since HashSet is unordered, we use lexicographic order as tie-breaker
                let mut same_score_before = 0;
                for m in members {
                    if m.as_slice() < member {
                        same_score_before += 1;
                    }
                }
                // Reverse rank = higher_count + same_score_before
                return Some(higher_count + same_score_before);
            }
        }

        None
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
}

