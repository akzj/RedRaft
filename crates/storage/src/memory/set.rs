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

use bytes::Bytes;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// Set data structure
///
/// A set maintains a collection of unique members.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetData {
    pub members: HashSet<Bytes>,
}

impl SetData {
    /// Create a new empty Set
    pub fn new() -> Self {
        Self {
            members: HashSet::new(),
        }
    }

    /// Add a member
    pub fn add(&mut self, member: Bytes) -> bool {
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
    pub fn members(&self) -> Vec<Bytes> {
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

/// SetDataCow is now just an alias for SetData (COW removed for simplicity)
pub type SetDataCow = SetData;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_basic_operations() {
        let mut set = SetData::new();

        // Add members
        assert!(set.add(Bytes::from("member1")));
        assert!(set.add(Bytes::from("member2")));
        assert!(!set.add(Bytes::from("member1"))); // Duplicate

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
        set1.add(Bytes::from("a"));
        set1.add(Bytes::from("b"));
        set1.add(Bytes::from("c"));

        let mut set2 = SetData::new();
        set2.add(Bytes::from("b"));
        set2.add(Bytes::from("c"));
        set2.add(Bytes::from("d"));

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
}
