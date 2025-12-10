//! Comprehensive tests for Set Data Structure with Incremental Copy-on-Write (COW)
//!
//! Tests cover:
//! - Basic SetData operations
//! - SetDataCow incremental COW semantics
//! - SetStoreCow operations and COW behavior
//! - Edge cases and error conditions
//! - Performance characteristics

use super::*;
use std::collections::HashSet;

// Helper functions for testing
fn create_test_member(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

fn create_test_members(values: &[&str]) -> Vec<Vec<u8>> {
    values.iter().map(|v| v.as_bytes().to_vec()).collect()
}

// ========== SetData Tests ==========

#[test]
fn test_set_data_basic_operations() {
    let mut set = SetData::new();
    
    // Test empty set
    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
    assert!(!set.contains(b"member1"));
    
    // Test add operations
    assert!(set.add(create_test_member("member1")));
    assert!(!set.add(create_test_member("member1"))); // Duplicate add
    assert!(set.add(create_test_member("member2")));
    
    assert_eq!(set.len(), 2);
    assert!(set.contains(b"member1"));
    assert!(set.contains(b"member2"));
    assert!(!set.contains(b"member3"));
    
    // Test remove operations
    assert!(set.remove(b"member1"));
    assert!(!set.remove(b"member1")); // Already removed
    assert_eq!(set.len(), 1);
    assert!(!set.contains(b"member1"));
    assert!(set.contains(b"member2"));
    
    // Test clear
    set.clear();
    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
}

#[test]
fn test_set_data_set_operations() {
    let mut set1 = SetData::new();
    let mut set2 = SetData::new();
    
    // Populate sets
    set1.add(create_test_member("a"));
    set1.add(create_test_member("b"));
    set1.add(create_test_member("c"));
    
    set2.add(create_test_member("b"));
    set2.add(create_test_member("c"));
    set2.add(create_test_member("d"));
    
    // Test intersection
    let intersection = set1.intersect(&set2);
    assert_eq!(intersection.len(), 2);
    assert!(intersection.contains(b"b"));
    assert!(intersection.contains(b"c"));
    assert!(!intersection.contains(b"a"));
    assert!(!intersection.contains(b"d"));
    
    // Test union
    let union = set1.union(&set2);
    assert_eq!(union.len(), 4);
    assert!(union.contains(b"a"));
    assert!(union.contains(b"b"));
    assert!(union.contains(b"c"));
    assert!(union.contains(b"d"));
    
    // Test difference
    let difference = set1.difference(&set2);
    assert_eq!(difference.len(), 1);
    assert!(difference.contains(b"a"));
    assert!(!difference.contains(b"b"));
    assert!(!difference.contains(b"c"));
    assert!(!difference.contains(b"d"));
}

#[test]
fn test_set_data_members() {
    let mut set = SetData::new();
    
    let members = create_test_members(&["member1", "member2", "member3"]);
    for member in &members {
        set.add(member.clone());
    }
    
    let all_members = set.members();
    assert_eq!(all_members.len(), 3);
    
    // Check all members are present (order may vary)
    let member_set: HashSet<_> = all_members.into_iter().collect();
    assert!(member_set.contains(&create_test_member("member1")));
    assert!(member_set.contains(&create_test_member("member2")));
    assert!(member_set.contains(&create_test_member("member3")));
}

// ========== SetDataCow Tests ==========

#[test]
fn test_set_data_cow_basic_operations() {
    let mut set = SetDataCow::new();
    
    // Test initial state
    assert!(!set.is_in_cow_mode());
    assert_eq!(set.ref_count(), 1);
    
    // Test basic operations without COW
    assert!(set.add(create_test_member("member1")));
    assert!(set.contains(b"member1"));
    assert_eq!(set.len(), 1);
    
    assert!(set.remove(b"member1"));
    assert!(!set.contains(b"member1"));
    assert_eq!(set.len(), 0);
}

#[test]
fn test_set_data_cow_snapshot_creation() {
    let mut set = SetDataCow::new();
    
    // Add some data
    set.add(create_test_member("member1"));
    set.add(create_test_member("member2"));
    
    // Create snapshot
    let snapshot = set.make_snapshot();
    assert!(set.is_in_cow_mode());
    assert_eq!(set.ref_count(), 2);
    
    // Verify snapshot contains original data
    let snapshot_data = snapshot.read();
    assert!(snapshot_data.contains(b"member1"));
    assert!(snapshot_data.contains(b"member2"));
    assert_eq!(snapshot_data.len(), 2);
}

#[test]
fn test_set_data_cow_incremental_changes() {
    let mut set = SetDataCow::new();
    
    // Add initial data
    set.add(create_test_member("member1"));
    set.add(create_test_member("member2"));
    
    // Create snapshot
    set.make_snapshot();
    
    // Make incremental changes
    assert!(set.add(create_test_member("member3"))); // New member
    assert!(!set.add(create_test_member("member1"))); // Existing member
    assert!(set.remove(b"member2")); // Remove existing
    assert!(!set.remove(b"nonexistent")); // Remove non-existent
    
    // Verify changes are visible
    assert!(set.contains(b"member1"));
    assert!(!set.contains(b"member2"));
    assert!(set.contains(b"member3"));
    assert_eq!(set.len(), 2);
    
    // Verify members() returns correct data
    let members = set.members();
    assert_eq!(members.len(), 2);
    let member_set: HashSet<_> = members.into_iter().collect();
    assert!(member_set.contains(&create_test_member("member1")));
    assert!(member_set.contains(&create_test_member("member3")));
}

#[test]
fn test_set_data_cow_merge_changes() {
    let mut set = SetDataCow::new();
    
    // Add initial data
    set.add(create_test_member("member1"));
    set.add(create_test_member("member2"));
    
    // Create snapshot
    set.make_snapshot();
    
    // Make changes in COW mode
    set.add(create_test_member("member3"));
    set.remove(b"member2");
    
    // Merge changes back
    set.merge_cow();
    assert!(!set.is_in_cow_mode());
    
    // Verify merged state
    assert!(set.contains(b"member1"));
    assert!(!set.contains(b"member2"));
    assert!(set.contains(b"member3"));
    assert_eq!(set.len(), 2);
}

#[test]
fn test_set_data_cow_clear_in_cow_mode() {
    let mut set = SetDataCow::new();
    
    // Add initial data
    set.add(create_test_member("member1"));
    set.add(create_test_member("member2"));
    
    // Create snapshot
    set.make_snapshot();
    
    // Clear in COW mode
    set.clear();
    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
    assert!(!set.contains(b"member1"));
    assert!(!set.contains(b"member2"));
    
    // Verify members() returns empty
    let members = set.members();
    assert!(members.is_empty());
}

#[test]
fn test_set_data_cow_multiple_snapshots() {
    let mut set1 = SetDataCow::new();
    let mut set2 = SetDataCow::new();
    
    // Add data to set1
    set1.add(create_test_member("member1"));
    set1.add(create_test_member("member2"));
    
    // Create snapshot and share between sets
    set1.make_snapshot();
    set2 = set1.make_cow();
    
    // Both should share the same base
    assert_eq!(set1.ref_count(), 3); // set1.base + set1.snapshot + set2
    assert!(set2.is_in_cow_mode());
    
    // Make independent changes
    set1.add(create_test_member("member3"));
    set2.remove(b"member1");
    
    // Verify independent states
    assert!(set1.contains(b"member1"));
    assert!(set1.contains(b"member3"));
    assert_eq!(set1.len(), 3);
    
    assert!(!set2.contains(b"member1"));
    assert!(set2.contains(b"member2"));
    assert_eq!(set2.len(), 1);
}

// ========== SetStoreCow Tests ==========

#[test]
fn test_set_store_cow_basic_operations() {
    let mut store = SetStoreCow::new();
    
    // Test initial state
    assert!(!store.is_cow_mode());
    
    // Test get_set_mut creates new set
    let set1 = store.get_set_mut(b"set1");
    assert!(set1.is_some());
    
    let set1_mut = set1.unwrap();
    assert!(set1_mut.add(create_test_member("member1")));
    assert!(set1_mut.contains(b"member1"));
}

#[test]
fn test_set_store_cow_snapshot_and_cow() {
    let mut store = SetStoreCow::new();
    
    // Create a set and add data
    let set1 = store.get_set_mut(b"set1").unwrap();
    set1.add(create_test_member("member1"));
    set1.add(create_test_member("member2"));
    
    // Create snapshot
    store.make_snapshot();
    assert!(store.is_cow_mode());
    
    // Modify existing set in COW mode
    let set1_mut = store.get_set_mut(b"set1").unwrap();
    set1_mut.add(create_test_member("member3"));
    set1_mut.remove(b"member1");
    
    // Create new set in COW mode
    let set2 = store.get_set_mut(b"set2").unwrap();
    set2.add(create_test_member("member4"));
    
    // Verify changes
    let set1_check = store.get_set(b"set1");
    assert!(set1_check.is_some());
    assert!(set1_check.unwrap().contains(b"member2"));
    assert!(set1_check.unwrap().contains(b"member3"));
    assert!(!set1_check.unwrap().contains(b"member1"));
    
    let set2_check = store.get_set(b"set2");
    assert!(set2_check.is_some());
    assert!(set2_check.unwrap().contains(b"member4"));
}

#[test]
fn test_set_store_cow_merge_changes() {
    let mut store = SetStoreCow::new();
    
    // Create initial sets
    let set1 = store.get_set_mut(b"set1").unwrap();
    set1.add(create_test_member("member1"));
    
    let set2 = store.get_set_mut(b"set2").unwrap();
    set2.add(create_test_member("member2"));
    
    // Create snapshot
    store.make_snapshot();
    
    // Make changes in COW mode
    let set1_mut = store.get_set_mut(b"set1").unwrap();
    set1_mut.add(create_test_member("member3"));
    
    // Create new set
    let set3 = store.get_set_mut(b"set3").unwrap();
    set3.add(create_test_member("member4"));
    
    // Merge changes
    store.merge_cow();
    assert!(!store.is_cow_mode());
    
    // Verify merged state persists
    let set1_check = store.get_set_mut(b"set1").unwrap();
    assert!(set1_check.contains(b"member1"));
    assert!(set1_check.contains(b"member3"));
    
    let set3_check = store.get_set_mut(b"set3").unwrap();
    assert!(set3_check.contains(b"member4"));
}

#[test]
fn test_set_store_cow_set_removal() {
    let mut store = SetStoreCow::new();
    
    // Create a set
    let set1 = store.get_set_mut(b"set1").unwrap();
    set1.add(create_test_member("member1"));
    
    // Create snapshot
    store.make_snapshot();
    
    // Remove set in COW mode (this would be done by removing all references)
    // Note: The current implementation doesn't have explicit set removal,
    // but we can test that the set data is properly managed
    
    let set1_mut = store.get_set_mut(b"set1").unwrap();
    set1_mut.clear();
    assert!(set1_mut.is_empty());
}

#[test]
fn test_set_data_cow_edge_cases() {
    let mut set = SetDataCow::new();
    
    // Test empty member
    assert!(set.add(vec![]));
    assert!(set.contains(b""));
    assert_eq!(set.len(), 1);
    
    // Test large member
    let large_member = vec![b'x'; 1024 * 1024]; // 1MB member
    assert!(set.add(large_member.clone()));
    assert!(set.contains(&large_member));
    
    // Test binary data
    let binary_data = vec![0, 1, 2, 255, 254, 253];
    assert!(set.add(binary_data.clone()));
    assert!(set.contains(&binary_data));
}

#[test]
fn test_set_data_cow_concurrent_read_write() {
    use std::sync::Arc;
    use std::thread;
    
    let mut set = SetDataCow::new();
    
    // Add initial data
    for i in 0..100 {
        set.add(create_test_member(&format!("member{}", i)));
    }
    
    // Create snapshot
    set.make_snapshot();
    
    let set_arc = Arc::new(parking_lot::RwLock::new(set));
    
    // Spawn reader threads
    let mut handles = vec![];
    
    for i in 0..5 {
        let set_clone = Arc::clone(&set_arc);
        let handle = thread::spawn(move || {
            for j in 0..20 {
                let set = set_clone.read();
                let member = create_test_member(&format!("member{}", j));
                let _contains = set.contains(&member);
                let _len = set.len();
            }
        });
        handles.push(handle);
    }
    
    // Spawn writer thread
    let set_clone = Arc::clone(&set_arc);
    let writer_handle = thread::spawn(move || {
        let mut set = set_clone.write();
        for i in 100..110 {
            set.add(create_test_member(&format!("member{}", i)));
        }
    });
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    writer_handle.join().unwrap();
    
    // Verify final state
    let set = set_arc.read();
    assert_eq!(set.len(), 110);
}

#[test]
fn test_set_store_cow_from_data() {
    let mut data = HashMap::new();
    let mut set_data = SetDataCow::new();
    set_data.add(create_test_member("member1"));
    data.insert(create_test_member("set1"), set_data);
    
    let store = SetStoreCow::from_data(data);
    
    // Verify data is accessible
    let set = store.get_set_mut(b"set1").unwrap();
    assert!(set.contains(b"member1"));
}

#[test]
fn test_set_data_cow_multiple_merges() {
    let mut set = SetDataCow::new();
    
    // First cycle: add data and merge
    set.add(create_test_member("member1"));
    set.make_snapshot();
    set.add(create_test_member("member2"));
    set.merge_cow();
    
    // Second cycle: modify and merge again
    set.make_snapshot();
    set.add(create_test_member("member3"));
    set.remove(b"member1");
    set.merge_cow();
    
    // Verify final state
    assert!(!set.contains(b"member1"));
    assert!(set.contains(b"member2"));
    assert!(set.contains(b"member3"));
    assert_eq!(set.len(), 2);
}