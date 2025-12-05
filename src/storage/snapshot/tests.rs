//! Tests for file-based snapshot storage.

use super::*;
use crate::cluster_config::ClusterConfig;
use crate::message::Snapshot;
use crate::traits::SnapshotStorage;
use crate::RaftId;
use tempfile::TempDir;

fn test_raft_id(group: &str, node: &str) -> RaftId {
    RaftId::new(group.to_string(), node.to_string())
}

fn create_test_snapshot(index: u64, term: u64) -> Snapshot {
    let mut voters = std::collections::HashSet::new();
    voters.insert(test_raft_id("test", "node1"));
    voters.insert(test_raft_id("test", "node2"));
    voters.insert(test_raft_id("test", "node3"));

    Snapshot {
        index,
        term,
        data: format!("snapshot_data_{}_{}", index, term).into_bytes(),
        config: ClusterConfig::simple(voters, index),
    }
}

#[tokio::test]
async fn test_save_and_load_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let options = SnapshotStorageOptions::with_base_dir(temp_dir.path());
    let storage = FileSnapshotStorage::new(options).unwrap();

    let raft_id = test_raft_id("test_group", "node1");
    let snapshot = create_test_snapshot(100, 5);

    // Save snapshot
    storage.save_snapshot(&raft_id, snapshot.clone()).await.unwrap();

    // Load snapshot (reads from disk each time - no caching)
    let loaded = storage.load_snapshot(&raft_id).await.unwrap();
    assert!(loaded.is_some());

    let loaded = loaded.unwrap();
    assert_eq!(loaded.index, snapshot.index);
    assert_eq!(loaded.term, snapshot.term);
    assert_eq!(loaded.data, snapshot.data);
}

#[tokio::test]
async fn test_load_nonexistent_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let options = SnapshotStorageOptions::with_base_dir(temp_dir.path());
    let storage = FileSnapshotStorage::new(options).unwrap();

    let raft_id = test_raft_id("nonexistent", "node1");
    let loaded = storage.load_snapshot(&raft_id).await.unwrap();
    assert!(loaded.is_none());
}

#[tokio::test]
async fn test_overwrite_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let options = SnapshotStorageOptions::with_base_dir(temp_dir.path());
    let storage = FileSnapshotStorage::new(options).unwrap();

    let raft_id = test_raft_id("test_group", "node1");

    // Save first snapshot
    let snapshot1 = create_test_snapshot(100, 5);
    storage.save_snapshot(&raft_id, snapshot1).await.unwrap();

    // Save second snapshot (overwrite)
    let snapshot2 = create_test_snapshot(200, 10);
    storage.save_snapshot(&raft_id, snapshot2.clone()).await.unwrap();

    // Load should return second snapshot
    let loaded = storage.load_snapshot(&raft_id).await.unwrap().unwrap();
    assert_eq!(loaded.index, 200);
    assert_eq!(loaded.term, 10);
}

#[tokio::test]
async fn test_multiple_raft_groups() {
    let temp_dir = TempDir::new().unwrap();
    let options = SnapshotStorageOptions::with_base_dir(temp_dir.path());
    let storage = FileSnapshotStorage::new(options).unwrap();

    let raft_id1 = test_raft_id("group1", "node1");
    let raft_id2 = test_raft_id("group2", "node1");

    let snapshot1 = create_test_snapshot(100, 5);
    let snapshot2 = create_test_snapshot(200, 10);

    // Save snapshots for different groups
    storage.save_snapshot(&raft_id1, snapshot1.clone()).await.unwrap();
    storage.save_snapshot(&raft_id2, snapshot2.clone()).await.unwrap();

    // Load and verify
    let loaded1 = storage.load_snapshot(&raft_id1).await.unwrap().unwrap();
    let loaded2 = storage.load_snapshot(&raft_id2).await.unwrap().unwrap();

    assert_eq!(loaded1.index, 100);
    assert_eq!(loaded2.index, 200);
}

#[tokio::test]
async fn test_delete_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let options = SnapshotStorageOptions::with_base_dir(temp_dir.path());
    let storage = FileSnapshotStorage::new(options).unwrap();

    let raft_id = test_raft_id("test_group", "node1");
    let snapshot = create_test_snapshot(100, 5);

    // Save snapshot
    storage.save_snapshot(&raft_id, snapshot).await.unwrap();

    // Verify it exists
    let loaded = storage.load_snapshot(&raft_id).await.unwrap();
    assert!(loaded.is_some());

    // Delete snapshot
    storage.delete_snapshot(&raft_id).unwrap();

    // Verify it's gone
    let loaded = storage.load_snapshot(&raft_id).await.unwrap();
    assert!(loaded.is_none());
}

#[tokio::test]
async fn test_get_snapshot_meta() {
    let temp_dir = TempDir::new().unwrap();
    let options = SnapshotStorageOptions::with_base_dir(temp_dir.path());
    let storage = FileSnapshotStorage::new(options).unwrap();

    let raft_id = test_raft_id("test_group", "node1");
    let snapshot = create_test_snapshot(100, 5);

    // Save snapshot
    storage.save_snapshot(&raft_id, snapshot).await.unwrap();

    // Get metadata without loading data
    let meta = storage.get_snapshot_meta(&raft_id).unwrap();
    assert!(meta.is_some());

    let (index, term) = meta.unwrap();
    assert_eq!(index, 100);
    assert_eq!(term, 5);
}

#[tokio::test]
async fn test_list_snapshots() {
    let temp_dir = TempDir::new().unwrap();
    let options = SnapshotStorageOptions::with_base_dir(temp_dir.path());
    let storage = FileSnapshotStorage::new(options).unwrap();

    let raft_id1 = test_raft_id("group1", "node1");
    let raft_id2 = test_raft_id("group2", "node2");

    let snapshot1 = create_test_snapshot(100, 5);
    let snapshot2 = create_test_snapshot(200, 10);

    // Save snapshots
    storage.save_snapshot(&raft_id1, snapshot1).await.unwrap();
    storage.save_snapshot(&raft_id2, snapshot2).await.unwrap();

    // List snapshots
    let snapshots = storage.list_snapshots().unwrap();
    assert_eq!(snapshots.len(), 2);
    assert!(snapshots.contains(&raft_id1));
    assert!(snapshots.contains(&raft_id2));
}

#[tokio::test]
async fn test_large_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let options = SnapshotStorageOptions::with_base_dir(temp_dir.path());
    let storage = FileSnapshotStorage::new(options).unwrap();

    let raft_id = test_raft_id("test_group", "node1");

    // Create a large snapshot (1MB)
    let large_data = vec![0u8; 1024 * 1024];
    let mut voters = std::collections::HashSet::new();
    voters.insert(test_raft_id("test", "node1"));

    let snapshot = Snapshot {
        index: 1000,
        term: 50,
        data: large_data.clone(),
        config: ClusterConfig::simple(voters, 1000),
    };

    // Save large snapshot
    storage.save_snapshot(&raft_id, snapshot).await.unwrap();

    // Load and verify (reads directly from disk - no caching)
    let loaded = storage.load_snapshot(&raft_id).await.unwrap().unwrap();
    assert_eq!(loaded.index, 1000);
    assert_eq!(loaded.term, 50);
    assert_eq!(loaded.data.len(), 1024 * 1024);
    assert_eq!(loaded.data, large_data);
}

#[test]
fn test_checksum_calculation() {
    let data = b"test data for checksum";
    let checksum = FileSnapshotStorage::calculate_checksum(data);

    // SHA256 should produce a 64-character hex string
    assert_eq!(checksum.len(), 64);

    // Same data should produce same checksum
    let checksum2 = FileSnapshotStorage::calculate_checksum(data);
    assert_eq!(checksum, checksum2);

    // Different data should produce different checksum
    let checksum3 = FileSnapshotStorage::calculate_checksum(b"different data");
    assert_ne!(checksum, checksum3);
}

#[test]
fn test_checksum_verification() {
    let data = b"test data for verification";
    let checksum = FileSnapshotStorage::calculate_checksum(data);

    assert!(FileSnapshotStorage::verify_checksum(data, &checksum));
    assert!(!FileSnapshotStorage::verify_checksum(b"wrong data", &checksum));
}

