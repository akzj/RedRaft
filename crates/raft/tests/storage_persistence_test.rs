//! End-to-end persistence tests for FileStorage.
//!
//! These tests verify that data survives storage restarts by:
//! 1. Writing data to storage
//! 2. Closing the storage
//! 3. Reopening the same storage directory
//! 4. Verifying all data is recovered correctly

use std::collections::HashSet;

use raft::cluster_config::ClusterConfig;
use raft::message::{HardState, LogEntry, Snapshot};
use raft::storage::{FileStorage, FileStorageOptions};
use raft::traits::{
    ClusterConfigStorage, HardStateStorage, LogEntryStorage, SnapshotStorage,
};
use raft::RaftId;

use tempfile::TempDir;

fn test_raft_id(name: &str) -> RaftId {
    RaftId::new(format!("group_{}", name), format!("node_{}", name))
}

#[tokio::test]
async fn test_storage_persistence_hard_state() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();
    let raft_id = test_raft_id("1");

    // First run: create storage and write data
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        let hard_state = HardState {
            raft_id: raft_id.clone(),
            term: 42,
            voted_for: Some(test_raft_id("2")),
        };

        storage
            .save_hard_state(&raft_id, hard_state)
            .await
            .unwrap();
    } // Storage dropped here

    // Second run: reopen storage and verify data
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        let loaded = storage.load_hard_state(&raft_id).await.unwrap().unwrap();
        assert_eq!(loaded.term, 42);
        assert_eq!(loaded.voted_for, Some(test_raft_id("2")));
    }
}

#[tokio::test]
async fn test_storage_persistence_cluster_config() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();
    let raft_id = test_raft_id("1");

    // First run: create storage and write config
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        let mut voters = HashSet::new();
        voters.insert(test_raft_id("1"));
        voters.insert(test_raft_id("2"));
        voters.insert(test_raft_id("3"));
        let config = ClusterConfig::simple(voters.clone(), 100);

        storage
            .save_cluster_config(&raft_id, config)
            .await
            .unwrap();
    } // Storage dropped here

    // Second run: reopen storage and verify config
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        let loaded = storage.load_cluster_config(&raft_id).await.unwrap();
        assert_eq!(loaded.voters.len(), 3);
        assert_eq!(loaded.log_index, 100);
        assert!(loaded.voters.contains(&test_raft_id("1")));
        assert!(loaded.voters.contains(&test_raft_id("2")));
        assert!(loaded.voters.contains(&test_raft_id("3")));
    }
}

#[tokio::test]
async fn test_storage_persistence_log_entries() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();
    let raft_id = test_raft_id("1");

    // First run: create storage and write log entries
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                command: b"command1".to_vec(),
                is_config: false,
                client_request_id: None,
            },
            LogEntry {
                index: 2,
                term: 1,
                command: b"command2".to_vec(),
                is_config: false,
                client_request_id: None,
            },
            LogEntry {
                index: 3,
                term: 2,
                command: b"command3".to_vec(),
                is_config: false,
                client_request_id: None,
            },
        ];

        storage
            .append_log_entries(&raft_id, &entries)
            .await
            .unwrap();

        // Verify last log index
        let (last_idx, last_term) = storage.get_last_log_index(&raft_id).await.unwrap();
        assert_eq!(last_idx, 3);
        assert_eq!(last_term, 2);
    } // Storage dropped here

    // Second run: reopen storage and verify log entries
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        let loaded = storage.get_log_entries(&raft_id, 1, 4).await.unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].index, 1);
        assert_eq!(loaded[0].term, 1);
        assert_eq!(loaded[0].command, b"command1");
        assert_eq!(loaded[1].index, 2);
        assert_eq!(loaded[1].command, b"command2");
        assert_eq!(loaded[2].index, 3);
        assert_eq!(loaded[2].term, 2);
        assert_eq!(loaded[2].command, b"command3");

        // Verify last log index
        let (last_idx, last_term) = storage.get_last_log_index(&raft_id).await.unwrap();
        assert_eq!(last_idx, 3);
        assert_eq!(last_term, 2);

        // Verify log term
        assert_eq!(storage.get_log_term(&raft_id, 1).await.unwrap(), 1);
        assert_eq!(storage.get_log_term(&raft_id, 2).await.unwrap(), 1);
        assert_eq!(storage.get_log_term(&raft_id, 3).await.unwrap(), 2);
    }
}

#[tokio::test]
async fn test_storage_persistence_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();
    let raft_id = test_raft_id("1");

    // First run: create storage and write snapshot
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        let mut voters = HashSet::new();
        voters.insert(test_raft_id("1"));
        let config = ClusterConfig::simple(voters, 50);

        let snapshot = Snapshot {
            index: 100,
            term: 5,
            data: b"snapshot data content".to_vec(),
            config,
        };

        storage
            .save_snapshot(&raft_id, snapshot)
            .await
            .unwrap();
    } // Storage dropped here

    // Second run: reopen storage and verify snapshot
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        let loaded = storage.load_snapshot(&raft_id).await.unwrap().unwrap();
        assert_eq!(loaded.index, 100);
        assert_eq!(loaded.term, 5);
        assert_eq!(loaded.data, b"snapshot data content".to_vec());
        assert_eq!(loaded.config.log_index, 50);
    }
}

#[tokio::test]
async fn test_storage_persistence_all_data_types() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();
    let raft_id = test_raft_id("1");

    // First run: write all types of data
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        // Hard state
        let hard_state = HardState {
            raft_id: raft_id.clone(),
            term: 10,
            voted_for: Some(test_raft_id("leader")),
        };
        storage
            .save_hard_state(&raft_id, hard_state)
            .await
            .unwrap();

        // Cluster config
        let mut voters = HashSet::new();
        voters.insert(test_raft_id("1"));
        voters.insert(test_raft_id("2"));
        let config = ClusterConfig::simple(voters, 1);
        storage
            .save_cluster_config(&raft_id, config)
            .await
            .unwrap();

        // Log entries
        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                command: b"entry1".to_vec(),
                is_config: false,
                client_request_id: None,
            },
            LogEntry {
                index: 2,
                term: 1,
                command: b"entry2".to_vec(),
                is_config: false,
                client_request_id: None,
            },
        ];
        storage
            .append_log_entries(&raft_id, &entries)
            .await
            .unwrap();

        // Snapshot
        let snapshot = Snapshot {
            index: 50,
            term: 3,
            data: b"snapshot".to_vec(),
            config: ClusterConfig::empty(),
        };
        storage
            .save_snapshot(&raft_id, snapshot)
            .await
            .unwrap();
    } // Storage dropped here

    // Second run: verify all data types
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        // Verify hard state
        let hs = storage.load_hard_state(&raft_id).await.unwrap().unwrap();
        assert_eq!(hs.term, 10);
        assert_eq!(hs.voted_for, Some(test_raft_id("leader")));

        // Verify cluster config
        let config = storage.load_cluster_config(&raft_id).await.unwrap();
        assert_eq!(config.voters.len(), 2);
        assert_eq!(config.log_index, 1);

        // Verify log entries
        let entries = storage.get_log_entries(&raft_id, 1, 3).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].command, b"entry1");
        assert_eq!(entries[1].command, b"entry2");

        // Verify snapshot
        let snap = storage.load_snapshot(&raft_id).await.unwrap().unwrap();
        assert_eq!(snap.index, 50);
        assert_eq!(snap.term, 3);
        assert_eq!(snap.data, b"snapshot");
    }
}

#[tokio::test]
async fn test_storage_persistence_multiple_raft_groups() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();
    let raft_id1 = test_raft_id("group1");
    let raft_id2 = test_raft_id("group2");

    // First run: write data for multiple groups
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        // Group 1
        storage
            .save_hard_state(
                &raft_id1,
                HardState {
                    raft_id: raft_id1.clone(),
                    term: 5,
                    voted_for: None,
                },
            )
            .await
            .unwrap();

        storage
            .append_log_entries(
                &raft_id1,
                &[LogEntry {
                    index: 1,
                    term: 1,
                    command: b"g1_cmd1".to_vec(),
                    is_config: false,
                    client_request_id: None,
                }],
            )
            .await
            .unwrap();

        // Group 2
        storage
            .save_hard_state(
                &raft_id2,
                HardState {
                    raft_id: raft_id2.clone(),
                    term: 10,
                    voted_for: Some(test_raft_id("leader2")),
                },
            )
            .await
            .unwrap();

        storage
            .append_log_entries(
                &raft_id2,
                &[LogEntry {
                    index: 1,
                    term: 2,
                    command: b"g2_cmd1".to_vec(),
                    is_config: false,
                    client_request_id: None,
                }],
            )
            .await
            .unwrap();
    } // Storage dropped here

    // Second run: verify both groups' data
    {
        let options = FileStorageOptions::with_base_dir(&base_dir);
        let (storage, rx) = FileStorage::new(options).unwrap();
        storage.start(rx);

        // Verify group 1
        let hs1 = storage.load_hard_state(&raft_id1).await.unwrap().unwrap();
        assert_eq!(hs1.term, 5);
        let entries1 = storage.get_log_entries(&raft_id1, 1, 2).await.unwrap();
        assert_eq!(entries1.len(), 1);
        assert_eq!(entries1[0].command, b"g1_cmd1");

        // Verify group 2
        let hs2 = storage.load_hard_state(&raft_id2).await.unwrap().unwrap();
        assert_eq!(hs2.term, 10);
        assert_eq!(hs2.voted_for, Some(test_raft_id("leader2")));
        let entries2 = storage.get_log_entries(&raft_id2, 1, 2).await.unwrap();
        assert_eq!(entries2.len(), 1);
        assert_eq!(entries2[0].command, b"g2_cmd1");
    }
}

