//! Example demonstrating how to use FileStorage for persistent Raft storage.
//!
//! This example shows:
//! 1. Creating a FileStorage instance
//! 2. Saving and loading hard state
//! 3. Saving and loading cluster config
//! 4. Appending and reading log entries
//! 5. Saving and loading snapshots
//! 6. Data persistence across restarts

use std::collections::HashSet;

use raft_lite::cluster_config::ClusterConfig;
use raft_lite::message::{HardState, LogEntry, Snapshot};
use raft_lite::storage::{FileStorage, FileStorageOptions};
use raft_lite::traits::{ClusterConfigStorage, HardStateStorage, LogEntryStorage, SnapshotStorage};
use raft_lite::RaftId;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    // Create a temporary directory for this example
    let temp_dir = tempfile::tempdir()?;
    let base_dir = temp_dir.path().to_path_buf();

    println!("Using storage directory: {:?}", base_dir);

    // Create FileStorage with custom options
    let options = FileStorageOptions {
        base_dir: base_dir.clone(),
        max_segment_size: 64 * 1024 * 1024, // 64MB
        max_io_threads: 4,
        sync_on_write: true,
        batch_size: 100,
        cache_entries_size: 1000,
        min_free_disk_space: 100 * 1024 * 1024, // 100MB
        verify_snapshot_checksum: true,
    };

    let (storage, rx) = FileStorage::new(options)?;
    
    // Start the background processor (required for write operations)
    storage.start(rx);

    // Create a Raft ID for this example
    let raft_id = RaftId::new("example_group".to_string(), "node1".to_string());

    println!("\n=== Example 1: Hard State ===");
    
    // Save hard state
    let hard_state = HardState {
        raft_id: raft_id.clone(),
        term: 5,
        voted_for: Some(RaftId::new("example_group".to_string(), "node2".to_string())),
    };
    storage.save_hard_state(&raft_id, hard_state.clone()).await?;
    println!("✓ Saved hard state: term={}, voted_for={:?}", hard_state.term, hard_state.voted_for);

    // Load hard state
    let loaded = storage.load_hard_state(&raft_id).await?.unwrap();
    println!("✓ Loaded hard state: term={}, voted_for={:?}", loaded.term, loaded.voted_for);

    println!("\n=== Example 2: Cluster Config ===");
    
    // Save cluster config
    let mut voters = HashSet::new();
    voters.insert(RaftId::new("example_group".to_string(), "node1".to_string()));
    voters.insert(RaftId::new("example_group".to_string(), "node2".to_string()));
    voters.insert(RaftId::new("example_group".to_string(), "node3".to_string()));
    let config = ClusterConfig::simple(voters.clone(), 10);
    storage.save_cluster_config(&raft_id, config.clone()).await?;
    println!("✓ Saved cluster config: {} voters, log_index={}", config.voters.len(), config.log_index);

    // Load cluster config
    let loaded_config = storage.load_cluster_config(&raft_id).await?;
    println!("✓ Loaded cluster config: {} voters, log_index={}", loaded_config.voters.len(), loaded_config.log_index);

    println!("\n=== Example 3: Log Entries ===");
    
    // Append log entries
    let entries = vec![
        LogEntry {
            index: 1,
            term: 1,
            command: b"set key1 value1".to_vec(),
            is_config: false,
            client_request_id: None,
        },
        LogEntry {
            index: 2,
            term: 1,
            command: b"set key2 value2".to_vec(),
            is_config: false,
            client_request_id: None,
        },
        LogEntry {
            index: 3,
            term: 2,
            command: b"delete key1".to_vec(),
            is_config: false,
            client_request_id: None,
        },
    ];
    storage.append_log_entries(&raft_id, &entries).await?;
    println!("✓ Appended {} log entries", entries.len());

    // Read log entries
    let loaded_entries = storage.get_log_entries(&raft_id, 1, 4).await?;
    println!("✓ Loaded {} log entries:", loaded_entries.len());
    for entry in &loaded_entries {
        println!("  - index={}, term={}, command={:?}", 
                 entry.index, entry.term, String::from_utf8_lossy(&entry.command));
    }

    // Get last log index
    let (last_idx, last_term) = storage.get_last_log_index(&raft_id).await?;
    println!("✓ Last log index: {}, term: {}", last_idx, last_term);

    println!("\n=== Example 4: Snapshot ===");
    
    // Save snapshot
    let snapshot = Snapshot {
        index: 100,
        term: 5,
        data: b"snapshot data: key2=value2".to_vec(),
        config: ClusterConfig::simple(voters, 50),
    };
    storage.save_snapshot(&raft_id, snapshot.clone()).await?;
    println!("✓ Saved snapshot: index={}, term={}, data_size={} bytes", 
             snapshot.index, snapshot.term, snapshot.data.len());

    // Load snapshot
    let loaded_snapshot = storage.load_snapshot(&raft_id).await?.unwrap();
    println!("✓ Loaded snapshot: index={}, term={}, data={:?}", 
             loaded_snapshot.index, loaded_snapshot.term, 
             String::from_utf8_lossy(&loaded_snapshot.data));

    println!("\n=== Example 5: Storage Statistics ===");
    
    // Get disk statistics
    println!("✓ Disk stats:");
    println!("  - Total segments: {}", storage.log_segment_count());
    println!("  - Total disk usage: {} bytes", storage.get_log_disk_usage());
    println!("  - Disk space low: {}", storage.is_disk_space_low());

    println!("\n=== Example 6: Persistence Test ===");
    println!("Storage will be closed and reopened to verify persistence...");
    
    // Drop storage to simulate restart
    drop(storage);
    
    // Reopen storage from the same directory
    let options = FileStorageOptions::with_base_dir(&base_dir);
    let (storage2, rx2) = FileStorage::new(options)?;
    storage2.start(rx2);

    // Verify all data is still there
    let loaded_hs = storage2.load_hard_state(&raft_id).await?.unwrap();
    assert_eq!(loaded_hs.term, 5);
    println!("✓ Hard state persisted: term={}", loaded_hs.term);

    let loaded_cc = storage2.load_cluster_config(&raft_id).await?;
    assert_eq!(loaded_cc.voters.len(), 3);
    println!("✓ Cluster config persisted: {} voters", loaded_cc.voters.len());

    let loaded_entries = storage2.get_log_entries(&raft_id, 1, 4).await?;
    assert_eq!(loaded_entries.len(), 3);
    println!("✓ Log entries persisted: {} entries", loaded_entries.len());

    let loaded_snap = storage2.load_snapshot(&raft_id).await?.unwrap();
    assert_eq!(loaded_snap.index, 100);
    println!("✓ Snapshot persisted: index={}", loaded_snap.index);

    println!("\n✅ All examples completed successfully!");
    println!("Storage directory: {:?}", base_dir);
    println!("You can inspect the files in the logs/ and snapshots/ subdirectories.");

    Ok(())
}

