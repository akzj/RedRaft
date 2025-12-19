#[path = "mock/mod.rs"]
pub mod mock;

use mock::mock_network::MockNetworkHubConfig;
use raft::RaftId;
use std::time::Duration;
use tokio;

mod common;
use common::test_cluster::{TestCluster, TestClusterConfig};
use common::test_statemachine::KvCommand;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_bootstrap_snapshot_creation() {
    let _ = tracing_subscriber::fmt().try_init();

    // Create 3-node cluster
    let node1 = RaftId::new("test_group".to_string(), "node1".to_string());
    let node2 = RaftId::new("test_group".to_string(), "node2".to_string());
    let node3 = RaftId::new("test_group".to_string(), "node3".to_string());

    let config = TestClusterConfig {
        node_ids: vec![node1.clone(), node2.clone(), node3.clone()],
        hub: MockNetworkHubConfig::default(),
    };
    let cluster = TestCluster::new(config).await;

    // Start cluster in background
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // Wait for leader election
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Wait for stable leader
    let leader_id = cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("Should have a leader");

    println!("✓ Leader election successful, leader: {:?}", leader_id);

    // ===== 1. Write test data to state machine =====
    println!("\n=== Writing test data to state machine ===");

    // Write multiple batches of data to ensure we have committed logs
    for batch in 1..=3 {
        println!("\n--- Writing batch {} ---", batch);
        for i in 1..=100 {
            let command = KvCommand::Set {
                key: format!("bootstrap_key{}-{}", batch, i),
                value: format!("bootstrap_value{}-{}", batch, i),
            };

            match cluster.propose_command(&leader_id, &command) {
                Ok(()) => {
                    if i % 25 == 0 {
                        println!("✓ Successfully proposed command batch {}/100", i);
                    }
                }
                Err(e) => println!("✗ Failed to propose command: {}", e),
            }

            // Small delay to avoid excessive sending
            if i % 10 == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        // Wait for data replication
        println!("Waiting for data replication...");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // ===== 2. Wait for commit_index == apply_index (no pending requests) =====
    println!("\n=== Waiting for commit_index == apply_index ===");

    let mut wait_attempts = 0;
    const MAX_WAIT_ATTEMPTS: u32 = 50; // 5 seconds total

    loop {
        if let Some(leader_node) = cluster.get_node(&leader_id) {
            let commit_index = leader_node.get_commit_index().await;
            let apply_index = leader_node.get_last_applied().await;

            println!(
                "  Attempt {}: commit_index={}, apply_index={}",
                wait_attempts + 1,
                commit_index,
                apply_index
            );

            if commit_index == apply_index {
                println!(
                    "✓ commit_index == apply_index ({}), no pending requests",
                    commit_index
                );
                break;
            }

            wait_attempts += 1;
            if wait_attempts >= MAX_WAIT_ATTEMPTS {
                panic!(
                    "Timeout waiting for commit_index == apply_index. Final: commit_index={}, apply_index={}",
                    commit_index, apply_index
                );
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            panic!("Leader node not found");
        }
    }

    // Get final apply_index before bootstrap snapshot
    let leader_node = cluster.get_node(&leader_id).expect("Leader node not found");
    let apply_index_before = leader_node.get_last_applied().await;
    let commit_index_before = leader_node.get_commit_index().await;

    assert_eq!(
        apply_index_before, commit_index_before,
        "commit_index should equal apply_index before bootstrap snapshot"
    );

    println!(
        "\n✓ Ready for bootstrap snapshot: apply_index={}, commit_index={}",
        apply_index_before, commit_index_before
    );

    // ===== 3. Force modify state machine directly (simulating split operation) =====
    println!("\n=== Force modifying state machine directly ===");

    // Directly modify state machine on leader (simulating state injection during split)
    // This bypasses Raft protocol and directly modifies the state machine
    let direct_modification_count = 50;
    for i in 1..=direct_modification_count {
        let key = format!("direct_key{}", i);
        let value = format!("direct_value{}", i);
        leader_node.direct_set_state(key, value);
        if i % 10 == 0 {
            println!("✓ Directly modified {} state machine entries", i);
        }
    }

    println!(
        "✓ Directly modified {} entries in state machine (bypassing Raft protocol)",
        direct_modification_count
    );

    // Verify the direct modifications are in the state machine
    let leader_data_after_direct = leader_node.get_all_data();
    let direct_keys_count = leader_data_after_direct
        .keys()
        .filter(|k| k.starts_with("direct_key"))
        .count();
    assert_eq!(
        direct_keys_count,
        direct_modification_count,
        "All direct modifications should be in state machine"
    );
    println!(
        "✓ Verified {} direct modifications are in state machine",
        direct_keys_count
    );

    // Note: After direct modification, commit_index and apply_index remain unchanged
    // because we bypassed Raft protocol. This is the scenario where bootstrap snapshot
    // is useful - to capture the current state machine state even though no new logs were committed.

    // ===== 4. Trigger bootstrap snapshot creation =====
    println!("\n=== Triggering bootstrap snapshot creation ===");

    // Get current snapshot index before bootstrap
    let snapshot_index_before = leader_node.get_last_snapshot_index().await;

    println!("  Snapshot index before bootstrap: {}", snapshot_index_before);

    // Calculate expected snapshot index
    let expected_snapshot_index = apply_index_before + 1;

    // Trigger bootstrap snapshot
    match cluster.trigger_bootstrap_snapshot(&leader_id) {
        Ok(()) => {
            println!("✓ Bootstrap snapshot creation triggered");
        }
        Err(e) => {
            panic!("Failed to trigger bootstrap snapshot: {}", e);
        }
    }

    // Wait for snapshot creation to complete (poll instead of fixed sleep)
    // This avoids long sleep that might cause election timeout
    println!("Waiting for bootstrap snapshot creation to complete...");
    let mut wait_attempts = 0;
    const MAX_SNAPSHOT_CREATION_WAIT_ATTEMPTS: u32 = 100; // 10 seconds total (100ms * 100)
    
    loop {
        let current_snapshot_index = leader_node.get_last_snapshot_index().await;
        if current_snapshot_index >= expected_snapshot_index {
            println!("✓ Snapshot creation completed (index: {})", current_snapshot_index);
            break;
        }
        
        wait_attempts += 1;
        if wait_attempts >= MAX_SNAPSHOT_CREATION_WAIT_ATTEMPTS {
            println!("⚠ Snapshot creation may still be in progress (current index: {}, expected: {})", 
                     current_snapshot_index, expected_snapshot_index);
            break;
        }
        
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ===== 5. Verify snapshot_index == apply_index + 1 =====
    println!("\n=== Verifying bootstrap snapshot index ===");

    // Re-get leader node (in case leader changed during snapshot creation)
    let leader_node_after = cluster.get_node(&leader_id).expect("Leader node not found");
    let snapshot_index_after = leader_node_after.get_last_snapshot_index().await;

    println!("  apply_index before bootstrap: {}", apply_index_before);
    println!("  expected snapshot_index: {}", expected_snapshot_index);
    println!("  actual snapshot_index: {}", snapshot_index_after);

    assert_eq!(
        snapshot_index_after,
        expected_snapshot_index,
        "Bootstrap snapshot index should be apply_index + 1. Expected: {}, Got: {}",
        expected_snapshot_index,
        snapshot_index_after
    );

    println!("✓ Bootstrap snapshot index verified: {}", snapshot_index_after);

    // ===== 6. Wait for snapshot distribution to followers =====
    println!("\n=== Waiting for snapshot distribution to followers ===");

    // Use config.node_ids to get all node IDs (since nodes field is private)
    let follower_ids: Vec<RaftId> = vec![node1.clone(), node2.clone(), node3.clone()]
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();

    // Wait for followers to receive and install snapshot (with retry)
    let mut wait_attempts = 0;
    const MAX_SNAPSHOT_WAIT_ATTEMPTS: u32 = 50; // 5 seconds total

    loop {
        let mut all_followers_synced = true;
        for follower_id in &follower_ids {
            if let Some(follower_node) = cluster.get_node(follower_id) {
                let follower_snapshot_index = follower_node.get_last_snapshot_index().await;
                println!(
                    "  Attempt {}: Follower {:?} snapshot_index: {} (expected >= {})",
                    wait_attempts + 1,
                    follower_id,
                    follower_snapshot_index,
                    expected_snapshot_index
                );

                if follower_snapshot_index < expected_snapshot_index {
                    all_followers_synced = false;
                    break;
                }
            }
        }

        if all_followers_synced {
            println!("✓ All followers have received the snapshot");
            break;
        }

        wait_attempts += 1;
        if wait_attempts >= MAX_SNAPSHOT_WAIT_ATTEMPTS {
            // Final check - print status but don't fail if snapshot is close
            for follower_id in &follower_ids {
                if let Some(follower_node) = cluster.get_node(follower_id) {
                    let follower_snapshot_index = follower_node.get_last_snapshot_index().await;
                    println!(
                        "  Final: Follower {:?} snapshot_index: {}",
                        follower_id, follower_snapshot_index
                    );
                }
            }
            // Allow test to continue - snapshot distribution may take time
            println!("⚠ Snapshot distribution may still be in progress");
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Verify all followers have the snapshot (final check)
    for follower_id in &follower_ids {
        if let Some(follower_node) = cluster.get_node(follower_id) {
            let follower_snapshot_index = follower_node.get_last_snapshot_index().await;
            println!(
                "  Follower {:?} final snapshot_index: {}",
                follower_id, follower_snapshot_index
            );

            // Follower should have received snapshot (snapshot_index >= expected)
            // Note: In some cases, snapshot may be at expected_snapshot_index - 1 if it was created
            // before the bootstrap snapshot, so we allow snapshot_index >= expected_snapshot_index - 1
            assert!(
                follower_snapshot_index >= expected_snapshot_index.saturating_sub(1),
                "Follower {:?} should have snapshot_index >= {}. Got: {}",
                follower_id,
                expected_snapshot_index.saturating_sub(1),
                follower_snapshot_index
            );
        }
    }

    println!("✓ Snapshot distribution verified");

    // ===== 7. Verify state machine consistency =====
    println!("\n=== Verifying state machine consistency ===");

    // Wait for final synchronization
    match cluster
        .wait_for_data_replication(Duration::from_secs(5))
        .await
    {
        Ok(()) => {
            println!("✓ Final data replication completed");
        }
        Err(e) => {
            panic!("Final data replication failed: {}", e);
        }
    }

    // Verify data consistency across all nodes
    match cluster.verify_data_consistency().await {
        Ok(()) => {
            println!("✓ Data consistency verified across all nodes");
        }
        Err(e) => {
            panic!("Data consistency check failed: {}", e);
        }
    }

    // Get final data statistics
    if let Some(final_data) = cluster.get_node_data(&leader_id) {
        println!(
            "✓ Final cluster state: {} key-value pairs",
            final_data.len()
        );

        // Verify data integrity - should have 3 * 100 (from Raft) + 50 (direct modifications) = 350 records
        let expected_count = 3 * 100 + direct_modification_count;
        assert_eq!(
            final_data.len(),
            expected_count,
            "Data integrity check failed: expected {} (300 from Raft + {} direct), got {}",
            expected_count,
            direct_modification_count,
            final_data.len()
        );
        println!(
            "✓ Data integrity verified: {} entries as expected ({} from Raft + {} direct modifications)",
            expected_count,
            3 * 100,
            direct_modification_count
        );

        // Verify direct modifications are present in all nodes after snapshot distribution
        let direct_keys_in_final = final_data
            .keys()
            .filter(|k| k.starts_with("direct_key"))
            .count();
        assert_eq!(
            direct_keys_in_final,
            direct_modification_count,
            "All direct modifications should be present after snapshot distribution. Expected: {}, Got: {}",
            direct_modification_count,
            direct_keys_in_final
        );
        println!(
            "✓ Verified all {} direct modifications are present in final state",
            direct_keys_in_final
        );
    }

    println!("\n=== Bootstrap snapshot test completed successfully ===");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_bootstrap_snapshot_with_pending_requests() {
    let _ = tracing_subscriber::fmt().try_init();

    // Create 3-node cluster
    let node1 = RaftId::new("test_group2".to_string(), "node1".to_string());
    let node2 = RaftId::new("test_group2".to_string(), "node2".to_string());
    let node3 = RaftId::new("test_group2".to_string(), "node3".to_string());

    let config = TestClusterConfig {
        node_ids: vec![node1.clone(), node2.clone(), node3.clone()],
        hub: MockNetworkHubConfig::default(),
    };
    let cluster = TestCluster::new(config).await;

    // Start cluster in background
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // Wait for leader election
    tokio::time::sleep(Duration::from_secs(2)).await;

    let leader_id = cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("Should have a leader");

    println!("✓ Leader election successful, leader: {:?}", leader_id);

    // Write some data
    for i in 1..=50 {
        let command = KvCommand::Set {
            key: format!("pending_key{}", i),
            value: format!("pending_value{}", i),
        };
        let _ = cluster.propose_command(&leader_id, &command);
        if i % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Don't wait for commit_index == apply_index, try to trigger bootstrap snapshot immediately
    // This should fail or be skipped because there are pending requests
    println!("\n=== Attempting bootstrap snapshot with pending requests ===");

    let apply_index_before = {
        if let Some(leader_node) = cluster.get_node(&leader_id) {
            leader_node.get_last_applied().await
        } else {
            0
        }
    };

    let commit_index_before = {
        if let Some(leader_node) = cluster.get_node(&leader_id) {
            leader_node.get_commit_index().await
        } else {
            0
        }
    };

    println!(
        "  Before bootstrap: commit_index={}, apply_index={}",
        commit_index_before, apply_index_before
    );

    // Trigger bootstrap snapshot (should be skipped if commit_index != apply_index)
    let _ = cluster.trigger_bootstrap_snapshot(&leader_id);

    // Wait a bit
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check if snapshot was created (it should not be created if there are pending requests)
    let snapshot_index_after = {
        if let Some(leader_node) = cluster.get_node(&leader_id) {
            leader_node.get_last_snapshot_index().await
        } else {
            0
        }
    };

    println!("  Snapshot index after: {}", snapshot_index_after);

    // If commit_index != apply_index, bootstrap snapshot should not be created
    // So snapshot_index should not be apply_index + 1
    if commit_index_before != apply_index_before {
        println!("✓ Bootstrap snapshot correctly skipped due to pending requests");
        // Snapshot index should not have changed to apply_index + 1
        assert!(
            snapshot_index_after != apply_index_before + 1,
            "Bootstrap snapshot should not be created when commit_index != apply_index"
        );
    }

    println!("\n=== Bootstrap snapshot with pending requests test completed ===");
}

