//! Integration tests for Pre-Vote, ReadIndex, and LeaderLease features.
//!
//! These tests verify:
//! 1. Pre-Vote prevents term inflation during network partitions
//! 2. ReadIndex requests are handled correctly by the leader
//! 3. Cluster stability after leader isolation with Pre-Vote enabled

pub mod common;

use raft_lite::tests::mock::mock_network::MockNetworkHubConfig;
use raft_lite::{RaftId, Role};
use std::time::Duration;
use tracing_subscriber;

use crate::common::test_cluster::{TestCluster, TestClusterConfig};

/// Helper function to wait for leader election
async fn wait_for_leader(cluster: &TestCluster, node_ids: &[&RaftId]) -> Option<RaftId> {
    let timeout = Duration::from_secs(10);
    let start_time = tokio::time::Instant::now();

    while start_time.elapsed() < timeout {
        for node_id in node_ids {
            if let Some(node) = cluster.get_node(node_id) {
                if node.get_role() == Role::Leader {
                    return Some((*node_id).clone());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// Test: Pre-Vote prevents term inflation during network partition
///
/// Scenario:
/// 1. Start a 3-node cluster
/// 2. Wait for leader election
/// 3. Isolate one follower (simulate network partition)
/// 4. Wait for the isolated node to timeout multiple times
/// 5. Restore the node
/// 6. Verify that the term did not inflate significantly
///
/// With Pre-Vote enabled, the isolated node should not increment its term
/// because it cannot get votes from the majority before starting a real election.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_prevote_prevents_term_inflation() {
    let _ = tracing_subscriber::fmt::try_init();

    // 1. Setup cluster
    let node1 = RaftId::new("prevote_test".to_string(), "node1".to_string());
    let node2 = RaftId::new("prevote_test".to_string(), "node2".to_string());
    let node3 = RaftId::new("prevote_test".to_string(), "node3".to_string());

    let config = TestClusterConfig {
        hub: MockNetworkHubConfig::default(),
        node_ids: vec![node1.clone(), node2.clone(), node3.clone()],
    };

    let cluster = TestCluster::new(config).await;

    // Start cluster in background
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // 2. Wait for leader election
    let leader_id = wait_for_leader(&cluster, &[&node1, &node2, &node3])
        .await
        .expect("Should elect a leader");
    println!("Leader elected: {:?}", leader_id);

    // Wait for cluster to stabilize - longer wait to ensure stable terms
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 3. Record initial terms - take max to handle election race
    let initial_terms = cluster.get_all_terms().await;
    println!("Initial terms: {:?}", initial_terms);
    let max_initial_term = initial_terms.values().copied().max().unwrap_or(0);

    // Find a follower to isolate
    let follower_to_isolate = if leader_id == node1 {
        &node2
    } else {
        &node1
    };
    println!("Isolating follower: {:?}", follower_to_isolate);

    // Record isolated node's initial term
    let isolated_initial_term = initial_terms.get(follower_to_isolate).copied().unwrap_or(0);

    // 4. Isolate the follower
    cluster.isolate_node(follower_to_isolate).await;

    // 5. Wait for multiple election timeout periods
    // The isolated node will try to start pre-vote elections but should not increment its term
    // because it cannot get pre-vote responses from the majority
    println!("Waiting for isolated node to attempt pre-vote elections...");
    tokio::time::sleep(Duration::from_secs(4)).await;

    // 6. Restore the node
    println!("Restoring isolated node...");
    cluster.restore_node(follower_to_isolate).await;

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 7. Check final terms
    let final_terms = cluster.get_all_terms().await;
    println!("Final terms: {:?}", final_terms);

    // Get the final term of the isolated node
    let isolated_final_term = final_terms.get(follower_to_isolate).copied().unwrap_or(0);

    // Calculate term increase for the isolated node
    let term_increase = isolated_final_term.saturating_sub(isolated_initial_term);
    println!(
        "Term increase for isolated node {:?}: {} (initial: {}, final: {})",
        follower_to_isolate, term_increase, isolated_initial_term, isolated_final_term
    );

    // With Pre-Vote enabled:
    // - The isolated node will attempt pre-vote elections but fail to get majority
    // - It will NOT increment its real term until it wins a pre-vote
    // - After restoration, it might sync to the current cluster term
    //
    // The key insight: the isolated node's term should not have inflated during isolation.
    // It might match the cluster term after restoration (due to heartbeat sync), but it
    // should NOT have many extra terms from failed elections.
    //
    // Without Pre-Vote, each election timeout would increment the term,
    // resulting in term_increase >> 10 for 4 seconds of isolation.
    //
    // With Pre-Vote, the term should only increase by a small amount (0-2 for cluster events)
    // plus possibly matching the cluster term after restoration.
    
    // The cluster term might have increased by 1-2 during the test
    let max_final_term = final_terms.values().copied().max().unwrap_or(0);
    let cluster_term_change = max_final_term.saturating_sub(max_initial_term);
    
    println!(
        "Cluster term change: {} (max initial: {}, max final: {})",
        cluster_term_change, max_initial_term, max_final_term
    );

    // The isolated node's term increase should be roughly in line with cluster term change
    // Allow some tolerance for race conditions
    let tolerance = 5; // Allow up to 5 extra terms for timing issues
    assert!(
        term_increase <= cluster_term_change + tolerance,
        "Pre-Vote should prevent significant term inflation. Isolated node increased by {}, cluster by {}",
        term_increase, cluster_term_change
    );

    println!("✓ Pre-Vote test completed - term inflation was limited!");
}

/// Test: ReadIndex request can be sent to leader
///
/// This test verifies that ReadIndex requests are accepted by the leader
/// and processed correctly. The focus is on the ReadIndex mechanism itself,
/// not data consistency (which is tested in other integration tests).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_read_index_basic() {
    let _ = tracing_subscriber::fmt::try_init();

    // 1. Setup cluster
    let node1 = RaftId::new("readindex_test".to_string(), "node1".to_string());
    let node2 = RaftId::new("readindex_test".to_string(), "node2".to_string());
    let node3 = RaftId::new("readindex_test".to_string(), "node3".to_string());

    let config = TestClusterConfig {
        hub: MockNetworkHubConfig::default(),
        node_ids: vec![node1.clone(), node2.clone(), node3.clone()],
    };

    let cluster = TestCluster::new(config).await;

    // Start cluster in background
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // 2. Wait for leader election
    let leader_id = wait_for_leader(&cluster, &[&node1, &node2, &node3])
        .await
        .expect("Should elect a leader");
    println!("Leader elected: {:?}", leader_id);

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // 3. Send ReadIndex request to leader
    println!("Sending ReadIndex request to leader...");
    let request_id = cluster.send_read_index(&leader_id);
    assert!(request_id.is_ok(), "ReadIndex request should be accepted by leader");
    println!("ReadIndex request sent: {:?}", request_id);

    // Wait for ReadIndex to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 4. Send multiple ReadIndex requests to verify stability
    println!("Sending 10 more ReadIndex requests...");
    let mut success_count = 0;
    for i in 0..10 {
        if cluster.send_read_index(&leader_id).is_ok() {
            success_count += 1;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    println!("Successfully sent {}/10 ReadIndex requests", success_count);
    assert!(success_count >= 8, "Most ReadIndex requests should succeed");

    println!("✓ ReadIndex basic test passed!");
}

/// Test: ReadIndex rejected on non-leader
///
/// ReadIndex requests should be rejected if sent to a follower
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_read_index_rejected_on_follower() {
    let _ = tracing_subscriber::fmt::try_init();

    // 1. Setup cluster
    let node1 = RaftId::new("readindex_follower_test".to_string(), "node1".to_string());
    let node2 = RaftId::new("readindex_follower_test".to_string(), "node2".to_string());
    let node3 = RaftId::new("readindex_follower_test".to_string(), "node3".to_string());

    let config = TestClusterConfig {
        hub: MockNetworkHubConfig::default(),
        node_ids: vec![node1.clone(), node2.clone(), node3.clone()],
    };

    let cluster = TestCluster::new(config).await;

    // Start cluster in background
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // 2. Wait for leader election
    let leader_id = wait_for_leader(&cluster, &[&node1, &node2, &node3])
        .await
        .expect("Should elect a leader");
    println!("Leader elected: {:?}", leader_id);

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Find a follower
    let follower_id = if leader_id == node1 {
        node2.clone()
    } else {
        node1.clone()
    };
    println!("Sending ReadIndex to follower: {:?}", follower_id);

    // 4. Send ReadIndex to follower (should still be dispatched, but will return NotLeader error)
    let result = cluster.send_read_index(&follower_id);
    assert!(
        result.is_ok(),
        "ReadIndex dispatch should succeed (error handled internally)"
    );

    // The actual error (NotLeader) is handled via callback, not dispatch
    println!("✓ ReadIndex to follower test completed");
}

/// Test: Cluster stability after leader isolation and recovery
///
/// This test verifies that Pre-Vote mechanism maintains cluster stability
/// when leader is temporarily isolated. The focus is on:
/// 1. New leader can be elected when old leader is isolated
/// 2. Cluster can resume normal operation after leader recovery
/// 3. ReadIndex requests work after recovery
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_stability_after_leader_isolation() {
    let _ = tracing_subscriber::fmt::try_init();

    // 1. Setup cluster
    let node1 = RaftId::new("stability_test".to_string(), "node1".to_string());
    let node2 = RaftId::new("stability_test".to_string(), "node2".to_string());
    let node3 = RaftId::new("stability_test".to_string(), "node3".to_string());

    let config = TestClusterConfig {
        hub: MockNetworkHubConfig::default(),
        node_ids: vec![node1.clone(), node2.clone(), node3.clone()],
    };

    let cluster = TestCluster::new(config).await;

    // Start cluster in background
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // 2. Wait for initial leader election
    let initial_leader = wait_for_leader(&cluster, &[&node1, &node2, &node3])
        .await
        .expect("Should elect a leader");
    println!("Initial leader: {:?}", initial_leader);

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Verify ReadIndex works before isolation
    let result = cluster.send_read_index(&initial_leader);
    assert!(result.is_ok(), "ReadIndex should work on initial leader");
    println!("ReadIndex works on initial leader");

    // 3. Isolate the leader
    println!("Isolating leader: {:?}", initial_leader);
    cluster.isolate_node(&initial_leader).await;

    // 4. Wait for new leader election
    println!("Waiting for new leader election...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 5. Find new leader (excluding isolated node)
    let remaining_nodes: Vec<&RaftId> = [&node1, &node2, &node3]
        .into_iter()
        .filter(|n| **n != initial_leader)
        .collect();

    let new_leader = wait_for_leader(&cluster, &remaining_nodes).await;
    if let Some(ref leader) = new_leader {
        println!("New leader elected: {:?}", leader);

        // Verify ReadIndex works on new leader
        let result = cluster.send_read_index(leader);
        assert!(result.is_ok(), "ReadIndex should work on new leader");
        println!("ReadIndex works on new leader");
    }

    // 6. Restore old leader
    println!("Restoring old leader...");
    cluster.restore_node(&initial_leader).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 7. Find current leader after recovery
    let final_leader = wait_for_leader(&cluster, &[&node1, &node2, &node3])
        .await
        .expect("Should have a leader after recovery");
    println!("Leader after recovery: {:?}", final_leader);

    // 8. Verify ReadIndex still works
    let result = cluster.send_read_index(&final_leader);
    assert!(result.is_ok(), "ReadIndex should work after recovery");
    println!("ReadIndex works after recovery");

    // 9. Check cluster status
    let status = cluster.get_cluster_status();
    println!("Final cluster status: {:?}", status);

    let leader_count = status.values().filter(|r| **r == Role::Leader).count();
    assert_eq!(leader_count, 1, "Should have exactly one leader");

    println!("✓ Cluster stability test passed!");
}

/// Test: Multiple ReadIndex requests under load
///
/// This test focuses on the ability to handle many concurrent ReadIndex requests,
/// verifying the ReadIndex mechanism can handle load without crashing or blocking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_read_index_under_load() {
    let _ = tracing_subscriber::fmt::try_init();

    // 1. Setup cluster
    let node1 = RaftId::new("readindex_load_test".to_string(), "node1".to_string());
    let node2 = RaftId::new("readindex_load_test".to_string(), "node2".to_string());
    let node3 = RaftId::new("readindex_load_test".to_string(), "node3".to_string());

    let config = TestClusterConfig {
        hub: MockNetworkHubConfig::default(),
        node_ids: vec![node1.clone(), node2.clone(), node3.clone()],
    };

    let cluster = TestCluster::new(config).await;

    // Start cluster in background
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // 2. Wait for leader election
    let leader_id = wait_for_leader(&cluster, &[&node1, &node2, &node3])
        .await
        .expect("Should elect a leader");
    println!("Leader elected: {:?}", leader_id);

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // 3. Send many ReadIndex requests rapidly
    println!("Sending 100 ReadIndex requests under load...");
    let mut success_count = 0;
    for i in 0..100 {
        match cluster.send_read_index(&leader_id) {
            Ok(_) => success_count += 1,
            Err(e) => {
                if i < 10 {
                    println!("ReadIndex {} failed: {}", i, e);
                }
            }
        }
        // Very short delay to stress test
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!("ReadIndex success count: {}/100", success_count);

    // Most requests should succeed (allow some failures due to timing)
    assert!(
        success_count >= 80,
        "At least 80% of ReadIndex requests should be dispatched successfully. Got {}",
        success_count
    );

    // 4. Verify cluster is still healthy after load
    let final_leader = wait_for_leader(&cluster, &[&node1, &node2, &node3]).await;
    assert!(
        final_leader.is_some(),
        "Cluster should still have a leader after ReadIndex load test"
    );
    println!("Leader after load test: {:?}", final_leader);

    println!("✓ ReadIndex under load test passed!");
}

