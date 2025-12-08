#[path = "mock/mod.rs"]
pub mod mock;

use mock::mock_network::MockNetworkHubConfig;
use raft::RaftId;
use std::time::Duration;
use tokio;

mod common;
use common::test_cluster::{TestCluster, TestClusterConfig};
use common::test_statemachine::KvCommand;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_config_application() {
    tracing_subscriber::fmt().init();

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

    // Wait for Leader election
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Wait for stable leader
    let leader_id = cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("Should have a leader");

    println!("✓ Leader election successful, leader: {:?}", leader_id);

    // Write some data
    println!("\n=== Writing test data ===");
    for i in 1..=50 {
        let command = KvCommand::Set {
            key: format!("test_key_{}", i),
            value: format!("test_value_{}", i),
        };


        cluster
            .propose_command(&leader_id, &command)
            .expect("Should be able to propose command");
    }

    // Wait for data replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify data consistency
    cluster
        .verify_data_consistency()
        .await
        .expect("Data should be consistent before snapshot");
    println!("✓ Data consistency verified before snapshot");

    // Trigger snapshot
    println!("\n=== Triggering snapshot ===");
    cluster
        .trigger_snapshot(&leader_id)
        .expect("Should be able to trigger snapshot");

    // Wait for snapshot to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("✓ Snapshot creation completed");

    // Verify data consistency after snapshot
    cluster
        .verify_data_consistency()
        .await
        .expect("Data should be consistent after snapshot");
    println!("✓ Data consistency verified after snapshot");

    // Add a new learner node to test snapshot config application
    println!("\n=== Testing snapshot config application with learner ===");
    let learner_id = RaftId::new("test_group".to_string(), "learner1".to_string());

    cluster
        .add_learner(learner_id.clone())
        .await
        .expect("Should be able to add learner");
    println!("✓ Added learner: {:?}", learner_id);

    // Wait for learner to sync data via snapshot
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify learner's data matches cluster
    if let Some(learner_data) = cluster.get_node_data(&learner_id) {
        if let Some(reference_data) = cluster.get_node_data(&leader_id) {
            assert_eq!(
                learner_data.len(),
                reference_data.len(),
                "Learner should have same data count as cluster"
            );
            println!(
                "✓ Learner synced {} entries via snapshot",
                learner_data.len()
            );
        }
    }

    println!("\n=== Snapshot config application test completed successfully ===");
}
