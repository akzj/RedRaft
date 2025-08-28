use raft_lite::{RaftId, tests::mock::mock_network::MockNetworkHubConfig};
use std::time::Duration;
use tokio;

mod common;
use common::test_cluster::{TestCluster, TestClusterConfig};
use common::test_statemachine::KvCommand;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_config_application() {
    tracing_subscriber::fmt().init();

    // 创建 3 节点集群
    let node1 = RaftId::new("test_group".to_string(), "node1".to_string());
    let node2 = RaftId::new("test_group".to_string(), "node2".to_string());
    let node3 = RaftId::new("test_group".to_string(), "node3".to_string());

    let config = TestClusterConfig {
        node_ids: vec![node1.clone(), node2.clone(), node3.clone()],
        hub: MockNetworkHubConfig::default(),
    };
    let cluster = TestCluster::new(config).await;

    // 启动集群在后台
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // 等待 Leader 选举
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 等待稳定的leader
    let leader_id = cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("Should have a leader");

    println!("✓ Leader election successful, leader: {:?}", leader_id);

    // 写入一些数据
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

    // 等待数据复制
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 验证数据一致性
    cluster
        .verify_data_consistency()
        .await
        .expect("Data should be consistent before snapshot");
    println!("✓ Data consistency verified before snapshot");

    // 触发快照
    println!("\n=== Triggering snapshot ===");
    cluster
        .trigger_snapshot(&leader_id)
        .expect("Should be able to trigger snapshot");

    // 等待快照完成
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("✓ Snapshot creation completed");

    // 验证快照后数据一致性
    cluster
        .verify_data_consistency()
        .await
        .expect("Data should be consistent after snapshot");
    println!("✓ Data consistency verified after snapshot");

    // 添加一个新的learner节点来测试快照配置应用
    println!("\n=== Testing snapshot config application with learner ===");
    let learner_id = RaftId::new("test_group".to_string(), "learner1".to_string());

    cluster
        .add_learner(learner_id.clone())
        .await
        .expect("Should be able to add learner");
    println!("✓ Added learner: {:?}", learner_id);

    // 等待learner通过快照同步数据
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 验证learner的数据与集群一致
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
