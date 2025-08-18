use raft_lite::{RaftId, tests::mock::mock_network::MockNetworkHubConfig};
use std::time::Duration;
use tokio;

mod common;
use common::test_cluster::{TestCluster, TestClusterConfig};
use common::test_statemachine::KvCommand;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_snapshot_and_learner_sync() {
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

    // ===== 1. 多轮数据写入和快照测试 =====
    println!("\n=== Testing multi-round data writing and snapshot ===");

    for s in 1..=5 {
        println!("\n--- Round {} ---", s);
        
        // 写入200条数据
        for i in 1..=200 {
            let command = KvCommand::Set {
                key: format!("key{}-{}", s, i),
                value: format!("value{}-{}", s, i),
            };
            let command_bytes = command.encode();

            match cluster.propose_command(&leader_id, command_bytes) {
                Ok(()) => {
                    if i % 50 == 0 {
                        println!("✓ Successfully proposed command batch {}/200", i);
                    }
                }
                Err(e) => println!("✗ Failed to propose command: {}", e),
            }
            
            // 小延迟避免过度发送
            if i % 10 == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        // 等待数据复制
        println!("Waiting for data replication...");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // 验证数据一致性
        if let Err(e) = cluster.verify_data_consistency().await {
            println!("✗ Data consistency check failed: {}", e);
        } else {
            println!("✓ Data consistency verified after round {}", s);
        }

        // 触发快照
        let current_leaders = cluster.get_current_leader().await;
        assert_eq!(current_leaders.len(), 1);
        let current_leader = current_leaders.first().unwrap();
        
        println!("Triggering snapshot on leader {:?}", current_leader);
        cluster
            .trigger_snapshot(current_leader)
            .unwrap();

        // 等待快照完成
        tokio::time::sleep(Duration::from_secs(3)).await;

        // 检查快照后数据一致性
        if let Err(e) = cluster.verify_data_consistency().await {
            println!("✗ Data consistency check failed after snapshot: {}", e);
        } else {
            println!("✓ Data consistency verified after snapshot in round {}", s);
        }

        // 每第3轮添加learner测试通过快照同步数据
        if s == 3 {
            println!("\n=== Testing learner sync via snapshot ===");
            
            // 创建learner节点
            let learner_id = RaftId::new("test_group".to_string(), format!("learner{}", s));
            
            println!("Adding learner {:?}", learner_id);
            match cluster.add_learner(learner_id.clone()).await {
                Ok(()) => {
                    println!("✓ Successfully added learner {:?}", learner_id);
                    
                    // 等待learner通过快照同步数据
                    println!("Waiting for learner to sync via snapshot...");
                    match cluster.wait_for_learner_sync(&learner_id, Duration::from_secs(10)).await {
                        Ok(()) => {
                            println!("✓ Learner {:?} successfully synced data via snapshot", learner_id);
                        }
                        Err(e) => {
                            println!("✗ Learner sync failed: {}", e);
                        }
                    }

                    // 验证learner的数据与集群一致
                    if let Some(learner_data) = cluster.get_node_data(&learner_id) {
                        if let Some(reference_data) = cluster.get_node_data(&leader_id) {
                            if learner_data == reference_data {
                                println!("✓ Learner data matches cluster data (verified {} entries)", learner_data.len());
                            } else {
                                println!("✗ Learner data mismatch! Learner: {}, Reference: {}", 
                                    learner_data.len(), reference_data.len());
                            }
                        }
                    }

                    // 移除learner
                    println!("Removing learner {:?}", learner_id);
                    match cluster.remove_learner(&learner_id).await {
                        Ok(()) => println!("✓ Successfully removed learner"),
                        Err(e) => println!("✗ Failed to remove learner: {}", e),
                    }
                }
                Err(e) => {
                    println!("✗ Failed to add learner: {}", e);
                }
            }
        }
    }

    // ===== 2. 最终数据一致性验证 =====
    println!("\n=== Final data consistency verification ===");
    
    // 等待最终同步
    match cluster.wait_for_data_replication(Duration::from_secs(5)).await {
        Ok(()) => {
            println!("✓ Final data replication completed");
            
            // 获取最终数据统计
            if let Some(final_data) = cluster.get_node_data(&leader_id) {
                println!("✓ Final cluster state: {} key-value pairs", final_data.len());
                
                // 验证数据完整性 - 应该有 5 * 200 = 1000 条记录
                let expected_count = 5 * 200;
                if final_data.len() == expected_count {
                    println!("✓ Data integrity verified: {} entries as expected", expected_count);
                } else {
                    println!("✗ Data integrity check failed: expected {}, got {}", expected_count, final_data.len());
                }
            }
        }
        Err(e) => {
            println!("✗ Final data replication failed: {}", e);
        }
    }

    println!("\n=== Snapshot test completed ===");
}
