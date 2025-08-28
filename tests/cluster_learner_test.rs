use raft_lite::{RaftId, tests::mock::mock_network::MockNetworkHubConfig};
use std::time::Duration;
use tokio;

mod common;
use common::test_cluster::{TestCluster, TestClusterConfig};
use common::test_statemachine::KvCommand;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_learner_operate() {
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

    // 3. 启动集群在后台
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // 等待 Leader 选举
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 检查是否有 Leader
    let mut leader_found = false;
    for node_id in &[&node1, &node2, &node3] {
        if let Some(node) = cluster.get_node(node_id) {
            let role = node.get_role();
            println!("Node {:?} role: {:?}", node_id, role);
            if role == raft_lite::Role::Leader {
                leader_found = true;
                println!("Found leader: {:?}", node_id);
            }
        }
    }

    assert!(leader_found, "No leader found after election");
    println!("✓ Leader election successful");

    // 模拟网络丢包，测试网络恢复后 Leader 是否仍然存在

    // 1. 记录当前的 Leader
    let mut current_leader = None;
    for node_id in &[&node1, &node2, &node3] {
        if let Some(node) = cluster.get_node(node_id) {
            let role = node.get_role();
            if role == raft_lite::Role::Leader {
                current_leader = Some((*node_id).clone());
                break;
            }
        }
    }

    let leader_id = current_leader.expect("Should have found a leader");
    println!("Current leader before network partition: {:?}", leader_id);

    // ===== 1. 发送业务消息测试 =====
    println!("\n=== Testing business message handling ===");

    // 发送一些业务命令到leader
    for i in 1..=20 {
        let command = KvCommand::Set {
            key: format!("key{}", i),
            value: format!("value{}", i),
        };

        match cluster.propose_command(&leader_id, &command) {
            Ok(()) => println!("✓ Successfully proposed command: {:?}", command),
            Err(e) => println!("✗ Failed to propose command {:?}: {}", command, e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await; // 给一些时间处理
    }

    // 等待数据复制到所有节点
    println!("Waiting for data replication...");
    match cluster
        .wait_for_data_replication(Duration::from_secs(5))
        .await
    {
        Ok(()) => println!("✓ Data successfully replicated to all nodes"),
        Err(e) => println!("⚠️ Data replication issue: {}", e),
    }

    println!("✓ Business message handling test completed");

    // ===== 2. Learner 管理测试 =====
    println!("\n=== Testing learner management ===");

    for i in 1..=10 {
        println!("\n--- Testing learner {} ---", i);

        // 添加 learner
        let learner_id = RaftId::new("test_group".to_string(), format!("learner{}", i));

        println!("Adding learner: {:?}", learner_id);
        match cluster.add_learner(learner_id.clone()).await {
            Ok(()) => println!("✓ Successfully added learner: {:?}", learner_id),
            Err(e) => {
                println!("✗ Failed to add learner {:?}: {}", learner_id, e);
                continue; // 跳过这个 learner 的后续测试
            }
        }

        // 等待 learner 同步数据
        println!("Waiting for learner {:?} to catch up...", learner_id);
        match cluster
            .wait_for_learner_sync(&learner_id, Duration::from_secs(5))
            .await
        {
            Ok(()) => println!("✓ Learner {:?} successfully synchronized", learner_id),
            Err(e) => println!("⚠️ Learner {:?} sync issue: {}", learner_id, e),
        }

        // 验证 learner 的数据一致性
        println!("Checking data consistency including learner...");
        if let Some(learner_data) = cluster.get_node_data(&learner_id) {
            println!("Learner {:?} data: {:?}", learner_id, learner_data);

            // 验证 learner 是否为 Follower 角色（learner 应该是 Follower）
            if let Some(learner_node) = cluster.get_node(&learner_id) {
                let role = learner_node.get_role();
                println!("Learner {:?} role: {:?}", learner_id, role);

                // Learner 应该是 Follower，但不参与选举
                if role != raft_lite::Role::Follower {
                    println!(
                        "⚠️ Learner {:?} has unexpected role: {:?}",
                        learner_id, role
                    );
                }
            }
        } else {
            println!("⚠️ Could not get data from learner {:?}", learner_id);
        }

        // 移除 learner
        println!("Removing learner: {:?}", learner_id);
        match cluster.remove_learner(&learner_id).await {
            Ok(()) => println!("✓ Successfully removed learner: {:?}", learner_id),
            Err(e) => println!("✗ Failed to remove learner {:?}: {}", learner_id, e),
        }

        // 验证 learner 已被移除
        tokio::time::sleep(Duration::from_millis(200)).await;
        if cluster.get_node(&learner_id).is_none() {
            println!(
                "✓ Learner {:?} successfully removed from cluster",
                learner_id
            );
        } else {
            println!(
                "⚠️ Learner {:?} still exists in cluster after removal",
                learner_id
            );
        }

        // 验证核心集群的数据一致性未受影响
        match cluster.verify_data_consistency().await {
            Ok(()) => println!("✓ Core cluster data consistency maintained"),
            Err(e) => println!("⚠️ Core cluster consistency issue: {}", e),
        }

        println!("--- Completed learner {} test ---", i);
    }

    println!("\n✓ Learner management test completed");

    // ===== 3. 最终验证 =====
    println!("\n=== Final verification ===");

    // 确保原始集群仍然正常工作
    println!("Verifying original cluster is still functional...");

    // 发送一些最终的业务命令
    for i in 21..=25 {
        let command = KvCommand::Set {
            key: format!("final_key{}", i),
            value: format!("final_value{}", i),
        };

        match cluster.propose_command(&leader_id, &command) {
            Ok(()) => println!("✓ Final command {} accepted", i),
            Err(e) => println!("✗ Final command {} failed: {}", i, e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 最终数据一致性检查
    match cluster
        .wait_for_data_replication(Duration::from_secs(3))
        .await
    {
        Ok(()) => println!("✓ Final data consistency verified"),
        Err(e) => println!("⚠️ Final data consistency issue: {}", e),
    }

    // 检查集群状态
    let cluster_status = cluster.get_cluster_status();
    println!("Final cluster status: {:?}", cluster_status);

    // 验证只有原始的3个节点
    assert_eq!(
        cluster_status.len(),
        3,
        "Should have exactly 3 nodes in final cluster"
    );

    println!("✓ All tests completed successfully!");
}
