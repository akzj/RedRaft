// integration_tests.rs
// 假设这个文件在 tests/ 目录下，或者在 src/bin/ 下作为示例

pub mod common;

use raft_lite::mock::mock_network::MockNetworkHubConfig;
use raft_lite::{RaftId, RequestId};
use tracing_subscriber;

use crate::common::test_cluster::{TestCluster, TestClusterConfig};
use crate::common::test_statemachine::KvCommand;

#[tokio::test]
async fn test_basic_raft_kv_cluster() {
    let _ = tracing_subscriber::fmt::try_init(); // 初始化日志

    // 1. 定义集群配置
    let node1 = RaftId::new("test_group".to_string(), "node1".to_string());
    let node2 = RaftId::new("test_group".to_string(), "node2".to_string());
    let node3 = RaftId::new("test_group".to_string(), "node3".to_string());

    let config = TestClusterConfig {
        hub: MockNetworkHubConfig::default(),
        node_ids: vec![node1.clone(), node2.clone(), node3.clone()],
    };

    // 2. 创建并启动集群
    let cluster = TestCluster::new(config).await;

    let cluster_clone = cluster.clone();

    tokio::spawn(async move { cluster_clone.start().await });

    // 3. 等待集群稳定，选出 Leader
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await; // 等待选举完成
    println!("Waiting for leader election to complete...");

    // 4. 找到 Leader
    let leader_node = find_leader(&cluster, &[&node1, &node2, &node3]).await
        .expect("Should have elected a leader");
    println!("Found leader: {:?}", leader_node.id);

    // 5. 执行业务操作 (SET)
    let set_cmd = KvCommand::Set { 
        key: "key1".to_string(), 
        value: "value1".to_string() 
    };
    let set_cmd_data = set_cmd.encode();
    let request_id = RequestId::new();
    
    leader_node.handle_event(raft_lite::Event::ClientPropose { 
        cmd: set_cmd_data, 
        request_id 
    }).await;
    println!("Sent SET command for key1=value1");

    // 6. 等待命令被提交和应用
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    println!("Waited for command to be applied");

    // 7. 验证 Leader 状态
    let leader_value = leader_node.get_value("key1");
    assert_eq!(leader_value, Some("value1".to_string()));
    println!("✓ Verified SET on leader");

    // 8. 验证 Follower 状态机 (等待日志复制)
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    // 检查所有节点的状态一致性
    for node_id in &[&node1, &node2, &node3] {
        if let Some(node) = cluster.get_node(node_id) {
            let value = node.get_value("key1");
            assert_eq!(value, Some("value1".to_string()), 
                      "Node {:?} should have key1=value1", node_id);
            println!("✓ Verified consistency on node {:?}", node_id);
        }
    }

    // 9. 测试另一个命令
    let set_cmd2 = KvCommand::Set { 
        key: "key2".to_string(), 
        value: "value2".to_string() 
    };
    leader_node.handle_event(raft_lite::Event::ClientPropose { 
        cmd: set_cmd2.encode(), 
        request_id: RequestId::new() 
    }).await;
    
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    
    // 验证第二个命令
    for node_id in &[&node1, &node2, &node3] {
        if let Some(node) = cluster.get_node(node_id) {
            let value = node.get_value("key2");
            assert_eq!(value, Some("value2".to_string()));
        }
    }

    println!("✓ Basic Raft KV cluster test completed successfully!");
    // 集群会在 drop 时自动清理
}

// Helper function to find the leader node
async fn find_leader(cluster: &TestCluster, node_ids: &[&RaftId]) -> Option<crate::common::test_node::TestNode> {
    // Try to find leader by checking node roles
    // For now, we'll just return the first available node as a placeholder
    // In a real implementation, you'd check the Role of each node's RaftState
    for node_id in node_ids {
        if let Some(node) = cluster.get_node(node_id) {
            // TODO: Add method to check if node is leader
            // if node.is_leader() { return Some(node); }
            return Some(node); // Return first node for now
        }
    }
    None
}
