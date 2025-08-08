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

    // 3. 启动集群在后台
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // 4. 等待选举完成，寻找 Leader
    println!("Waiting for leader election to complete...");
    let leader_node = wait_for_leader(&cluster, &[&node1, &node2, &node3]).await
        .expect("Should have elected a leader within timeout");
    println!("Found leader: {:?}", leader_node.id);

    // 5. 执行业务操作 (SET) 仅当 leader 确认是 Leader
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

// Helper function to wait for leader election
async fn wait_for_leader(cluster: &TestCluster, node_ids: &[&RaftId]) -> Option<crate::common::test_node::TestNode> {
    let timeout = tokio::time::Duration::from_secs(10);
    let start_time = tokio::time::Instant::now();
    
    while start_time.elapsed() < timeout {
        println!("Checking nodes for leader at {:?}...", start_time.elapsed());
        for node_id in node_ids {
            if let Some(node) = cluster.get_node(node_id) {
                let role = node.get_role();
                println!("Node {:?} role: {:?}", node_id, role);
                if role == raft_lite::Role::Leader {
                    return Some(node);
                }
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    println!("Timeout waiting for leader election");
    None
}
