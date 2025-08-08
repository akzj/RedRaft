// test_cluster.rs
use crate::common::test_node::TestNode;
use raft_lite::RaftId;
use raft_lite::mock::mock_network::MockNetworkHub;
use raft_lite::mock::mock_network::MockNetworkHubConfig;
use raft_lite::mock::mock_network::MockRaftNetworkConfig;
use raft_lite::mock::mock_storage::SnapshotStorage;
use raft_lite::mutl_raft_driver::MultiRaftDriver;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::{info, warn};

#[derive(Clone)]
pub struct TestClusterConfig {
    pub node_ids: Vec<RaftId>,
    pub hub: MockNetworkHubConfig,
}

#[derive(Clone)]
pub struct TestCluster {
    driver: MultiRaftDriver,
    config: TestClusterConfig,
    hub: MockNetworkHub,
    snapshot_storage: SnapshotStorage,
    network_config: Arc<Mutex<HashMap<RaftId, MockRaftNetworkConfig>>>,
    nodes: Arc<Mutex<HashMap<RaftId, TestNode>>>,
}

impl TestCluster {
    pub async fn new(config: TestClusterConfig) -> Self {
        let hub = MockNetworkHub::new(config.hub.clone());
        let cluster = TestCluster {
            snapshot_storage: SnapshotStorage::new(),
            driver: MultiRaftDriver::new(),
            config,
            hub,
            network_config: Arc::new(Mutex::new(HashMap::new())),
            nodes: Arc::new(Mutex::new(HashMap::new())),
        };

        // 创建所有节点
        let all_node_ids: Vec<RaftId> = cluster.config.node_ids.clone();
        for node_id in &cluster.config.node_ids {
            // 初始 peers 是集群中除自己外的所有节点
            let initial_peers: Vec<RaftId> = all_node_ids
                .iter()
                .filter(|&id| id != node_id)
                .cloned()
                .collect();

            match TestNode::new(
                node_id.clone(),
                cluster.hub.clone(),
                cluster.driver.get_timer_service(),
                cluster.snapshot_storage.clone(),
                cluster.driver.clone(),
                initial_peers,
            )
            .await
            {
                Ok(node) => {
                    cluster
                        .nodes
                        .lock()
                        .unwrap()
                        .insert(node_id.clone(), node.clone());
                    cluster
                        .driver
                        .add_raft_group(node_id.clone(), Box::new(node));
                }
                Err(e) => {
                    panic!("Failed to create node {:?}: {}", node_id, e);
                }
            }
        }

        info!(
            "TestCluster created with {} nodes",
            cluster.nodes.lock().unwrap().len()
        );
        cluster
    }

    // 启动集群（例如，触发初始选举）
    pub async fn start(&self) {
        info!("Starting TestCluster...");
        self.driver.main_loop().await
    }

    // 获取节点
    pub fn get_node(&self, id: &RaftId) -> Option<TestNode> {
        let nodes = self.nodes.lock().unwrap();
        nodes.get(id).cloned()
    }

    // 获取节点（可变）
    pub fn get_node_mut(&self, id: &RaftId) -> Option<TestNode> {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.get_mut(id).cloned()
    }

    // 停止节点（模拟故障）
    pub async fn stop_node(&mut self, id: &RaftId) -> bool {
        if self.nodes.lock().unwrap().remove(id).is_some() {
            info!("Stopped node {:?}", id);
            // 可能还需要通知网络层该节点已失效
            // hub.unregister_node(id) ? (如果 MockNetworkHub 有此方法)
            true
        } else {
            warn!("Attempted to stop non-existent node {:?}", id);
            false
        }
    }

    // 重启节点（模拟恢复）
    // pub async fn restart_node(&mut self, id: &RaftId) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //     // 需要重新创建 TestNode
    //     // ...
    // }

    // 更新网络配置
    pub async fn update_network_config_for_node(
        &self,
        node_id: &RaftId,
        new_config: MockRaftNetworkConfig,
    ) {
        self.hub.update_config(node_id.clone(), new_config).await;
    }

    // 等待选举完成并找到 Leader
    // pub async fn wait_for_leader(&self, timeout_duration: std::time::Duration) -> Option<RaftId> {
    //     // 实现一个轮询机制，检查各节点的角色
    //     // 或者，TestNode 可以在角色变更时通知 TestCluster (通过通道)
    // }
}
