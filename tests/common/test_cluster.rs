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
use std::time::Duration;
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

    // isolate_node
    pub async fn isolate_node(&self, id: &RaftId) {
        info!("Isolating node {:?}", id);
        if let Some(node) = self.get_node(id) {
            node.isolate().await;
        } else {
            warn!("Node {:?} not found for isolation", id);
        }
    }

    //restore_node
    pub async fn restore_node(&self, id: &RaftId) {
        info!("Restoring node {:?}", id);
        if let Some(node) = self.get_node(id) {
            node.restore().await;
        } else {
            warn!("Node {:?} not found for restoration", id);
        }
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

    // 更新网络配置
    pub async fn update_network_config_for_node(
        &self,
        node_id: &RaftId,
        new_config: MockRaftNetworkConfig,
    ) {
        self.hub.update_config(node_id.clone(), new_config).await;
    }

    // 发送业务命令
    pub async fn propose_command(&self, leader_id: &RaftId, command: Vec<u8>) -> Result<(), String> {
        if let Some(node) = self.get_node(leader_id) {
            let request_id = raft_lite::RequestId::new();
            let event = raft_lite::Event::ClientPropose {
                cmd: command,
                request_id,
            };
            node.handle_event(event).await;
            Ok(())
        } else {
            Err(format!("Node {:?} not found", leader_id))
        }
    }

    // 获取当前leader
    pub async fn get_current_leader(&self) -> Vec<RaftId> {
        let mut leaders = Vec::new();
        let nodes = self.nodes.lock().unwrap();
        for node in nodes.values() {
            if node.get_role() == raft_lite::Role::Leader {
                leaders.push(node.id.clone());
            }
        }
        leaders
    }

    // 添加新节点到集群
    pub async fn add_node(&self, new_node_id: &RaftId) -> Result<(), String> {
        info!("Adding new node {:?} to cluster", new_node_id);
        
        // 获取当前所有节点ID（包括新节点）
        let all_node_ids: Vec<RaftId> = {
            let nodes = self.nodes.lock().unwrap();
            let mut ids: Vec<RaftId> = nodes.keys().cloned().collect();
            ids.push(new_node_id.clone());
            ids
        };
        
        // 新节点的初始peers是集群中除自己外的所有节点
        let initial_peers: Vec<RaftId> = all_node_ids
            .iter()
            .filter(|&id| id != new_node_id)
            .cloned()
            .collect();

        // 创建新节点
        let new_node = TestNode::new(
            new_node_id.clone(),
            self.hub.clone(),
            self.driver.get_timer_service(),
            self.snapshot_storage.clone(),
            self.driver.clone(),
            initial_peers,
        )
        .await
        .map_err(|e| format!("Failed to create new node: {}", e))?;

        // 将新节点添加到集群
        self.nodes.lock().unwrap().insert(new_node_id.clone(), new_node.clone());
        self.driver.add_raft_group(new_node_id.clone(), Box::new(new_node));

        info!("Successfully added node {:?} to cluster", new_node_id);
        Ok(())
    }

    // 移除节点
    pub async fn remove_node(&self, node_id: &RaftId) -> Result<(), String> {
        info!("Removing node {:?} from cluster", node_id);
        
        if self.nodes.lock().unwrap().remove(node_id).is_some() {
            // TODO: 这里可能需要通知其他节点该节点已被移除
            // 这通常需要通过集群配置变更来实现
            info!("Successfully removed node {:?} from cluster", node_id);
            Ok(())
        } else {
            Err(format!("Node {:?} not found in cluster", node_id))
        }
    }

    // 获取所有节点的状态
    pub fn get_cluster_status(&self) -> HashMap<RaftId, raft_lite::Role> {
        let nodes = self.nodes.lock().unwrap();
        nodes.iter().map(|(id, node)| (id.clone(), node.get_role())).collect()
    }

    // 等待集群稳定（有一个leader）
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<RaftId, String> {
        let start = std::time::Instant::now();
        
        while start.elapsed() < timeout {
            let leaders = self.get_current_leader().await;
            if leaders.len() == 1 {
                return Ok(leaders[0].clone());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        
        Err("Timeout waiting for leader election".to_string())
    }
}
