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
use tokio::time::Timeout;
use tokio::time::timeout;
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
    pub fn propose_command(&self, leader_id: &RaftId, command: Vec<u8>) -> Result<(), String> {
        let request_id = raft_lite::RequestId::new();
        let event = raft_lite::Event::ClientPropose {
            cmd: command,
            request_id,
        };
        // if let Some(node) = self.get_node(leader_id) {
        //     node.handle_event(event).await;
        //     Ok(())
        // } else {
        //     Err(format!("Node {:?} not found", leader_id))
        // }

        match self.driver.send_event(leader_id.clone(), event) {
            raft_lite::mutl_raft_driver::SendEventResult::Success => {
                return Ok(());
            }
            raft_lite::mutl_raft_driver::SendEventResult::NotFound => {
                warn!("Node {:?} not found for command proposal", leader_id);
                return Err(format!("Node {:?} not found", leader_id));
            }
            raft_lite::mutl_raft_driver::SendEventResult::SendFailed => {
                warn!("Failed to send event to node {:?}", leader_id);
                return Err(format!("Failed to send event to node {:?}", leader_id));
            }
        }
    }

    // 发送业务命令
    pub fn trigger_snapshot(&self, leader_id: &RaftId) -> Result<(), String> {
        let request_id = raft_lite::RequestId::new();
        let event = raft_lite::Event::CreateSnapshot {};

        match self.driver.send_event(leader_id.clone(), event) {
            raft_lite::mutl_raft_driver::SendEventResult::Success => {
                return Ok(());
            }
            raft_lite::mutl_raft_driver::SendEventResult::NotFound => {
                warn!("Node {:?} not found for command proposal", leader_id);
                return Err(format!("Node {:?} not found", leader_id));
            }
            raft_lite::mutl_raft_driver::SendEventResult::SendFailed => {
                warn!("Failed to send event to node {:?}", leader_id);
                return Err(format!("Failed to send event to node {:?}", leader_id));
            }
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

    // 添加新节点到集群 - 使用 Raft 配置变更
    pub async fn add_node(&self, new_node_id: &RaftId) -> Result<(), String> {
        info!(
            "Adding new node {:?} to cluster via Raft config change",
            new_node_id
        );

        // 1. 首先获取当前 leader
        let leader_id = self
            .wait_for_leader(Duration::from_secs(5))
            .await
            .map_err(|e| format!("No leader available for config change: {}", e))?;

        // 2. 准备新的配置（当前所有节点 + 新节点）
        let mut new_voters: std::collections::HashSet<RaftId> = {
            let nodes = self.nodes.lock().unwrap();
            nodes.keys().cloned().collect()
        };
        new_voters.insert(new_node_id.clone());

        info!(
            "Proposing config change to add node {:?}. New voters: {:?}",
            new_node_id, new_voters
        );

        // 3. 通过 leader 提交配置变更
        let request_id = raft_lite::RequestId::new();
        let config_change_event = raft_lite::Event::ChangeConfig {
            new_voters,
            request_id,
        };

        if let Some(leader_node) = self.get_node(&leader_id) {
            // leader_node.handle_event(config_change_event).await;

            match self
                .driver
                .send_event(leader_id.clone(), config_change_event)
            {
                raft_lite::mutl_raft_driver::SendEventResult::Success => {
                    info!("Config change event sent to leader {:?}", leader_id);
                }
                raft_lite::mutl_raft_driver::SendEventResult::NotFound => {
                    return Err(format!("Leader node {:?} not found", leader_id));
                }
                raft_lite::mutl_raft_driver::SendEventResult::SendFailed => {
                    return Err(format!(
                        "Failed to send config change event to leader {:?}",
                        leader_id
                    ));
                }
            }
            info!("Config change event sent to leader {:?}", leader_id);
        } else {
            return Err(format!("Leader node {:?} not found", leader_id));
        }

        // 4. 等待配置变更提交（给一些时间让配置变更日志被复制）
        tokio::time::sleep(Duration::from_millis(500)).await;

        // 5. 现在创建新节点（此时配置变更应该已经在进行中）
        // 新节点的初始peers是当前集群中除自己外的所有节点
        // let initial_peers: Vec<RaftId> = {
        //     let nodes = self.nodes.lock().unwrap();
        //     nodes.keys().cloned().collect()
        // };

        let new_node = TestNode::new(
            new_node_id.clone(),
            self.hub.clone(),
            self.driver.get_timer_service(),
            self.snapshot_storage.clone(),
            self.driver.clone(),
            Vec::new(), // 初始peers为空，配置变更后会自动更新
        )
        .await
        .map_err(|e| format!("Failed to create new node: {}", e))?;

        // 6. 将新节点添加到本地集群管理
        self.nodes
            .lock()
            .unwrap()
            .insert(new_node_id.clone(), new_node.clone());
        self.driver
            .add_raft_group(new_node_id.clone(), Box::new(new_node));

        info!(
            "Successfully added node {:?} to cluster via Raft config change",
            new_node_id
        );

        // 7. 等待新节点同步数据
        info!(
            "Waiting for new node {:?} to synchronize data...",
            new_node_id
        );
        tokio::time::sleep(Duration::from_secs(2)).await;

        Ok(())
    }

    // 移除节点 - 使用 Raft 配置变更
    pub async fn remove_node(&self, node_id: &RaftId) -> Result<(), String> {
        info!(
            "Removing node {:?} from cluster via Raft config change",
            node_id
        );

        // 1. 检查节点是否存在

        let node = self.get_node(node_id);
        if node.is_none() {
            return Err(format!("Node {:?} not found in cluster", node_id));
        }

        // 2. 获取当前 leader
        let leader_id = self
            .wait_for_leader(Duration::from_secs(5))
            .await
            .map_err(|e| format!("No leader available for config change: {}", e))?;

        // 3. 准备新的配置（当前所有节点 - 要移除的节点）
        let mut new_voters: std::collections::HashSet<RaftId> = {
            let nodes = self.nodes.lock().unwrap();
            nodes.keys().cloned().collect()
        };
        new_voters.remove(node_id);

        info!(
            "Proposing config change to remove node {:?}. New voters: {:?}",
            node_id, new_voters
        );

        // 4. 通过 leader 提交配置变更
        let request_id = raft_lite::RequestId::new();
        let config_change_event = raft_lite::Event::ChangeConfig {
            new_voters,
            request_id,
        };

        if let Some(leader_node) = self.get_node(&leader_id) {
            //leader_node.handle_event(config_change_event).await;

            match self
                .driver
                .send_event(leader_id.clone(), config_change_event)
            {
                raft_lite::mutl_raft_driver::SendEventResult::Success => {
                    info!("Config change event sent to leader {:?}", leader_id);
                }
                raft_lite::mutl_raft_driver::SendEventResult::NotFound => {
                    return Err(format!("Leader node {:?} not found", leader_id));
                }
                raft_lite::mutl_raft_driver::SendEventResult::SendFailed => {
                    return Err(format!(
                        "Failed to send config change event to leader {:?}",
                        leader_id
                    ));
                }
            }
            info!("Config change event sent to leader {:?}", leader_id);
        } else {
            return Err(format!("Leader node {:?} not found", leader_id));
        }

        // 5. 等待配置变更提交

        timeout(
            Duration::from_secs(5),
            node.as_ref().unwrap().wait_remove_node(),
        )
        .await
        .ok();

        self.driver.del_raft_group(&node_id);
        // 6. 从本地集群管理中移除节点
        if self.nodes.lock().unwrap().remove(node_id).is_some() {
            info!(
                "Successfully removed node {:?} from cluster via Raft config change",
                node_id
            );
            Ok(())
        } else {
            Err(format!(
                "Failed to remove node {:?} from local cluster management",
                node_id
            ))
        }
    }

    // 获取所有节点的状态
    pub fn get_cluster_status(&self) -> HashMap<RaftId, raft_lite::Role> {
        let nodes = self.nodes.lock().unwrap();
        nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.get_role()))
            .collect()
    }

    // 等待集群稳定（有一个leader）
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<RaftId, String> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            let leaders = self.get_current_leader().await;
            if let Some(leader) = leaders.first() {
                return Ok(leader.clone());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err("No leader found within timeout".to_string())
    }

    // Verify that all nodes have the same data
    pub async fn verify_data_consistency(&self) -> Result<(), String> {
        let nodes = self.nodes.lock().unwrap();
        if nodes.is_empty() {
            return Err("No nodes in cluster".to_string());
        }

        // Get data from the first node as reference
        let mut reference_data: Option<std::collections::HashMap<String, String>> = None;
        let mut reference_node_id: Option<RaftId> = None;

        for (node_id, node) in nodes.iter() {
            let node_data = node.get_all_data();

            if reference_data.is_none() {
                reference_data = Some(node_data);
                reference_node_id = Some(node_id.clone());
            } else {
                let ref_data = reference_data.as_ref().unwrap();
                if node_data != *ref_data {
                    return Err(format!(
                        "Data inconsistency detected between {:?} and {:?}. Reference: {:?}, Current: {:?}",
                        reference_node_id.as_ref().unwrap(),
                        node_id,
                        ref_data,
                        node_data
                    ));
                }
            }
        }

        println!("✓ Data consistency verified across {} nodes", nodes.len());
        if let Some(data) = reference_data {
            if !data.is_empty() {
                //println!("  Shared data: {:?}", data);
            } else {
                println!("  All nodes have empty data (expected initially)");
            }
        }

        Ok(())
    }

    // Wait for data to be replicated to all nodes
    pub async fn wait_for_data_replication(&self, timeout: Duration) -> Result<(), String> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            match self.verify_data_consistency().await {
                Ok(()) => return Ok(()),
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            }
        }

        Err("Data replication did not complete within timeout".to_string())
    }

    // Get data from a specific node for debugging
    pub fn get_node_data(
        &self,
        node_id: &RaftId,
    ) -> Option<std::collections::HashMap<String, String>> {
        let nodes = self.nodes.lock().unwrap();
        nodes.get(node_id).map(|node| node.get_all_data())
    }

    // 添加 learner 到集群
    pub async fn add_learner(&self, learner_id: RaftId) -> Result<(), String> {
        info!("Adding learner {:?} to cluster", learner_id);

        // 1. 获取当前 leader
        let leader_id = self
            .wait_for_leader(Duration::from_secs(5))
            .await
            .map_err(|e| format!("No leader available for adding learner: {}", e))?;

        // 2. 通过 leader 添加 learner
        let request_id = raft_lite::RequestId::new();
        let add_learner_event = raft_lite::Event::AddLearner {
            learner: learner_id.clone(),
            request_id,
        };

        match self.driver.send_event(leader_id.clone(), add_learner_event) {
            raft_lite::mutl_raft_driver::SendEventResult::Success => {
                info!("Add learner event sent to leader {:?}", leader_id);
            }
            raft_lite::mutl_raft_driver::SendEventResult::NotFound => {
                return Err(format!("Leader node {:?} not found", leader_id));
            }
            raft_lite::mutl_raft_driver::SendEventResult::SendFailed => {
                return Err(format!(
                    "Failed to send add learner event to leader {:?}",
                    leader_id
                ));
            }
        }

        // 3. 等待配置变更传播
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 4. 创建新的 learner 节点
        // 获取当前的 voters 列表作为 learner 的初始配置
        let current_voters: Vec<RaftId> = self.config.node_ids.clone();
        let learner_node = TestNode::new_learner(
            learner_id.clone(),
            self.hub.clone(),
            self.driver.get_timer_service(),
            self.snapshot_storage.clone(),
            self.driver.clone(),
            current_voters, // learner 需要知道当前的 voters
        )
        .await
        .map_err(|e| format!("Failed to create learner node: {}", e))?;

        // 5. 将 learner 添加到本地集群管理
        self.nodes
            .lock()
            .unwrap()
            .insert(learner_id.clone(), learner_node.clone());
        self.driver
            .add_raft_group(learner_id.clone(), Box::new(learner_node));

        info!("Successfully added learner {:?} to cluster", learner_id);
        Ok(())
    }

    // 从集群中移除 learner
    pub async fn remove_learner(&self, learner_id: &RaftId) -> Result<(), String> {
        info!("Removing learner {:?} from cluster", learner_id);

        let learner = self.get_node(learner_id);

        // 1. 检查 learner 是否存在
        if learner.is_none() {
            return Err(format!("Learner {:?} not found in cluster", learner_id));
        }

        // 2. 获取当前 leader
        let leader_id = self
            .wait_for_leader(Duration::from_secs(5))
            .await
            .map_err(|e| format!("No leader available for removing learner: {}", e))?;

        // 3. 通过 leader 移除 learner
        let request_id = raft_lite::RequestId::new();
        let remove_learner_event = raft_lite::Event::RemoveLearner {
            learner: learner_id.clone(),
            request_id,
        };

        match self
            .driver
            .send_event(leader_id.clone(), remove_learner_event)
        {
            raft_lite::mutl_raft_driver::SendEventResult::Success => {
                info!("Remove learner event sent to leader {:?}", leader_id);
            }
            raft_lite::mutl_raft_driver::SendEventResult::NotFound => {
                return Err(format!("Leader node {:?} not found", leader_id));
            }
            raft_lite::mutl_raft_driver::SendEventResult::SendFailed => {
                return Err(format!(
                    "Failed to send remove learner event to leader {:?}",
                    leader_id
                ));
            }
        }

        // 4. 等待配置变更传播

        timeout(
            Duration::from_secs(5),
            learner.as_ref().unwrap().wait_remove_node(),
        )
        .await
        .ok();

        // 5. 从本地集群管理中移除 learner
        self.driver.del_raft_group(learner_id);
        if self.nodes.lock().unwrap().remove(learner_id).is_some() {
            info!("Successfully removed learner {:?} from cluster", learner_id);
            Ok(())
        } else {
            Err(format!(
                "Failed to remove learner {:?} from local cluster management",
                learner_id
            ))
        }
    }

    // 等待 learner 同步数据
    pub async fn wait_for_learner_sync(
        &self,
        learner_id: &RaftId,
        timeout: Duration,
    ) -> Result<(), String> {
        let start = std::time::Instant::now();

        // 获取参考节点的数据（从任意一个 voter 获取）
        let reference_data = {
            let nodes = self.nodes.lock().unwrap();
            let voter_node = nodes.values().find(|node| {
                let role = node.get_role();
                role == raft_lite::Role::Leader || role == raft_lite::Role::Follower
            });

            match voter_node {
                Some(node) => node.get_all_data(),
                None => return Err("No voter nodes found for reference".to_string()),
            }
        };

        // 等待 learner 的数据与参考数据一致
        while start.elapsed() < timeout {
            if let Some(learner_data) = self.get_node_data(learner_id) {
                if learner_data == reference_data {
                    info!(
                        "Learner {:?} has synchronized with cluster data",
                        learner_id
                    );
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(format!(
            "Learner {:?} failed to synchronize within timeout",
            learner_id
        ))
    }
}
