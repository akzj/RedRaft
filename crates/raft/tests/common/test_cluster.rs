// test_cluster.rs

use crate::common::test_node::TestNode;
use crate::common::test_statemachine::KvCommand;
use crate::mock::mock_network::MockNetworkHub;
use crate::mock::mock_network::MockNetworkHubConfig;
use crate::mock::mock_network::MockRaftNetworkConfig;
use crate::mock::mock_storage::SnapshotMemStore;
use raft::multi_raft_driver::MultiRaftDriver;
use raft::RaftId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::timeout;
use tokio::time::Timeout;
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
    snapshot_storage: SnapshotMemStore,
    nodes: Arc<Mutex<HashMap<RaftId, TestNode>>>,
}

impl TestCluster {
    pub async fn new(config: TestClusterConfig) -> Self {
        let hub = MockNetworkHub::new(config.hub.clone());
        let cluster = TestCluster {
            snapshot_storage: SnapshotMemStore::new(),
            driver: MultiRaftDriver::new(),
            config,
            hub,
            nodes: Arc::new(Mutex::new(HashMap::new())),
        };

        // Create all nodes
        let all_node_ids: Vec<RaftId> = cluster.config.node_ids.clone();
        for node_id in &cluster.config.node_ids {
            // Initial peers are all nodes in the cluster except itself
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

    // Start the cluster (e.g., trigger initial election)
    pub async fn start(&self) {
        info!("Starting TestCluster...");
        self.driver.main_loop().await
    }

    // Isolate node
    pub async fn isolate_node(&self, id: &RaftId) {
        info!("Isolating node {:?}", id);
        if let Some(node) = self.get_node(id) {
            node.isolate().await;
        } else {
            warn!("Node {:?} not found for isolation", id);
        }
    }

    // Restore node
    pub async fn restore_node(&self, id: &RaftId) {
        info!("Restoring node {:?}", id);
        if let Some(node) = self.get_node(id) {
            node.restore().await;
        } else {
            warn!("Node {:?} not found for restoration", id);
        }
    }

    // Get node
    pub fn get_node(&self, id: &RaftId) -> Option<TestNode> {
        let nodes = self.nodes.lock().unwrap();
        nodes.get(id).cloned()
    }

    // Get node (mutable)
    pub fn get_node_mut(&self, id: &RaftId) -> Option<TestNode> {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.get_mut(id).cloned()
    }

    // Stop node (simulate failure)
    pub async fn stop_node(&mut self, id: &RaftId) -> bool {
        if self.nodes.lock().unwrap().remove(id).is_some() {
            info!("Stopped node {:?}", id);
            // May also need to notify network layer that this node is down
            // hub.unregister_node(id) ? (if MockNetworkHub has this method)
            true
        } else {
            warn!("Attempted to stop non-existent node {:?}", id);
            false
        }
    }

    // Update network configuration
    pub async fn update_network_config_for_node(
        &self,
        node_id: &RaftId,
        new_config: MockRaftNetworkConfig,
    ) {
        self.hub.update_config(node_id.clone(), new_config).await;
    }

    // Send business command
    pub fn propose_command(&self, leader_id: &RaftId, command: &KvCommand) -> Result<(), String> {
        let request_id = raft::RequestId::new();
        let command_bytes = command.encode();
        let event = raft::Event::ClientPropose {
            cmd: command_bytes,
            request_id,
        };
        // if let Some(node) = self.get_node(leader_id) {
        //     node.handle_event(event).await;
        //     Ok(())
        // } else {
        //     Err(format!("Node {:?} not found", leader_id))
        // }

        match self.driver.dispatch_event(leader_id.clone(), event) {
            raft::multi_raft_driver::SendEventResult::Success => {
                return Ok(());
            }
            raft::multi_raft_driver::SendEventResult::NotFound => {
                warn!("Node {:?} not found for command proposal", leader_id);
                return Err(format!("Node {:?} not found", leader_id));
            }
            raft::multi_raft_driver::SendEventResult::SendFailed
            | raft::multi_raft_driver::SendEventResult::ChannelFull => {
                warn!("Failed to send event to node {:?}", leader_id);
                return Err(format!("Failed to send event to node {:?}", leader_id));
            }
        }
    }

    // Send business command
    pub fn trigger_snapshot(&self, leader_id: &RaftId) -> Result<(), String> {
        let event = raft::Event::CreateSnapshot(raft::message::CreateSnapshot { bootstrap: false });

        match self.driver.dispatch_event(leader_id.clone(), event) {
            raft::multi_raft_driver::SendEventResult::Success => {
                return Ok(());
            }
            raft::multi_raft_driver::SendEventResult::NotFound => {
                warn!("Node {:?} not found for command proposal", leader_id);
                return Err(format!("Node {:?} not found", leader_id));
            }
            raft::multi_raft_driver::SendEventResult::SendFailed
            | raft::multi_raft_driver::SendEventResult::ChannelFull => {
                warn!("Failed to send event to node {:?}", leader_id);
                return Err(format!("Failed to send event to node {:?}", leader_id));
            }
        }
    }

    // Get current leader
    pub async fn get_current_leader(&self) -> Vec<RaftId> {
        let mut leaders = Vec::new();
        let nodes = self.nodes.lock().unwrap();
        for node in nodes.values() {
            if node.get_role() == raft::Role::Leader {
                leaders.push(node.id.clone());
            }
        }
        leaders
    }

    // Add new node to cluster - using Raft config change
    pub async fn add_node(&self, new_node_id: &RaftId) -> Result<(), String> {
        info!(
            "Adding new node {:?} to cluster via Raft config change",
            new_node_id
        );

        // 1. First get current leader
        let leader_id = self
            .wait_for_leader(Duration::from_secs(5))
            .await
            .map_err(|e| format!("No leader available for config change: {}", e))?;

        // 2. Prepare new configuration (current all nodes + new node)
        let mut new_voters: std::collections::HashSet<RaftId> = {
            let nodes = self.nodes.lock().unwrap();
            nodes.keys().cloned().collect()
        };
        new_voters.insert(new_node_id.clone());

        info!(
            "Proposing config change to add node {:?}. New voters: {:?}",
            new_node_id, new_voters
        );

        // 3. Submit configuration change through leader
        let request_id = raft::RequestId::new();
        let config_change_event = raft::Event::ChangeConfig {
            new_voters,
            request_id,
        };

        if let Some(leader_node) = self.get_node(&leader_id) {
            // leader_node.handle_event(config_change_event).await;

            match self
                .driver
                .dispatch_event(leader_id.clone(), config_change_event)
            {
                raft::multi_raft_driver::SendEventResult::Success => {
                    info!("Config change event sent to leader {:?}", leader_id);
                }
                raft::multi_raft_driver::SendEventResult::NotFound => {
                    return Err(format!("Leader node {:?} not found", leader_id));
                }
                raft::multi_raft_driver::SendEventResult::SendFailed
                | raft::multi_raft_driver::SendEventResult::ChannelFull => {
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

        // 4. Wait for configuration change to commit (give some time for configuration change log to be replicated)
        tokio::time::sleep(Duration::from_millis(500)).await;

        // 5. Now create new node (configuration change should be in progress at this point)
        // New node's initial peers are all nodes in current cluster except itself
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
            Vec::new(), // Initial peers are empty, will be automatically updated after configuration change
        )
        .await
        .map_err(|e| format!("Failed to create new node: {}", e))?;

        // 6. Add new node to local cluster management
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

        // 7. Wait for new node to sync data
        info!(
            "Waiting for new node {:?} to synchronize data...",
            new_node_id
        );
        tokio::time::sleep(Duration::from_secs(2)).await;

        Ok(())
    }

    // Remove node - using Raft config change
    pub async fn remove_node(&self, node_id: &RaftId) -> Result<(), String> {
        info!(
            "Removing node {:?} from cluster via Raft config change",
            node_id
        );

        // 1. Check if node exists

        let node = self.get_node(node_id);
        if node.is_none() {
            return Err(format!("Node {:?} not found in cluster", node_id));
        }

        // 2. Get current leader
        let leader_id = self
            .wait_for_leader(Duration::from_secs(5))
            .await
            .map_err(|e| format!("No leader available for config change: {}", e))?;

        // 3. Prepare new configuration (current all nodes - node to remove)
        let mut new_voters: std::collections::HashSet<RaftId> = {
            let nodes = self.nodes.lock().unwrap();
            nodes.keys().cloned().collect()
        };
        new_voters.remove(node_id);

        info!(
            "Proposing config change to remove node {:?}. New voters: {:?}",
            node_id, new_voters
        );

        // 4. Submit configuration change through leader
        let request_id = raft::RequestId::new();
        let config_change_event = raft::Event::ChangeConfig {
            new_voters,
            request_id,
        };

        if let Some(leader_node) = self.get_node(&leader_id) {
            //leader_node.handle_event(config_change_event).await;

            match self
                .driver
                .dispatch_event(leader_id.clone(), config_change_event)
            {
                raft::multi_raft_driver::SendEventResult::Success => {
                    info!("Config change event sent to leader {:?}", leader_id);
                }
                raft::multi_raft_driver::SendEventResult::NotFound => {
                    return Err(format!("Leader node {:?} not found", leader_id));
                }
                raft::multi_raft_driver::SendEventResult::SendFailed
                | raft::multi_raft_driver::SendEventResult::ChannelFull => {
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

        // 5. Wait for configuration change to commit

        timeout(
            Duration::from_secs(5),
            node.as_ref().unwrap().wait_remove_node(),
        )
        .await
        .ok();

        self.driver.del_raft_group(&node_id);
        // 6. Remove node from local cluster management
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

    // Get status of all nodes
    pub fn get_cluster_status(&self) -> HashMap<RaftId, raft::Role> {
        let nodes = self.nodes.lock().unwrap();
        nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.get_role()))
            .collect()
    }

    // Wait for cluster to stabilize (have one leader)
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

    /// Send ReadIndex request (for linear consistency read test)
    pub fn send_read_index(&self, leader_id: &RaftId) -> Result<raft::RequestId, String> {
        let request_id = raft::RequestId::new();
        let event = raft::Event::ReadIndex { request_id };

        match self.driver.dispatch_event(leader_id.clone(), event) {
            raft::multi_raft_driver::SendEventResult::Success => Ok(request_id),
            raft::multi_raft_driver::SendEventResult::NotFound => {
                Err(format!("Node {:?} not found", leader_id))
            }
            raft::multi_raft_driver::SendEventResult::SendFailed
            | raft::multi_raft_driver::SendEventResult::ChannelFull => {
                Err(format!("Failed to send ReadIndex to node {:?}", leader_id))
            }
        }
    }

    /// Get node's current term
    pub async fn get_node_term(&self, node_id: &RaftId) -> Option<u64> {
        if let Some(node) = self.get_node(node_id) {
            Some(node.get_term().await)
        } else {
            None
        }
    }

    /// Get all nodes' terms (to verify Pre-Vote doesn't cause term inflation)
    pub async fn get_all_terms(&self) -> HashMap<RaftId, u64> {
        let nodes = self.nodes.lock().unwrap();
        let mut terms = HashMap::new();
        for (id, node) in nodes.iter() {
            terms.insert(id.clone(), node.get_term().await);
        }
        terms
    }

    // Add learner to cluster
    pub async fn add_learner(&self, learner_id: RaftId) -> Result<(), String> {
        info!("Adding learner {:?} to cluster", learner_id);

        // 1. Get current leader
        let leader_id = self
            .wait_for_leader(Duration::from_secs(5))
            .await
            .map_err(|e| format!("No leader available for adding learner: {}", e))?;

        // 2. Add learner through leader
        let request_id = raft::RequestId::new();
        let add_learner_event = raft::Event::AddLearner {
            learner: learner_id.clone(),
            request_id,
        };

        match self
            .driver
            .dispatch_event(leader_id.clone(), add_learner_event)
        {
            raft::multi_raft_driver::SendEventResult::Success => {
                info!("Add learner event sent to leader {:?}", leader_id);
            }
            raft::multi_raft_driver::SendEventResult::NotFound => {
                return Err(format!("Leader node {:?} not found", leader_id));
            }
            raft::multi_raft_driver::SendEventResult::SendFailed
            | raft::multi_raft_driver::SendEventResult::ChannelFull => {
                return Err(format!(
                    "Failed to send add learner event to leader {:?}",
                    leader_id
                ));
            }
        }

        // 3. Wait for configuration change to propagate
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 4. Create new learner node
        // Get current voters list as learner's initial configuration
        let current_voters: Vec<RaftId> = self.config.node_ids.clone();
        let learner_node = TestNode::new_learner(
            learner_id.clone(),
            self.hub.clone(),
            self.driver.get_timer_service(),
            self.snapshot_storage.clone(),
            self.driver.clone(),
            current_voters, // Learner needs to know current voters
        )
        .await
        .map_err(|e| format!("Failed to create learner node: {}", e))?;

        // 5. Add learner to local cluster management
        self.nodes
            .lock()
            .unwrap()
            .insert(learner_id.clone(), learner_node.clone());
        self.driver
            .add_raft_group(learner_id.clone(), Box::new(learner_node));

        info!("Successfully added learner {:?} to cluster", learner_id);
        Ok(())
    }

    // Remove learner from cluster
    pub async fn remove_learner(&self, learner_id: &RaftId) -> Result<(), String> {
        info!("Removing learner {:?} from cluster", learner_id);

        let learner = self.get_node(learner_id);

        // 1. Check if learner exists
        if learner.is_none() {
            return Err(format!("Learner {:?} not found in cluster", learner_id));
        }

        // 2. Get current leader
        let leader_id = self
            .wait_for_leader(Duration::from_secs(5))
            .await
            .map_err(|e| format!("No leader available for removing learner: {}", e))?;

        // 3. Remove learner through leader
        let request_id = raft::RequestId::new();
        let remove_learner_event = raft::Event::RemoveLearner {
            learner: learner_id.clone(),
            request_id,
        };

        match self
            .driver
            .dispatch_event(leader_id.clone(), remove_learner_event)
        {
            raft::multi_raft_driver::SendEventResult::Success => {
                info!("Remove learner event sent to leader {:?}", leader_id);
            }
            raft::multi_raft_driver::SendEventResult::NotFound => {
                return Err(format!("Leader node {:?} not found", leader_id));
            }
            raft::multi_raft_driver::SendEventResult::SendFailed
            | raft::multi_raft_driver::SendEventResult::ChannelFull => {
                return Err(format!(
                    "Failed to send remove learner event to leader {:?}",
                    leader_id
                ));
            }
        }

        // 4. Wait for configuration change to propagate

        timeout(
            Duration::from_secs(5),
            learner.as_ref().unwrap().wait_remove_node(),
        )
        .await
        .ok();

        // 5. Remove learner from local cluster management
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

    // Wait for learner to synchronize data
    pub async fn wait_for_learner_sync(
        &self,
        learner_id: &RaftId,
        timeout: Duration,
    ) -> Result<(), String> {
        let start = std::time::Instant::now();

        // Get reference node's data (from any voter)
        let reference_data = {
            let nodes = self.nodes.lock().unwrap();
            let voter_node = nodes.values().find(|node| {
                let role = node.get_role();
                role == raft::Role::Leader || role == raft::Role::Follower
            });

            match voter_node {
                Some(node) => node.get_all_data(),
                None => return Err("No voter nodes found for reference".to_string()),
            }
        };

        // Wait for learner's data to match reference data
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
