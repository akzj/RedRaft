//! RedRaft node implementation
//!
//! Integrates Multi-Raft, KV state machine, routing, and storage

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use parking_lot::Mutex;
use tracing::{debug, info};

use raft::{ClusterConfig, Network, RaftCallbacks, RaftId, RaftState, RaftStateOptions, Storage};

use crate::config::Config;
use crate::snapshot_transfer::SnapshotTransferManager;
use crate::state_machine::{ShardStateMachine, StateMachineCommand};
use proto::node::{
    node_service_server::NodeService, CreateRaftGroupRequest, CreateRaftGroupResponse,
    GetRaftStateRequest, GetRaftStateResponse, GetSplitStatusRequest, GetSplitStatusResponse,
    PrepareSplitRequest, PrepareSplitResponse, Role as ProtoRole, SplitPhase, SplitStatus,
};
use raft::event::Role as RaftRole;
use resp::{Command, CommandType, RespValue};
use rr_core::routing::RoutingTable;
use storage::{traits::KeyStore, ApplyResult as StoreApplyResult, RedisStore};
use tonic::{Request, Response, Status};

/// RedRaft node
pub struct RRNode {
    /// Node ID
    node_id: String,
    /// Multi-Raft driver
    driver: raft::multi_raft_driver::MultiRaftDriver,
    /// Storage backend
    storage: Arc<dyn Storage>,

    redis_store: Arc<storage::store::HybridStore>,
    /// Network layer
    network: Arc<dyn Network>,
    /// Routing table for shard and raft group management
    /// Managed by node, can be synced with pilot in the future
    routing_table: Arc<RoutingTable>,
    /// Raft group state machine mapping (shard_id -> state_machine)
    pub state_machines: Arc<Mutex<HashMap<String, Arc<ShardStateMachine>>>>,

    /// Snapshot transfer manager
    snapshot_transfer_manager: Arc<SnapshotTransferManager>,
    /// gRPC client connection pool (node_id -> client)
    /// Used to reuse connections across different services (SyncService, SplitService, etc.)
    grpc_client_pool:
        Arc<parking_lot::RwLock<std::collections::HashMap<String, Arc<tonic::transport::Channel>>>>,
    /// Split status tracking (task_id -> SplitStatus)
    split_statuses: Arc<tokio::sync::RwLock<HashMap<String, SplitStatusInfo>>>,
    /// Blocked slot ranges for writes (slot_start -> (slot_end, task_id))
    blocked_slot_ranges: Arc<tokio::sync::RwLock<HashMap<u32, (u32, String)>>>,
    /// Configuration
    config: Config,
}

/// Split status information
#[derive(Debug, Clone)]
pub struct SplitStatusInfo {
    pub split_task_id: String,
    pub source_shard_id: String,
    pub target_shard_id: String,
    pub slot_start: u32,
    pub slot_end: u32,
    pub phase: proto::node::SplitPhase,
    pub noop_apply_index: Option<u64>,
}

impl RRNode {
    pub fn new(
        node_id: String,
        storage: Arc<dyn Storage>,
        network: Arc<dyn Network>,
        redis_store: Arc<storage::store::HybridStore>,
        routing_table: Arc<RoutingTable>,
        config: Config,
    ) -> Self {
        Self {
            node_id: node_id.clone(),
            driver: raft::multi_raft_driver::MultiRaftDriver::new(),
            storage,
            network,
            redis_store,
            routing_table,
            state_machines: Arc::new(Mutex::new(HashMap::new())),
            snapshot_transfer_manager: Arc::new(SnapshotTransferManager::new()),
            grpc_client_pool: Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
            split_statuses: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            blocked_slot_ranges: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Get node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get configuration
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get snapshot transfer manager
    pub fn snapshot_transfer_manager(&self) -> &Arc<SnapshotTransferManager> {
        &self.snapshot_transfer_manager
    }

    /// Get multi-raft driver (for dispatching events to Raft groups)
    pub fn driver(&self) -> &raft::multi_raft_driver::MultiRaftDriver {
        &self.driver
    }

    /// Get routing table
    pub fn routing_table(&self) -> &Arc<RoutingTable> {
        &self.routing_table
    }

    /// Get gRPC endpoint URI for a node
    ///
    /// Retrieves the gRPC address from routing table and ensures it has
    /// a protocol prefix (http:// or https://).
    ///
    /// Returns the endpoint URI, or an error if the node is not found.
    pub fn get_node_grpc_endpoint(&self, node_id: &str) -> Result<String, String> {
        let grpc_addr = self
            .routing_table
            .get_grpc_address(&node_id.to_string())
            .ok_or_else(|| format!("No gRPC address found for node {}", node_id))?;

        // Ensure the address has a protocol prefix
        // If it already has http:// or https://, use it as-is
        // Otherwise, assume http:// (for development/non-TLS environments)
        // Note: gRPC uses HTTP/2, so http:// is valid for non-TLS connections
        let endpoint_uri = if grpc_addr.starts_with("http://") || grpc_addr.starts_with("https://")
        {
            grpc_addr
        } else {
            format!("http://{}", grpc_addr)
        };

        Ok(endpoint_uri)
    }

    /// Get or create gRPC channel for a node
    ///
    /// Uses connection pool to reuse existing connections.
    /// If connection doesn't exist, creates a new one and adds it to the pool.
    /// Returns the channel wrapped in Arc for sharing across services.
    pub async fn get_or_create_grpc_channel(
        &self,
        node_id: &str,
    ) -> Result<Arc<tonic::transport::Channel>, String> {
        // Check if channel already exists in pool
        {
            let pool = self.grpc_client_pool.read();
            if let Some(channel) = pool.get(node_id) {
                return Ok(Arc::clone(channel));
            }
        }

        // Channel doesn't exist, create new connection
        let endpoint_uri = self
            .get_node_grpc_endpoint(node_id)
            .map_err(|e| format!("Failed to get endpoint: {}", e))?;

        use tonic::transport::Endpoint;
        let endpoint = Endpoint::from_shared(endpoint_uri.clone())
            .map_err(|e| format!("Invalid endpoint {}: {}", endpoint_uri, e))?;

        let channel = endpoint
            .initial_connection_window_size(1024 * 1024)
            .initial_stream_window_size(128 * 1024)
            .http2_keep_alive_interval(std::time::Duration::from_secs(30)) // 每 30s 发 PING
            .keep_alive_timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(10))
            .keep_alive_while_idle(true) // 即使 idle 也发 keep-alive
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to node {}: {}", node_id, e))?;

        let channel_arc = Arc::new(channel);

        // Add to pool
        {
            let mut pool = self.grpc_client_pool.write();
            // Double-check in case another thread created it while we were connecting
            if let Some(existing) = pool.get(node_id) {
                return Ok(Arc::clone(existing));
            }
            pool.insert(node_id.to_string(), Arc::clone(&channel_arc));
        }

        tracing::info!("Created new gRPC channel connection for node {}", node_id);
        Ok(channel_arc)
    }

    /// Get existing Raft group
    ///
    /// Used for business request routing, will not create new Raft groups.
    /// Returns None if shard does not exist.
    ///
    /// # Type Conversion
    /// - Input: `shard_id` (business layer ShardId)
    /// - Output: `group` field in `RaftId` (Raft layer GroupId)
    /// - Semantics: One Shard corresponds to one Raft Group, ShardId is used as GroupId
    pub fn get_raft_group(&self, shard_id: &str) -> Option<RaftId> {
        if self.state_machines.lock().contains_key(shard_id) {
            // ShardId (business layer) -> GroupId (Raft layer)
            // Same type (both String), but different semantics
            Some(RaftId::new(shard_id.to_string(), self.node_id.clone()))
        } else {
            None
        }
    }

    /// Get state machine for a shard
    ///
    /// Returns a cloned Arc reference to the state machine.
    /// This allows holding a reference throughout an operation to avoid state inconsistency.
    ///
    /// # Arguments
    /// - `shard_id`: Shard ID
    ///
    /// # Returns
    /// - `Some(Arc<KVStateMachine>)`: State machine if shard exists
    /// - `None`: If shard does not exist
    pub fn get_state_machine(&self, shard_id: &str) -> Option<Arc<ShardStateMachine>> {
        self.state_machines.lock().get(shard_id).cloned()
    }

    /// Create Raft group (for Pilot control plane only)
    ///
    /// # Important
    /// This function should only be called by Pilot control plane, not in business request handling.
    /// Business requests should use `get_raft_group` to get existing groups.
    ///
    /// # Arguments
    /// - `shard_id`: Shard ID (business layer ShardId)
    /// - `nodes`: List of all node IDs for this shard
    ///
    /// # Type Conversion
    /// - Input: `shard_id` (business layer ShardId)
    /// - Internal: Pass `shard_id` as `group` to `RaftId` (Raft layer GroupId)
    /// - Semantics: One Shard corresponds to one Raft Group, ShardId is used as GroupId
    pub async fn create_raft_group(
        &self,
        shard_id: String,
        nodes: Vec<String>,
    ) -> Result<RaftId, String> {
        // ShardId (business layer) -> GroupId (Raft layer)
        // Same type (both String), but different semantics: Shard can split, Raft Group cannot split
        let raft_id = RaftId::new(shard_id.clone(), self.node_id.clone());

        // Check if already exists
        if self.state_machines.lock().contains_key(&shard_id) {
            debug!("Raft group already exists: {}", raft_id);
            return Ok(raft_id);
        }

        // Create state machine (using memory store, can be replaced with RocksDB later)

        // Create cluster configuration
        let voters: HashSet<RaftId> = nodes
            .iter()
            .map(|node| RaftId::new(shard_id.clone(), node.clone()))
            .collect();
        let config = ClusterConfig::simple(voters, 0);

        // Create Raft state
        let mut options = RaftStateOptions::default();
        options.id = raft_id.clone();
        let timers = self.driver.get_timer_service();

        // Create state machine with all dependencies (implements RaftCallbacks directly)
        let state_machine = Arc::new(ShardStateMachine::new(
            self.redis_store.clone(),
            self.storage.clone(),
            self.network.clone(),
            timers,
            self.snapshot_transfer_manager.clone(),
            self.routing_table.clone(),
            self.config.clone(),
            Arc::new(self.driver.clone()),
            raft_id.clone(),
        ));
        self.state_machines
            .lock()
            .insert(shard_id.clone(), state_machine.clone());

        // Use state_machine directly as RaftCallbacks
        let callbacks: Arc<dyn RaftCallbacks> = state_machine.clone();

        let mut raft_state = RaftState::new(options, callbacks.clone())
            .await
            .map_err(|e| e.to_string())?;

        // Load persisted state
        if let Ok(Some(hard_state)) = self.storage.load_hard_state(&raft_id).await {
            raft_state.current_term = hard_state.term;
            raft_state.voted_for = hard_state.voted_for;
        }

        // Load cluster configuration
        if let Ok(loaded_config) = self.storage.load_cluster_config(&raft_id).await {
            raft_state.config = loaded_config;
        } else {
            raft_state.config = config;
            self.storage
                .save_cluster_config(&raft_id, raft_state.config.clone())
                .await
                .map_err(|e| format!("Failed to save cluster config: {}", e))?;
        }

        // Store RaftState in ShardStateMachine (one-to-one correspondence)
        // Using Weak reference breaks the circular dependency:
        // - ShardStateMachine stores Weak<RaftState> (weak reference)
        // - RaftState.callbacks stores Arc<ShardStateMachine> (strong reference)
        // This allows ShardStateMachine to be dropped when external references are removed,
        // even though RaftState still holds a strong reference to it.
        let raft_state_arc = Arc::new(tokio::sync::Mutex::new(raft_state));
        state_machine.set_raft_state(raft_state_arc.clone());

        // Register to MultiRaftDriver (ShardStateMachine implements HandleEventTrait)
        // Create a wrapper to pass Arc<ShardStateMachine> as HandleEventTrait
        let handle_event: Box<dyn raft::multi_raft_driver::HandleEventTrait> =
            Box::new(ShardStateMachineHandler {
                state_machine: state_machine.clone(),
            });
        self.driver.add_raft_group(raft_id.clone(), handle_event);

        // Update routing table (shard routing is managed by routing_table)
        // Note: add_shard functionality will be redesigned

        info!("Created Raft group: {}", raft_id);
        Ok(raft_id)
    }

    /// Handle client command
    pub async fn handle_command(&self, cmd: Command) -> anyhow::Result<RespValue> {
        debug!("Handling command: {:?}", cmd.name());

        match cmd.command_type() {
            CommandType::Read => self.handle_read(cmd).await,
            CommandType::Write => self.handle_write(cmd).await,
        }
    }

    /// Handle read command - read directly from state machine
    async fn handle_read(&self, cmd: Command) -> anyhow::Result<RespValue> {
        // Get routing key
        let key = cmd.get_key();

        // For commands without key (e.g., PING, ECHO), use any available state machine
        // These commands are stateless and handled by storage
        let state_machines = self.state_machines.lock();

        if key.is_none() {
            // Commands without key: use first available state machine
            // storage will handle PING, ECHO, etc.
            if let Some((_group_id, sm)) = state_machines.iter().next() {
                // For read operations, use current apply_index as read_index
                // apply_index can be 0 for read-only commands
                let read_index = sm.apply_index().load(std::sync::atomic::Ordering::SeqCst);
                let result = sm.store().apply(read_index, 0, &cmd);
                return Ok(apply_result_to_resp(result));
            } else {
                // No state machines available, but PING/ECHO can still work
                // Create a temporary store instance for these stateless commands
                // Note: This is a fallback, ideally we should have at least one shard
                return Err(anyhow::anyhow!("No shards available"));
            }
        }

        let key_bytes = key.unwrap();

        // Determine shard for key-based commands
        let shard_id = self
            .routing_table
            .find_shard_for_key(&key_bytes)
            .map_err(|e| anyhow::anyhow!("CLUSTERDOWN {}", e))?;

        // Check split status - return redirect if MOVED needed
        // TODO: Split functionality will be redesigned
        // if let Some((_target_shard, target_addr)) = ... {
        //     let slot = RoutingTable::slot_for_key(&key_bytes);
        //     return Err(format!("MOVED {} {}", slot, target_addr));
        // }

        // Get Raft group (must exist, created by Pilot)
        let raft_id = self
            .get_raft_group(&shard_id)
            .ok_or_else(|| anyhow::anyhow!("CLUSTERDOWN Shard {} not ready", shard_id))?;

        // Read from state machine
        if let Some(sm) = state_machines.get(&raft_id.group) {
            // For read operations, use current apply_index as read_index
            // apply_index can be 0 for read-only commands
            let read_index = sm.apply_index().load(std::sync::atomic::Ordering::SeqCst);
            let result = sm.store().apply(read_index, 0, &cmd);
            Ok(apply_result_to_resp(result))
        } else {
            Err(anyhow::anyhow!("State machine not found"))
        }
    }

    /// Handle write command - through Raft consensus
    /// Node-level logic: determine shard and delegate to state machine
    async fn handle_write(&self, cmd: Command) -> anyhow::Result<RespValue> {
        // Get routing key
        let key = match cmd.get_key() {
            Some(k) => k,
            None => {
                // Global write commands like FlushDb need special handling
                return self.handle_global_write(cmd).await;
            }
        };

        // Determine shard (node-level routing)
        let shard_id = self
            .routing_table
            .find_shard_for_key(key)
            .map_err(|e| anyhow::anyhow!("CLUSTERDOWN {}", e))?;

        // Check split status - return redirect if MOVED needed
        // TODO: Split functionality will be redesigned
        // if let Some((_target_shard, target_addr)) = ... {
        //     let slot = RoutingTable::slot_for_key(key);
        //     return Err(format!("MOVED {} {}", slot, target_addr));
        // }

        // Check if in buffering phase - return TRYAGAIN if so
        // TODO: Split functionality will be redesigned
        // if ... {
        //     return Err("TRYAGAIN Split in progress, please retry".to_string());
        // }

        // Get state machine for this shard (must exist, created by Pilot)
        let state_machine = self
            .get_state_machine(&shard_id)
            .ok_or_else(|| anyhow::anyhow!("CLUSTERDOWN Shard {} not ready", shard_id))?;

        // Delegate to state machine for shard-specific Raft operations
        state_machine
            .propose_command(StateMachineCommand::RedisCommand(cmd))
            .await
    }

    /// Handle global write commands
    async fn handle_global_write(&self, cmd: Command) -> anyhow::Result<RespValue> {
        match cmd {
            Command::FlushDb => {
                // Clear all shards
                let state_machines = self.state_machines.lock();
                for sm in state_machines.values() {
                    // HybridStore implements RedisStore, so we can call flushdb directly
                    sm.store()
                        .flushdb()
                        .map_err(|e| anyhow::anyhow!("Failed to flushdb: {}", e))?;
                }
                Ok(RespValue::SimpleString(bytes::Bytes::from("OK")))
            }
            _ => Err(anyhow::anyhow!(
                "Unsupported global write command: {}",
                cmd.name()
            )),
        }
    }

    /// Start node
    pub async fn start(&self) -> anyhow::Result<()> {
        info!("Starting RedRaft node: {}", self.node_id);

        // Start MultiRaftDriver
        let driver = self.driver.clone();
        tokio::spawn(async move {
            driver.main_loop().await;
        });

        info!("RedRaft node started: {}", self.node_id);
        Ok(())
    }

    /// Stop node
    pub fn stop(&self) {
        info!("Stopping RedRaft node: {}", self.node_id);
        self.driver.stop();
    }
}

/// Wrapper to make Arc<ShardStateMachine> implement HandleEventTrait
struct ShardStateMachineHandler {
    state_machine: Arc<ShardStateMachine>,
}

#[async_trait::async_trait]
impl raft::multi_raft_driver::HandleEventTrait for ShardStateMachineHandler {
    async fn handle_event(&self, event: raft::Event) {
        self.state_machine.handle_event(event).await;
    }
}

/// Convert StoreApplyResult to RespValue
fn apply_result_to_resp(result: StoreApplyResult) -> RespValue {
    match result {
        StoreApplyResult::Ok => RespValue::SimpleString(bytes::Bytes::from("OK")),
        StoreApplyResult::Pong(msg) => match msg {
            Some(m) => RespValue::BulkString(Some(m)),
            None => RespValue::SimpleString(bytes::Bytes::from("PONG")),
        },
        StoreApplyResult::Integer(n) => RespValue::Integer(n),
        StoreApplyResult::Value(v) => match v {
            Some(data) => RespValue::BulkString(Some(data)),
            None => RespValue::Null,
        },
        StoreApplyResult::Array(items) => RespValue::Array(
            items
                .into_iter()
                .map(|item| match item {
                    Some(data) => RespValue::BulkString(Some(data)),
                    None => RespValue::Null,
                })
                .collect(),
        ),
        StoreApplyResult::KeyValues(kvs) => {
            let mut result = Vec::with_capacity(kvs.len() * 2);
            for (k, v) in kvs {
                result.push(RespValue::BulkString(Some(k)));
                result.push(RespValue::BulkString(Some(v)));
            }
            RespValue::Array(result)
        }
        StoreApplyResult::Type(t) => match t {
            Some(type_name) => RespValue::SimpleString(bytes::Bytes::from(type_name)),
            None => RespValue::SimpleString(bytes::Bytes::from("none")),
        },
        StoreApplyResult::Error(e) => {
            RespValue::Error(bytes::Bytes::from(format!("ERR {}", e.to_string())))
        }
    }
}

// ============================================================================
// NodeService Implementation
// ============================================================================

/// Node Service implementation
pub struct NodeServiceImpl {
    /// Node reference
    node: Arc<RRNode>,
}

impl NodeServiceImpl {
    pub fn new(node: Arc<RRNode>) -> Self {
        Self { node }
    }
}

/// Convert Raft Role to Proto Role
fn raft_role_to_proto(role: &RaftRole) -> ProtoRole {
    match role {
        RaftRole::Follower => ProtoRole::Follower,
        RaftRole::Candidate => ProtoRole::Candidate,
        RaftRole::Leader => ProtoRole::Leader,
        RaftRole::Learner => ProtoRole::Learner,
    }
}

#[tonic::async_trait]
impl NodeService for NodeServiceImpl {
    async fn get_raft_state(
        &self,
        request: Request<GetRaftStateRequest>,
    ) -> Result<Response<GetRaftStateResponse>, Status> {
        let req = request.into_inner();
        let raft_group_id = req.raft_group_id;

        // If raft_group_id is empty, return error (for now, we require a specific group)
        if raft_group_id.is_empty() {
            return Err(Status::invalid_argument(
                "raft_group_id is required".to_string(),
            ));
        }

        // Get RaftState from ShardStateMachine (one-to-one correspondence)
        let raft_state_arc = {
            let state_machines = self.node.state_machines.lock();
            let state_machine = match state_machines.get(&raft_group_id) {
                Some(sm) => sm,
                None => {
                    return Err(Status::not_found(format!(
                        "Raft group {} not found",
                        raft_group_id
                    )));
                }
            };

            match state_machine.raft_state() {
                Some(state) => state,
                None => {
                    return Err(Status::internal(format!(
                        "Raft state not initialized for group {}",
                        raft_group_id
                    )));
                }
            }
        };

        // Lock and read RaftState (lock is released before await)
        let raft_state = raft_state_arc.lock().await;

        // Build response
        let response = GetRaftStateResponse {
            raft_group_id: raft_group_id.clone(),
            node_id: self.node.node_id.clone(),
            role: raft_role_to_proto(&raft_state.role) as i32,
            current_term: raft_state.current_term,
            leader_id: raft_state
                .leader_id
                .as_ref()
                .map(|id| id.node.clone())
                .unwrap_or_default(),
            last_applied: raft_state.last_applied,
        };

        Ok(Response::new(response))
    }

    async fn create_raft_group(
        &self,
        request: Request<CreateRaftGroupRequest>,
    ) -> Result<Response<CreateRaftGroupResponse>, Status> {
        let req = request.into_inner();
        let split_task_id = req.split_task_id;
        let target_shard_id = req.target_shard_id;
        let target_nodes = req.target_nodes;

        // Validate request
        if split_task_id.is_empty() {
            return Err(Status::invalid_argument("split_task_id is required"));
        }
        if target_shard_id.is_empty() {
            return Err(Status::invalid_argument("target_shard_id is required"));
        }
        if target_nodes.is_empty() {
            return Err(Status::invalid_argument("target_nodes is required"));
        }

        // Find the source shard (the shard that contains this node as leader)
        // Collect candidates first, then check leadership
        let candidates: Vec<(String, Arc<ShardStateMachine>)> = {
            let state_machines = self.node.state_machines.lock();
            state_machines
                .iter()
                .map(|(shard_id, sm)| (shard_id.clone(), sm.clone()))
                .collect()
        };

        // Find leader shard
        let (source_shard_id, source_state_machine) = {
            let mut found = None;
            for (shard_id, sm) in candidates {
                if let Some(raft_state_arc) = sm.raft_state() {
                    let raft_state = raft_state_arc.lock().await;
                    if raft_state.role == RaftRole::Leader {
                        found = Some((shard_id, sm));
                        break;
                    }
                }
            }
            match found {
                Some((id, sm)) => (id, sm),
                None => {
                    return Err(Status::failed_precondition(
                        "No leader found in source shard",
                    ));
                }
            }
        };

        // Submit CreateRaftGroup command to Raft
        let cmd = StateMachineCommand::CreateRaftGroup {
            split_task_id: split_task_id.clone(),
            target_shard_id: target_shard_id.clone(),
            target_nodes: target_nodes.clone(),
        };

        // Update split status
        {
            let mut statuses = self.node.split_statuses.write().await;
            statuses.insert(
                split_task_id.clone(),
                SplitStatusInfo {
                    split_task_id: split_task_id.clone(),
                    source_shard_id: source_shard_id.clone(),
                    target_shard_id: target_shard_id.clone(),
                    slot_start: 0,
                    slot_end: 0,
                    phase: proto::node::SplitPhase::CreatingRaftGroup,
                    noop_apply_index: None,
                },
            );
        }

        // Propose command (this will be applied on all nodes via Raft)
        match source_state_machine.propose_command(cmd).await {
            Ok(_) => {
                // Wait a bit for the command to be applied and leader election
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                // Get target leader (after election)
                let target_leader_id = {
                    let target_sm_opt = {
                        let state_machines = self.node.state_machines.lock();
                        state_machines.get(&target_shard_id).cloned()
                    };
                    if let Some(target_sm) = target_sm_opt {
                        if let Some(raft_state_arc) = target_sm.raft_state() {
                            let raft_state = raft_state_arc.lock().await;
                            raft_state
                                .leader_id
                                .as_ref()
                                .map(|id| id.node.clone())
                                .unwrap_or_default()
                        } else {
                            String::new()
                        }
                    } else {
                        String::new()
                    }
                };

                let response = CreateRaftGroupResponse {
                    split_task_id: split_task_id.clone(),
                    success: true,
                    error_message: String::new(),
                    target_shard_id: target_shard_id.clone(),
                    target_leader_id,
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                let response = CreateRaftGroupResponse {
                    split_task_id: split_task_id.clone(),
                    success: false,
                    error_message: e.to_string(),
                    target_shard_id: target_shard_id.clone(),
                    target_leader_id: String::new(),
                };
                Ok(Response::new(response))
            }
        }
    }

    async fn prepare_split(
        &self,
        request: Request<PrepareSplitRequest>,
    ) -> Result<Response<PrepareSplitResponse>, Status> {
        let req = request.into_inner();
        let split_task_id = req.split_task_id;
        let source_shard_id = req.source_shard_id;
        let target_shard_id = req.target_shard_id;
        let slot_start = req.slot_start;
        let slot_end = req.slot_end;

        // Validate request
        if split_task_id.is_empty() {
            return Err(Status::invalid_argument("split_task_id is required"));
        }
        if source_shard_id.is_empty() {
            return Err(Status::invalid_argument("source_shard_id is required"));
        }
        if target_shard_id.is_empty() {
            return Err(Status::invalid_argument("target_shard_id is required"));
        }
        if slot_start >= slot_end {
            return Err(Status::invalid_argument("slot_start must be less than slot_end"));
        }

        // Get source state machine
        let source_state_machine = {
            let state_machines = self.node.state_machines.lock();
            match state_machines.get(&source_shard_id) {
                Some(sm) => sm.clone(),
                None => {
                    return Err(Status::not_found(format!(
                        "Source shard {} not found",
                        source_shard_id
                    )));
                }
            }
        };

        // Check if we're the leader
        {
            if let Some(raft_state) = source_state_machine.raft_state() {
                let raft_state = raft_state.lock().await;
                if raft_state.role != RaftRole::Leader {
                    return Err(Status::failed_precondition(
                        "This node is not the leader of the source shard",
                    ));
                }
            } else {
                return Err(Status::internal("Raft state not initialized"));
            }
        }

        // Block writes to target slot range in memory
        {
            let mut blocked_ranges = self.node.blocked_slot_ranges.write().await;
            blocked_ranges.insert(slot_start, (slot_end, split_task_id.clone()));
        }

        // Update split status
        {
            let mut statuses = self.node.split_statuses.write().await;
            if let Some(status) = statuses.get_mut(&split_task_id) {
                status.slot_start = slot_start;
                status.slot_end = slot_end;
                status.phase = proto::node::SplitPhase::Preparing;
            }
        }

        // Submit noop command to Raft and wait for apply
        let noop_cmd = StateMachineCommand::Noop(format!("prepare_split_{}", split_task_id));
        match source_state_machine.propose_command(noop_cmd).await {
            Ok(_) => {
                // Get the apply index from the state machine
                let noop_apply_index = source_state_machine.last_apply_index();

                // Update split status
                {
                    let mut statuses = self.node.split_statuses.write().await;
                    if let Some(status) = statuses.get_mut(&split_task_id) {
                        status.noop_apply_index = Some(noop_apply_index);
                        status.phase = proto::node::SplitPhase::RoutingUpdated;
                    }
                }

                let response = PrepareSplitResponse {
                    split_task_id: split_task_id.clone(),
                    success: true,
                    error_message: String::new(),
                    noop_apply_index,
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                // Unblock writes on error
                {
                    let mut blocked_ranges = self.node.blocked_slot_ranges.write().await;
                    blocked_ranges.remove(&slot_start);
                }

                let response = PrepareSplitResponse {
                    split_task_id: split_task_id.clone(),
                    success: false,
                    error_message: e.to_string(),
                    noop_apply_index: 0,
                };
                Ok(Response::new(response))
            }
        }
    }

    async fn get_split_status(
        &self,
        request: Request<GetSplitStatusRequest>,
    ) -> Result<Response<GetSplitStatusResponse>, Status> {
        let req = request.into_inner();
        let split_task_id = req.split_task_id;

        if split_task_id.is_empty() {
            return Err(Status::invalid_argument("split_task_id is required"));
        }

        let status_info = {
            let statuses = self.node.split_statuses.read().await;
            statuses.get(&split_task_id).cloned()
        };

        match status_info {
            Some(info) => {
                // Get last applied index from source shard
                let last_applied_index = {
                    let state_machines = self.node.state_machines.lock();
                    if let Some(sm) = state_machines.get(&info.source_shard_id) {
                        sm.last_apply_index()
                    } else {
                        0
                    }
                };

                let status = match info.phase {
                    SplitPhase::CreatingRaftGroup => SplitStatus::CreatingRaftGroup,
                    SplitPhase::WaitingLeader => SplitStatus::WaitingLeader,
                    SplitPhase::Preparing => SplitStatus::Preparing,
                    SplitPhase::RoutingUpdated => SplitStatus::Preparing, // Use Preparing as closest match
                    SplitPhase::Completing => SplitStatus::Preparing, // Use Preparing as closest match
                    _ => SplitStatus::Unspecified,
                };

                let response = GetSplitStatusResponse {
                    split_task_id: info.split_task_id.clone(),
                    status: status as i32,
                    error_message: String::new(),
                    phase: info.phase as i32,
                    source_shard_id: info.source_shard_id,
                    target_shard_id: info.target_shard_id,
                    slot_start: info.slot_start,
                    slot_end: info.slot_end,
                    last_applied_index,
                };
                Ok(Response::new(response))
            }
            None => Err(Status::not_found(format!(
                "Split task {} not found",
                split_task_id
            ))),
        }
    }
}
