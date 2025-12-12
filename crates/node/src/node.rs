//! RedRaft node implementation
//!
//! Integrates Multi-Raft, KV state machine, routing, and storage

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

use raft::{
    ClusterConfig, Event, Network,
    RaftCallbacks, RaftId, RaftState, RaftStateOptions, RequestId,
    Storage,
};

use crate::config::Config;
use crate::router::ShardRouter;
use crate::snapshot_transfer::SnapshotTransferManager;
use crate::state_machine::KVStateMachine;
use resp::{Command, CommandType, RespValue};
use storage::{
    ApplyResult as StoreApplyResult, RedisStore,
    traits::KeyStore,
};

/// Pending request tracker
#[derive(Clone)]
pub struct PendingRequests {
    /// request_id -> (command, result_sender)
    requests: Arc<Mutex<HashMap<u64, (Command, oneshot::Sender<StoreApplyResult>)>>>,
}

impl PendingRequests {
    pub fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a pending request
    pub fn register(
        &self,
        request_id: RequestId,
        cmd: Command,
    ) -> oneshot::Receiver<StoreApplyResult> {
        let (tx, rx) = oneshot::channel();
        self.requests.lock().insert(request_id.into(), (cmd, tx));
        rx
    }

    /// Complete request and send result
    pub fn complete(&self, request_id: RequestId, result: StoreApplyResult) -> bool {
        if let Some((_, tx)) = self.requests.lock().remove(&request_id.into()) {
            let _ = tx.send(result);
            true
        } else {
            false
        }
    }

    /// Get pending command (for execution during apply)
    pub fn get_command(&self, request_id: RequestId) -> Option<Command> {
        self.requests
            .lock()
            .get(&request_id.into())
            .map(|(cmd, _)| cmd.clone())
    }

    /// Remove timed out request
    pub fn remove(&self, request_id: RequestId) {
        self.requests.lock().remove(&request_id.into());
    }
}

impl Default for PendingRequests {
    fn default() -> Self {
        Self::new()
    }
}

/// RedRaft node
pub struct RedRaftNode {
    /// Node ID
    node_id: String,
    /// Multi-Raft driver
    driver: raft::multi_raft_driver::MultiRaftDriver,
    /// Storage backend
    storage: Arc<dyn Storage>,

    redis_store: Arc<storage::store::HybridStore>,
    /// Network layer
    network: Arc<dyn Network>,
    /// Shard Router
    router: Arc<ShardRouter>,
    /// Raft group state machine mapping (shard_id -> state_machine)
    state_machines: Arc<Mutex<HashMap<String, Arc<KVStateMachine>>>>,
    /// Pending request tracker
    pending_requests: PendingRequests,
    /// Optional Pilot client for status reporting
    pilot_client: Arc<RwLock<Option<Arc<crate::pilot_client::PilotClient>>>>,
    /// Snapshot transfer manager
    snapshot_transfer_manager: Arc<SnapshotTransferManager>,
    /// Configuration
    config: Config,
}

impl RedRaftNode {
    pub fn new(
        node_id: String,
        storage: Arc<dyn Storage>,
        network: Arc<dyn Network>,
        redis_store: Arc<storage::store::HybridStore>,
        shard_count: usize,
        config: Config,
    ) -> Self {
        Self {
            node_id: node_id.clone(),
            driver: raft::multi_raft_driver::MultiRaftDriver::new(),
            storage,
            network,
            redis_store,
            router: Arc::new(ShardRouter::new(shard_count)),
            state_machines: Arc::new(Mutex::new(HashMap::new())),
            pending_requests: PendingRequests::new(),
            pilot_client: Arc::new(RwLock::new(None)),
            snapshot_transfer_manager: Arc::new(SnapshotTransferManager::new()),
            config,
        }
    }

    /// Set Pilot client for status reporting
    pub fn set_pilot_client(&self, client: Arc<crate::pilot_client::PilotClient>) {
        *self.pilot_client.write() = Some(client);
    }

    /// Get pending request tracker
    pub fn pending_requests(&self) -> &PendingRequests {
        &self.pending_requests
    }

    /// Get node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get router reference
    pub fn router(&self) -> Arc<ShardRouter> {
        self.router.clone()
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
        let state_machine = Arc::new(KVStateMachine::with_pending_requests(
            self.redis_store.clone(),
            self.storage.clone(),
            self.network.clone(),
            timers,
            self.snapshot_transfer_manager.clone(),
            self.pending_requests.clone(),
            self.config.clone(),
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

        // Register to MultiRaftDriver
        let raft_state_arc = Arc::new(tokio::sync::Mutex::new(raft_state));
        let handle_event = Box::new(RaftGroupHandler {
            raft_state: raft_state_arc.clone(),
        });

        self.driver.add_raft_group(raft_id.clone(), handle_event);

        // Update routing table
        self.router.add_shard(shard_id, nodes);

        info!("Created Raft group: {}", raft_id);
        Ok(raft_id)
    }

    /// Sync Raft groups from routing table
    ///
    /// Check shards this node is responsible for in routing table, automatically create missing Raft groups.
    /// This method should be called after routing table updates.
    ///
    /// # Returns
    /// Returns the number of newly created Raft groups
    pub async fn sync_raft_groups_from_routing(
        &self,
        routing_table: &crate::pilot_client::RoutingTable,
    ) -> usize {
        let mut created_count = 0;

        for (shard_id, nodes) in &routing_table.shard_nodes {
            // Check if this node is a replica for this shard
            if !nodes.contains(&self.node_id) {
                continue;
            }

            // Check if Raft group already exists
            if self.get_raft_group(shard_id).is_some() {
                continue;
            }

            // Create Raft group
            info!(
                "Creating Raft group for shard {} (nodes: {:?})",
                shard_id, nodes
            );

            match self
                .create_raft_group(shard_id.clone(), nodes.clone())
                .await
            {
                Ok(raft_id) => {
                    info!("Successfully created Raft group: {}", raft_id);
                    created_count += 1;

                    // Report success to Pilot
                    if let Some(client) = self.pilot_client.read().as_ref() {
                        let client = client.clone();
                        let shard_id_clone = shard_id.clone();
                        tokio::spawn(async move {
                            use pilot::RaftGroupStatus;
                            if let Err(e) = client
                                .report_shard_status(&shard_id_clone, RaftGroupStatus::Ready, None)
                                .await
                            {
                                warn!(
                                    "Failed to report shard {} ready status to pilot: {}",
                                    shard_id_clone, e
                                );
                            }
                        });
                    }
                }
                Err(e) => {
                    let error_msg = e.clone();
                    info!("Failed to create Raft group for shard {}: {}", shard_id, e);

                    // Report failure to Pilot
                    if let Some(client) = self.pilot_client.read().as_ref() {
                        let client = client.clone();
                        let shard_id_clone = shard_id.clone();
                        tokio::spawn(async move {
                            use pilot::RaftGroupStatus;
                            if let Err(report_err) = client
                                .report_shard_status(
                                    &shard_id_clone,
                                    RaftGroupStatus::Failed,
                                    Some(&error_msg),
                                )
                                .await
                            {
                                warn!(
                                    "Failed to report shard {} failure status to pilot: {}",
                                    shard_id_clone, report_err
                                );
                            }
                        });
                    }
                }
            }
        }

        if created_count > 0 {
            info!(
                "Synced {} Raft groups from routing table (version {})",
                created_count, routing_table.version
            );
        }

        created_count
    }

    /// Handle client command
    pub async fn handle_command(&self, cmd: Command) -> Result<RespValue, String> {
        debug!("Handling command: {:?}", cmd.name());

        match cmd.command_type() {
            CommandType::Read => self.handle_read(cmd).await,
            CommandType::Write => self.handle_write(cmd).await,
        }
    }

    /// Handle read command - read directly from state machine
    async fn handle_read(&self, cmd: Command) -> Result<RespValue, String> {
        // Get routing key
        let key = cmd.get_key();

        // For commands without key (e.g., PING, ECHO), use any available state machine
        // These commands are stateless and handled by storage
        let state_machines = self.state_machines.lock();

        if key.is_none() {
            // Commands without key: use first available state machine
            // storage will handle PING, ECHO, etc.
            if let Some((_group_id, sm)) = state_machines.iter().next() {
                let result = sm.store().apply(&cmd);
                return Ok(apply_result_to_resp(result));
            } else {
                // No state machines available, but PING/ECHO can still work
                // Create a temporary store instance for these stateless commands
                // Note: This is a fallback, ideally we should have at least one shard
                return Err("No shards available".to_string());
            }
        }

        let key_bytes = key.unwrap();

        // Determine shard for key-based commands
        let shard_id = self.router.route_key(&key_bytes);

        // Check split status - return redirect if MOVED needed
        if let Some((_target_shard, target_addr)) =
            self.router.should_move_for_split(&key_bytes, &shard_id)
        {
            let slot = crate::router::ShardRouter::slot_for_key(&key_bytes);
            return Err(format!("MOVED {} {}", slot, target_addr));
        }

        // Get Raft group (must exist, created by Pilot)
        let raft_id = self
            .get_raft_group(&shard_id)
            .ok_or_else(|| format!("CLUSTERDOWN Shard {} not ready", shard_id))?;

        // Read from state machine
        if let Some(sm) = state_machines.get(&raft_id.group) {
            let result = sm.store().apply(&cmd);
            Ok(apply_result_to_resp(result))
        } else {
            Err("State machine not found".to_string())
        }
    }

    /// Handle write command - through Raft consensus
    async fn handle_write(&self, cmd: Command) -> Result<RespValue, String> {
        // Get routing key
        let key = match cmd.get_key() {
            Some(k) => k,
            None => {
                // Global write commands like FlushDb need special handling
                return self.handle_global_write(cmd).await;
            }
        };

        // Determine shard
        let shard_id = self.router.route_key(key);

        // Check split status - return redirect if MOVED needed
        if let Some((_target_shard, target_addr)) =
            self.router.should_move_for_split(key, &shard_id)
        {
            let slot = crate::router::ShardRouter::slot_for_key(key);
            // Return MOVED error, format: MOVED <slot> <target_addr>
            return Err(format!("MOVED {} {}", slot, target_addr));
        }

        // Check if in buffering phase - return TRYAGAIN if so
        if self.router.should_buffer_for_split(key, &shard_id) {
            return Err("TRYAGAIN Split in progress, please retry".to_string());
        }

        // Get Raft group (must exist, created by Pilot)
        let raft_id = self
            .get_raft_group(&shard_id)
            .ok_or_else(|| format!("CLUSTERDOWN Shard {} not ready", shard_id))?;

        // Serialize command
        let serialized = bincode::serde::encode_to_vec(&cmd.clone(), bincode::config::standard())
            .map_err(|e| format!("Failed to serialize command: {}", e))?;

        // Generate request ID and register wait
        let request_id = RequestId::new();
        let result_rx = self.pending_requests.register(request_id, cmd);

        // Send event to Raft group
        let event = Event::ClientPropose {
            cmd: serialized,
            request_id,
        };

        match self.driver.dispatch_event(raft_id.clone(), event) {
            raft::multi_raft_driver::SendEventResult::Success => {}
            _ => {
                self.pending_requests.remove(request_id);
                return Err("Failed to dispatch event".to_string());
            }
        }

        // Wait for Raft commit and return result
        let timeout = self.config.raft.request_timeout();
        match tokio::time::timeout(timeout, result_rx).await {
            Ok(Ok(result)) => Ok(apply_result_to_resp(result)),
            Ok(Err(_)) => {
                // Channel closed - node may be shutting down
                Err("Request cancelled".to_string())
            }
            Err(_) => {
                // Timeout
                self.pending_requests.remove(request_id);
                Err("Request timeout".to_string())
            }
        }
    }

    /// Handle global write commands
    async fn handle_global_write(&self, cmd: Command) -> Result<RespValue, String> {
        match cmd {
            Command::FlushDb => {
                // Clear all shards
                let state_machines = self.state_machines.lock();
                for sm in state_machines.values() {
                    // HybridStore implements RedisStore, so we can call flushdb directly
                    let _ = sm.store().flushdb();
                }
                Ok(RespValue::SimpleString(bytes::Bytes::from("OK")))
            }
            _ => Err(format!("Unsupported global write command: {}", cmd.name())),
        }
    }

    /// Start node
    pub async fn start(&self) -> Result<(), String> {
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

/// Raft group event handler
struct RaftGroupHandler {
    raft_state: Arc<tokio::sync::Mutex<RaftState>>,
}

#[async_trait::async_trait]
impl raft::multi_raft_driver::HandleEventTrait for RaftGroupHandler {
    async fn handle_event(&self, event: raft::Event) {
        let mut state = self.raft_state.lock().await;
        state.handle_event(event).await;
    }
}

/// Async task to generate snapshot file using channel (chunk-based with zstd compression)
pub(crate) async fn generate_snapshot_file_async(
    shard_id: &str,
    transfer_id: &str,
    transfer_manager: &SnapshotTransferManager,
    store: Arc<storage::store::HybridStore>,
    snapshot_config: crate::config::SnapshotConfig,
) -> Result<(), String> {
    use std::io::Write;
    use crc32fast::Hasher as Crc32Hasher;
    
    info!(
        "Generating snapshot file for shard {} transfer {}",
        shard_id, transfer_id
    );

    // Get transfer state to access chunk_index and snapshot_path
    let state = transfer_manager.get_transfer_state(transfer_id)
        .ok_or_else(|| format!("Transfer {} not found", transfer_id))?;
    
    let (snapshot_path, chunk_index) = match state {
        crate::snapshot_transfer::SnapshotTransferState::Preparing { snapshot_path, chunk_index, .. } => {
            (snapshot_path, chunk_index)
        }
        _ => return Err(format!("Transfer {} is not in Preparing state", transfer_id)),
    };

    // Create channel for receiving snapshot data
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);

    // Spawn task to write data to file in chunks
    let file_path = snapshot_path.clone();
    let chunk_index_clone = chunk_index.clone();
    let transfer_id_clone = transfer_id.to_string();
    let write_handle = tokio::spawn(async move {
        use std::fs::File;
        use std::io::BufWriter;

        let file = File::create(&file_path)
            .map_err(|e| format!("Failed to create snapshot file: {}", e))?;
        let mut writer = BufWriter::new(file);

        // Chunk configuration from config (clone values for closure)
        let chunk_size = snapshot_config.chunk_size;
        let zstd_level = snapshot_config.zstd_level;

        let mut chunk_buffer = Vec::new();
        let mut chunk_index_counter = 0u32;
        let mut current_file_offset = 0u64;
        let mut total_uncompressed_size = 0u64;
        let mut total_compressed_size = 0u64;

        // Helper function to write a chunk
        let zstd_level = zstd_level; // Capture for closure
        let mut write_chunk = |buffer: &[u8], is_last: bool| -> Result<(), String> {
            // Compress with zstd
            let compressed = zstd::encode_all(buffer, zstd_level)
                .map_err(|e| format!("Failed to compress chunk: {}", e))?;

            // Calculate CRC32 of compressed data
            let mut hasher = Crc32Hasher::new();
            hasher.update(&compressed);
            let crc32 = hasher.finalize();

            // Write chunk header (metadata)
            let header = crate::snapshot_transfer::ChunkMetadata {
                chunk_index: chunk_index_counter,
                uncompressed_size: buffer.len() as u32,
                compressed_size: compressed.len() as u32,
                crc32,
                file_offset: current_file_offset,
                is_last,
            };

            // Serialize header with bincode
            let header_bytes = bincode::serde::encode_to_vec(&header, bincode::config::standard())
                .map_err(|e| format!("Failed to serialize chunk header: {}", e))?;

            // Write header length (u32) + header + compressed data
            let header_len = header_bytes.len() as u32;
            writer.write_all(&header_len.to_le_bytes())
                .map_err(|e| format!("Failed to write header length: {}", e))?;
            writer.write_all(&header_bytes)
                .map_err(|e| format!("Failed to write header: {}", e))?;
            writer.write_all(&compressed)
                .map_err(|e| format!("Failed to write compressed data: {}", e))?;

            // Update file offset (header_len (4) + header_bytes + compressed)
            let chunk_size = 4 + header_bytes.len() + compressed.len();
            current_file_offset += chunk_size as u64;

            // Update chunk index
            {
                let mut index = chunk_index_clone.write();
                index.chunks.push(header.clone());
                index.total_uncompressed_size += buffer.len() as u64;
                index.total_compressed_size += compressed.len() as u64;
                index.generated_chunks = chunk_index_counter + 1;
                if is_last {
                    index.is_complete = true;
                }
            }

            // Notify that new chunk is available
            chunk_index_clone.read().notify.notify_waiters();

            chunk_index_counter += 1;
            total_uncompressed_size += buffer.len() as u64;
            total_compressed_size += compressed.len() as u64;

            info!(
                "Chunk {} written for transfer {}: {} bytes (uncompressed) -> {} bytes (compressed)",
                chunk_index_counter - 1,
                transfer_id_clone,
                buffer.len(),
                compressed.len()
            );

            Ok(())
        };

        // Collect entries into chunks
        while let Some(entry) = rx.recv().await {
            match entry {
                storage::SnapshotStoreEntry::Error(err) => {
                    error!("Received error from snapshot creation: {}", err);
                    let mut index = chunk_index_clone.write();
                    index.error = Some(format!("Snapshot creation error: {}", err));
                    index.notify.notify_waiters();
                    return Err(format!("Snapshot creation error: {}", err));
                }
                storage::SnapshotStoreEntry::Completed => {
                    // Write remaining data as last chunk
                    if !chunk_buffer.is_empty() {
                        write_chunk(&chunk_buffer, true)?;
                        chunk_buffer.clear();
                    } else {
                        // Write empty last chunk if no data
                        write_chunk(&[], true)?;
                    }
                    info!("Snapshot data transfer completed");
                    break;
                }
                _ => {
                    // Serialize entry using bincode
                    let serialized = bincode::serde::encode_to_vec(&entry, bincode::config::standard())
                        .map_err(|e| format!("Failed to serialize entry: {}", e))?;
                    
                    // Check if adding this entry would exceed chunk size
                    if chunk_buffer.len() + serialized.len() > chunk_size && !chunk_buffer.is_empty() {
                        // Write current chunk
                        write_chunk(&chunk_buffer, false)?;
                        chunk_buffer.clear();
                    }

                    // Add entry to chunk buffer
                    // Write entry length (u32) followed by serialized data
                    let entry_len = serialized.len() as u32;
                    chunk_buffer.extend_from_slice(&entry_len.to_le_bytes());
                    chunk_buffer.extend_from_slice(&serialized);
                }
            }
        }

        writer.flush()
            .map_err(|e| format!("Failed to flush snapshot file: {}", e))?;
        
        // Sync file to disk
        writer.get_ref().sync_all()
            .map_err(|e| format!("Failed to sync snapshot file: {}", e))?;

        Ok::<(u64, u64), String>((total_uncompressed_size, total_compressed_size))
    });

    // Synchronously wait for snapshot object creation in load_snapshot context
    // This ensures state consistency - snapshot object (RocksDB snapshot + Memory COW)
    // is created before returning metadata, preventing state changes
    let shard_id_str = shard_id.to_string();
    
    // Use block_in_place to wait synchronously for snapshot object creation
    // create_snapshot uses spawn_blocking internally and signals via oneshot when snapshot is ready
    // We block here to ensure snapshot object is created in synchronous context
    tokio::task::block_in_place(|| {
        use storage::SnapshotStore;
        // Block on the async create_snapshot call
        // This will wait for the snapshot object to be created (via oneshot in create_snapshot)
        // before returning, ensuring state consistency
        tokio::runtime::Handle::current().block_on(
            store.create_snapshot(&shard_id_str, tx)
        )
    })
    .map_err(|e| format!("Failed to create snapshot: {}", e))?;

    // Wait for file writing to complete
    let (total_uncompressed, total_compressed) = write_handle
        .await
        .map_err(|e| format!("File writing task failed: {:?}", e))??;

    // Update transfer state to Ready
    transfer_manager.update_transfer_state(
        transfer_id,
        crate::snapshot_transfer::SnapshotTransferState::Ready {
            snapshot_path: snapshot_path.clone(),
            total_size: total_compressed,
            chunk_index,
        },
    );

    info!(
        "Snapshot file generated for transfer {}: {} bytes (uncompressed) -> {} bytes (compressed)",
        transfer_id, total_uncompressed, total_compressed
    );

    Ok(())
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
