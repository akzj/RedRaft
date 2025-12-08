//! Pilot control plane client
//!
//! Responsible for communicating with Pilot service: registration, heartbeat, fetching routing table

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use raft::NodeId;

// Re-export RaftGroupStatus from pilot crate
pub use pilot::RaftGroupStatus;

/// Pilot client error
#[derive(Debug, thiserror::Error)]
pub enum PilotError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {0}")]
    Api(String),
    #[error("Not connected to pilot")]
    NotConnected,
}

/// Routing table (fetched from Pilot)
/// 
/// Note: Uses ShardId (business layer concept), not GroupId (Raft layer concept)
/// When creating Raft groups, ShardId is passed as GroupId
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingTable {
    /// Version number
    pub version: u64,
    /// Slot to shard mapping (uses ShardId, business layer concept)
    pub slots: Vec<Option<String>>,
    /// Shard to nodes mapping (first is leader)
    /// Uses ShardId as key (business layer concept)
    pub shard_nodes: HashMap<String, Vec<NodeId>>,
    /// Node address mapping
    pub node_addrs: HashMap<NodeId, String>,
    /// Splitting shard information (source_shard_id -> SplitInfo)
    #[serde(default)]
    pub splitting_shards: HashMap<String, ShardSplitInfo>,
}

/// Shard split information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSplitInfo {
    /// Split point slot
    pub split_slot: u32,
    /// Target shard ID (business layer ShardId, will be used as GroupId when creating Raft group)
    pub target_shard: String,
    /// Split status
    pub status: String,
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self {
            version: 0,
            slots: vec![None; 16384],
            shard_nodes: HashMap::new(),
            node_addrs: HashMap::new(),
            splitting_shards: HashMap::new(),
        }
    }
}

impl RoutingTable {
    /// Calculate slot for key
    pub fn slot_for_key(key: &[u8]) -> u32 {
        crc16(key) as u32 % 16384
    }

    /// Get target node address for key
    pub fn get_node_addr_for_key(&self, key: &[u8]) -> Option<&String> {
        let slot = Self::slot_for_key(key);
        let shard_id = self.slots.get(slot as usize)?.as_ref()?;
        let nodes = self.shard_nodes.get(shard_id)?;
        let leader = nodes.first()?;
        self.node_addrs.get(leader)
    }

    /// Get shard ID (business layer ShardId)
    pub fn get_shard_for_key(&self, key: &[u8]) -> Option<&String> {
        let slot = Self::slot_for_key(key);
        self.slots.get(slot as usize)?.as_ref()
    }

    /// Check if shard is splitting
    pub fn is_shard_splitting(&self, shard_id: &str) -> bool {
        self.splitting_shards.contains_key(shard_id)
    }

    /// Get shard split information
    pub fn get_split_info(&self, shard_id: &str) -> Option<&ShardSplitInfo> {
        self.splitting_shards.get(shard_id)
    }

    /// Check if key is in split target range (should MOVED to new shard)
    /// 
    /// Returns (target_shard_id, target_leader_addr)
    pub fn should_move_key(&self, key: &[u8], shard_id: &str) -> Option<(&String, &String)> {
        let split_info = self.splitting_shards.get(shard_id)?;
        let slot = Self::slot_for_key(key);
        
        // If slot >= split_slot, should move to target shard
        if slot >= split_info.split_slot {
            // Get target shard's leader address
            let target_nodes = self.shard_nodes.get(&split_info.target_shard)?;
            let target_leader = target_nodes.first()?;
            let target_addr = self.node_addrs.get(target_leader)?;
            Some((&split_info.target_shard, target_addr))
        } else {
            None
        }
    }
}

/// CRC16 implementation (compatible with Redis Cluster)
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for byte in data {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Pilot client configuration
#[derive(Debug, Clone)]
pub struct PilotClientConfig {
    /// Pilot service address
    pub pilot_addr: String,
    /// Heartbeat interval (seconds)
    pub heartbeat_interval_secs: u64,
    /// Routing table refresh interval (seconds)
    pub routing_refresh_interval_secs: u64,
    /// Request timeout (seconds)
    pub request_timeout_secs: u64,
}

impl Default for PilotClientConfig {
    fn default() -> Self {
        Self {
            pilot_addr: "http://127.0.0.1:8080".to_string(),
            heartbeat_interval_secs: 10,
            routing_refresh_interval_secs: 30,
            request_timeout_secs: 5,
        }
    }
}

/// Pilot client
pub struct PilotClient {
    config: PilotClientConfig,
    node_id: String,
    grpc_addr: String,
    redis_addr: String,
    http_client: reqwest::Client,
    routing_table: Arc<RwLock<RoutingTable>>,
    connected: Arc<RwLock<bool>>,
}

impl PilotClient {
    /// Create Pilot client
    pub fn new(
        config: PilotClientConfig,
        node_id: String,
        grpc_addr: String,
        redis_addr: String,
    ) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            config,
            node_id,
            grpc_addr,
            redis_addr,
            http_client,
            routing_table: Arc::new(RwLock::new(RoutingTable::default())),
            connected: Arc::new(RwLock::new(false)),
        }
    }

    /// Get routing table reference
    pub fn routing_table(&self) -> Arc<RwLock<RoutingTable>> {
        self.routing_table.clone()
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        *self.connected.read()
    }

    /// Register node with Pilot
    pub async fn register(&self) -> Result<bool, PilotError> {
        #[derive(Serialize)]
        struct RegisterRequest {
            node_id: String,
            grpc_addr: String,
            redis_addr: String,
        }

        #[derive(Deserialize)]
        struct ApiResponse<T> {
            success: bool,
            data: Option<T>,
            error: Option<String>,
        }

        #[derive(Deserialize)]
        struct RegisterResponse {
            is_new: bool,
        }

        let url = format!("{}/api/v1/nodes", self.config.pilot_addr);
        let req = RegisterRequest {
            node_id: self.node_id.clone(),
            grpc_addr: self.grpc_addr.clone(),
            redis_addr: self.redis_addr.clone(),
        };

        let resp: ApiResponse<RegisterResponse> = self
            .http_client
            .post(&url)
            .json(&req)
            .send()
            .await?
            .json()
            .await?;

        if resp.success {
            *self.connected.write() = true;
            let is_new = resp.data.map(|d| d.is_new).unwrap_or(false);
            if is_new {
                info!("Registered as new node with pilot");
            } else {
                info!("Re-registered with pilot (reconnected)");
            }
            Ok(is_new)
        } else {
            Err(PilotError::Api(resp.error.unwrap_or_default()))
        }
    }

    /// Send heartbeat
    pub async fn heartbeat(&self) -> Result<(), PilotError> {
        #[derive(Deserialize)]
        struct ApiResponse<T> {
            success: bool,
            #[allow(dead_code)]
            data: Option<T>,
            error: Option<String>,
        }

        let url = format!(
            "{}/api/v1/nodes/{}/heartbeat",
            self.config.pilot_addr, self.node_id
        );

        let resp: ApiResponse<()> = self
            .http_client
            .post(&url)
            .send()
            .await?
            .json()
            .await?;

        if resp.success {
            debug!("Heartbeat sent to pilot");
            Ok(())
        } else {
            *self.connected.write() = false;
            Err(PilotError::Api(resp.error.unwrap_or_default()))
        }
    }

    /// Fetch routing table
    /// 
    /// # Arguments
    /// - `current_version`: Current routing table version number (optional)
    ///   If provided and matches server version, register watch and wait for updates
    /// 
    /// # Returns
    /// - `Ok(RoutingTable)`: Latest routing table
    /// - `Err(PilotError)`: Fetch failed
    pub async fn fetch_routing_table(&self, current_version: Option<u64>) -> Result<RoutingTable, PilotError> {
        #[derive(Deserialize)]
        struct ApiResponse<T> {
            success: bool,
            data: Option<T>,
            error: Option<String>,
        }

        let mut url = format!("{}/api/v1/routing", self.config.pilot_addr);
        
        // If version provided, add to query parameters
        if let Some(version) = current_version {
            url = format!("{}?version={}", url, version);
        }

        let resp: ApiResponse<RoutingTable> = self
            .http_client
            .get(&url)
            .send()
            .await?
            .json()
            .await?;

        if resp.success {
            let table = resp.data.ok_or_else(|| PilotError::Api("No data".into()))?;
            debug!("Fetched routing table version {}", table.version);
            Ok(table)
        } else {
            Err(PilotError::Api(resp.error.unwrap_or_default()))
        }
    }

    /// Refresh local routing table
    pub async fn refresh_routing(&self) -> Result<bool, PilotError> {
        // Get current version number
        let current_version = self.routing_table.read().version;
        
        // Fetch latest routing table (if version matches, will wait for updates)
        let new_table = self.fetch_routing_table(Some(current_version)).await?;
        let mut current = self.routing_table.write();
        
        if new_table.version > current.version {
            info!(
                "Routing table updated: version {} -> {}",
                current.version, new_table.version
            );
            *current = new_table;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Report Raft group status to Pilot
    /// 
    /// # Arguments
    /// - `shard_id`: Shard ID (business layer ShardId)
    /// - `status`: Raft group status
    /// - `error`: Optional error message if status is "failed"
    pub async fn report_shard_status(
        &self,
        shard_id: &str,
        status: RaftGroupStatus,
        error: Option<&str>,
    ) -> Result<(), PilotError> {
        #[derive(Serialize)]
        struct ReportRequest {
            status: RaftGroupStatus,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<String>,
        }

        #[derive(Deserialize)]
        struct ApiResponse<T> {
            success: bool,
            #[allow(dead_code)]
            data: Option<T>,
            error: Option<String>,
        }

        let url = format!(
            "{}/api/v1/nodes/{}/shards/{}/status",
            self.config.pilot_addr, self.node_id, shard_id
        );

        let req = ReportRequest {
            status,
            error: error.map(|e| e.to_string()),
        };

        let resp: ApiResponse<()> = self
            .http_client
            .post(&url)
            .json(&req)
            .send()
            .await?
            .json()
            .await?;

        if resp.success {
            debug!("Reported shard {} status: {} to pilot", shard_id, status);
            Ok(())
        } else {
            let error_msg = resp.error.clone().unwrap_or_default();
            warn!(
                "Failed to report shard {} status to pilot: {}",
                shard_id, error_msg
            );
            Err(PilotError::Api(error_msg))
        }
    }

    /// Start background tasks (heartbeat + routing refresh)
    pub fn start_background_tasks(self: Arc<Self>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();

        // Heartbeat task
        let client = self.clone();
        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(
                client.config.heartbeat_interval_secs,
            ));
            
            loop {
                interval.tick().await;
                
                if let Err(e) = client.heartbeat().await {
                    warn!("Heartbeat failed: {}, attempting to re-register", e);
                    // Try to re-register
                    if let Err(e) = client.register().await {
                        error!("Re-registration failed: {}", e);
                    }
                }
            }
        });
        handles.push(heartbeat_handle);

        // Routing refresh task
        let client = self.clone();
        let routing_handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(
                client.config.routing_refresh_interval_secs,
            ));
            
            loop {
                interval.tick().await;
                
                if let Err(e) = client.refresh_routing().await {
                    warn!("Routing refresh failed: {}", e);
                }
            }
        });
        handles.push(routing_handle);

        handles
    }

    /// Connect and initialize
    pub async fn connect(&self) -> Result<(), PilotError> {
        // Register node
        self.register().await?;
        
        // Fetch initial routing table
        self.refresh_routing().await?;
        
        info!(
            "Connected to pilot at {}, routing version {}",
            self.config.pilot_addr,
            self.routing_table.read().version
        );
        
        Ok(())
    }
}
