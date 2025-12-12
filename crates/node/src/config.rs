//! Configuration module for RedRaft node
//!
//! Supports YAML configuration files with module-based organization

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Node configuration
    pub node: NodeConfig,
    /// Network configuration
    pub network: NetworkConfig,
    /// Storage configuration
    pub storage: StorageConfig,
    /// Snapshot configuration
    pub snapshot: SnapshotConfig,
    /// Raft configuration
    pub raft: RaftConfig,
    /// Pilot configuration (optional)
    #[serde(default)]
    pub pilot: Option<PilotConfig>,
    /// Server configuration
    pub server: ServerConfig,
    /// Logging configuration
    pub log: LogConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            network: NetworkConfig::default(),
            storage: StorageConfig::default(),
            snapshot: SnapshotConfig::default(),
            raft: RaftConfig::default(),
            pilot: None,
            server: ServerConfig::default(),
            log: LogConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from YAML file
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let content =
            std::fs::read_to_string(path).map_err(|e| ConfigError::IoError(e.to_string()))?;
        Self::from_yaml(&content)
    }

    /// Load configuration from YAML string
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        serde_yaml::from_str(yaml).map_err(|e| ConfigError::ParseError(e.to_string()))
    }

    /// Save configuration to YAML file
    pub fn to_file(&self, path: impl AsRef<std::path::Path>) -> Result<(), ConfigError> {
        let yaml =
            serde_yaml::to_string(self).map_err(|e| ConfigError::SerializeError(e.to_string()))?;
        std::fs::write(path, yaml).map_err(|e| ConfigError::IoError(e.to_string()))?;
        Ok(())
    }

    /// Merge with another config (other takes precedence)
    pub fn merge(&mut self, other: Config) {
        self.node.merge(other.node);
        self.network.merge(other.network);
        self.storage.merge(other.storage);
        self.snapshot.merge(other.snapshot);
        self.raft.merge(other.raft);
        if other.pilot.is_some() {
            self.pilot = other.pilot;
        }
        self.server.merge(other.server);
        self.log.merge(other.log);
    }
}

/// Node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node ID
    pub node_id: String,
    /// Shard count (only used when no pilot)
    #[serde(default = "default_shard_count")]
    pub shard_count: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: "node1".to_string(),
            shard_count: 3,
        }
    }
}

impl NodeConfig {
    fn merge(&mut self, other: Self) {
        if !other.node_id.is_empty() {
            self.node_id = other.node_id;
        }
        if other.shard_count > 0 {
            self.shard_count = other.shard_count;
        }
    }
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Redis server listen address
    pub redis_addr: String,
    /// gRPC service address (for Raft communication)
    pub grpc_addr: String,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            redis_addr: "127.0.0.1:6379".to_string(),
            grpc_addr: "127.0.0.1:50051".to_string(),
        }
    }
}

impl NetworkConfig {
    fn merge(&mut self, other: Self) {
        if !other.redis_addr.is_empty() {
            self.redis_addr = other.redis_addr;
        }
        if !other.grpc_addr.is_empty() {
            self.grpc_addr = other.grpc_addr;
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Data storage directory
    pub data_dir: PathBuf,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
        }
    }
}

impl StorageConfig {
    fn merge(&mut self, other: Self) {
        if !other.data_dir.as_os_str().is_empty() {
            self.data_dir = other.data_dir;
        }
    }
}

/// Snapshot configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfig {
    /// Chunk size in bytes (uncompressed)
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    /// ZSTD compression level (1-22)
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,
    /// Chunk wait check interval in milliseconds
    #[serde(default = "default_chunk_check_interval_ms")]
    pub chunk_check_interval_ms: u64,
    /// Chunk wait timeout in seconds
    #[serde(default = "default_chunk_timeout_secs")]
    pub chunk_timeout_secs: u64,
    /// Snapshot transfer directory base path
    #[serde(default = "default_snapshot_transfer_dir")]
    pub transfer_dir: PathBuf,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            chunk_size: default_chunk_size(),
            zstd_level: default_zstd_level(),
            chunk_check_interval_ms: default_chunk_check_interval_ms(),
            chunk_timeout_secs: default_chunk_timeout_secs(),
            transfer_dir: default_snapshot_transfer_dir(),
        }
    }
}

impl SnapshotConfig {
    fn merge(&mut self, other: Self) {
        if other.chunk_size > 0 {
            self.chunk_size = other.chunk_size;
        }
        if other.zstd_level > 0 {
            self.zstd_level = other.zstd_level;
        }
        if other.chunk_check_interval_ms > 0 {
            self.chunk_check_interval_ms = other.chunk_check_interval_ms;
        }
        if other.chunk_timeout_secs > 0 {
            self.chunk_timeout_secs = other.chunk_timeout_secs;
        }
        if !other.transfer_dir.as_os_str().is_empty() {
            self.transfer_dir = other.transfer_dir;
        }
    }

    /// Get chunk check interval as Duration
    pub fn chunk_check_interval(&self) -> Duration {
        Duration::from_millis(self.chunk_check_interval_ms)
    }

    /// Get chunk timeout as Duration
    pub fn chunk_timeout(&self) -> Duration {
        Duration::from_secs(self.chunk_timeout_secs)
    }
}

/// Raft configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Request timeout in seconds
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// Election timeout in milliseconds
    #[serde(default = "default_election_timeout_ms")]
    pub election_timeout_ms: u64,
    /// Heartbeat timeout in milliseconds
    #[serde(default = "default_heartbeat_timeout_ms")]
    pub heartbeat_timeout_ms: u64,
    /// Leader transfer timeout in milliseconds
    #[serde(default = "default_leader_transfer_timeout_ms")]
    pub leader_transfer_timeout_ms: u64,
    /// Apply log timeout in milliseconds
    #[serde(default = "default_apply_log_timeout_ms")]
    pub apply_log_timeout_ms: u64,
    /// Config change timeout in milliseconds
    #[serde(default = "default_config_change_timeout_ms")]
    pub config_change_timeout_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            request_timeout_secs: default_request_timeout_secs(),
            election_timeout_ms: default_election_timeout_ms(),
            heartbeat_timeout_ms: default_heartbeat_timeout_ms(),
            leader_transfer_timeout_ms: default_leader_transfer_timeout_ms(),
            apply_log_timeout_ms: default_apply_log_timeout_ms(),
            config_change_timeout_ms: default_config_change_timeout_ms(),
        }
    }
}

impl RaftConfig {
    fn merge(&mut self, other: Self) {
        if other.request_timeout_secs > 0 {
            self.request_timeout_secs = other.request_timeout_secs;
        }
        if other.election_timeout_ms > 0 {
            self.election_timeout_ms = other.election_timeout_ms;
        }
        if other.heartbeat_timeout_ms > 0 {
            self.heartbeat_timeout_ms = other.heartbeat_timeout_ms;
        }
        if other.leader_transfer_timeout_ms > 0 {
            self.leader_transfer_timeout_ms = other.leader_transfer_timeout_ms;
        }
        if other.apply_log_timeout_ms > 0 {
            self.apply_log_timeout_ms = other.apply_log_timeout_ms;
        }
        if other.config_change_timeout_ms > 0 {
            self.config_change_timeout_ms = other.config_change_timeout_ms;
        }
    }

    /// Get request timeout as Duration
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }

    /// Get election timeout as Duration
    pub fn election_timeout(&self) -> Duration {
        Duration::from_millis(self.election_timeout_ms)
    }

    /// Get heartbeat timeout as Duration
    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_millis(self.heartbeat_timeout_ms)
    }

    /// Get leader transfer timeout as Duration
    pub fn leader_transfer_timeout(&self) -> Duration {
        Duration::from_millis(self.leader_transfer_timeout_ms)
    }

    /// Get apply log timeout as Duration
    pub fn apply_log_timeout(&self) -> Duration {
        Duration::from_millis(self.apply_log_timeout_ms)
    }

    /// Get config change timeout as Duration
    pub fn config_change_timeout(&self) -> Duration {
        Duration::from_millis(self.config_change_timeout_ms)
    }
}

/// Pilot configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PilotConfig {
    /// Pilot service address
    pub pilot_addr: String,
    /// Heartbeat interval in seconds
    #[serde(default = "default_pilot_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,
    /// Routing table refresh interval in seconds
    #[serde(default = "default_pilot_routing_refresh_interval_secs")]
    pub routing_refresh_interval_secs: u64,
    /// Request timeout in seconds
    #[serde(default = "default_pilot_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

impl Default for PilotConfig {
    fn default() -> Self {
        Self {
            pilot_addr: "http://127.0.0.1:8080".to_string(),
            heartbeat_interval_secs: default_pilot_heartbeat_interval_secs(),
            routing_refresh_interval_secs: default_pilot_routing_refresh_interval_secs(),
            request_timeout_secs: default_pilot_request_timeout_secs(),
        }
    }
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Routing table sync interval in seconds
    #[serde(default = "default_routing_sync_interval_secs")]
    pub routing_sync_interval_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            routing_sync_interval_secs: default_routing_sync_interval_secs(),
        }
    }
}

impl ServerConfig {
    fn merge(&mut self, other: Self) {
        if other.routing_sync_interval_secs > 0 {
            self.routing_sync_interval_secs = other.routing_sync_interval_secs;
        }
    }

    /// Get routing sync interval as Duration
    pub fn routing_sync_interval(&self) -> Duration {
        Duration::from_secs(self.routing_sync_interval_secs)
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// Log level (trace, debug, info, warn, error)
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

impl LogConfig {
    fn merge(&mut self, other: Self) {
        if !other.level.is_empty() {
            self.level = other.level;
        }
    }
}

// Default value functions

fn default_shard_count() -> usize {
    3
}

fn default_chunk_size() -> usize {
    64 * 1024 * 1024 // 64MB
}

fn default_zstd_level() -> i32 {
    3
}

fn default_chunk_check_interval_ms() -> u64 {
    100 // 100ms
}

fn default_chunk_timeout_secs() -> u64 {
    300 // 5 minutes
}

fn default_snapshot_transfer_dir() -> PathBuf {
    PathBuf::from("./data/snapshot_transfers")
}

fn default_request_timeout_secs() -> u64 {
    5
}

fn default_election_timeout_ms() -> u64 {
    1000 // 1 second
}

fn default_heartbeat_timeout_ms() -> u64 {
    100 // 100ms
}

fn default_leader_transfer_timeout_ms() -> u64 {
    5000 // 5 seconds
}

fn default_apply_log_timeout_ms() -> u64 {
    1000 // 1 second
}

fn default_config_change_timeout_ms() -> u64 {
    10000 // 10 seconds
}

fn default_pilot_heartbeat_interval_secs() -> u64 {
    10
}

fn default_pilot_routing_refresh_interval_secs() -> u64 {
    30
}

fn default_pilot_request_timeout_secs() -> u64 {
    5
}

fn default_routing_sync_interval_secs() -> u64 {
    5
}

fn default_log_level() -> String {
    "info".to_string()
}

/// Configuration error
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    IoError(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Serialize error: {0}")]
    SerializeError(String),
}
