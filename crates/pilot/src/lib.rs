//! Pilot - Distributed control plane
//!
//! Responsible for cluster shard management, routing management, and data migration
//!
//! # Features
//! - Node registration and heartbeat management
//! - Shard creation and placement
//! - Routing table maintenance and distribution
//! - Data migration coordination
//!
//! # Usage Example
//! ```ignore
//! use pilot::{Pilot, PilotConfig};
//!
//! let config = PilotConfig::default();
//! let pilot = Pilot::new(config).await?;
//! pilot.run().await?;
//! ```

pub mod metadata;
pub mod storage;
pub mod node_manager;
pub mod scheduler;
pub mod api;
pub mod watch;

// Re-export commonly used types
pub use metadata::RaftGroupStatus;

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use metadata::ClusterMetadata;
use node_manager::{NodeManager, NodeManagerConfig};
use scheduler::Scheduler;
use storage::{FileStorage, StorageError};
use watch::RoutingTableWatcher;

/// Pilot configuration
#[derive(Debug, Clone)]
pub struct PilotConfig {
    /// Cluster name
    pub cluster_name: String,
    /// Data directory
    pub data_dir: String,
    /// HTTP API listen address
    pub http_addr: String,
    /// Node manager configuration
    pub node_manager: NodeManagerConfig,
}

impl Default for PilotConfig {
    fn default() -> Self {
        Self {
            cluster_name: "default".to_string(),
            data_dir: "./pilot_data".to_string(),
            http_addr: "0.0.0.0:8080".to_string(),
            node_manager: NodeManagerConfig::default(),
        }
    }
}

/// Pilot control plane
pub struct Pilot {
    config: PilotConfig,
    storage: FileStorage,
    metadata: Arc<RwLock<ClusterMetadata>>,
    node_manager: Arc<NodeManager>,
    scheduler: Scheduler,
    routing_watcher: RoutingTableWatcher,
}

impl Pilot {
    /// Create Pilot instance
    pub async fn new(config: PilotConfig) -> Result<Self, StorageError> {
        let storage = FileStorage::new(&config.data_dir);
        let metadata = storage.load_or_create(&config.cluster_name).await?;
        let metadata = Arc::new(RwLock::new(metadata));

        let node_manager = Arc::new(NodeManager::new(
            config.node_manager.clone(),
            metadata.clone(),
        ));

        let scheduler = Scheduler::new(metadata.clone());
        let routing_watcher = RoutingTableWatcher::new();

        info!(
            "Pilot initialized: cluster={}, data_dir={}",
            config.cluster_name, config.data_dir
        );

        Ok(Self {
            config,
            storage,
            metadata,
            node_manager,
            scheduler,
            routing_watcher,
        })
    }

    /// Get configuration
    pub fn config(&self) -> &PilotConfig {
        &self.config
    }

    /// Get metadata (read-only)
    pub async fn metadata(&self) -> ClusterMetadata {
        self.metadata.read().await.clone()
    }

    /// Get metadata (writable)
    /// 
    /// Note: After updating routing table, will automatically notify waiting watches
    pub async fn metadata_mut(&self) -> tokio::sync::RwLockWriteGuard<'_, ClusterMetadata> {
        self.metadata.write().await
    }

    /// Notify routing table watches (call after routing table updates)
    pub async fn notify_routing_watchers(&self) {
        let version = self.metadata.read().await.routing_table.version;
        self.routing_watcher.notify_version(version).await;
    }

    /// Get routing table
    pub async fn routing_table(&self) -> metadata::RoutingTable {
        self.metadata.read().await.routing_table.clone()
    }

    /// Get node manager
    pub fn node_manager(&self) -> &Arc<NodeManager> {
        &self.node_manager
    }

    /// Get scheduler
    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// Get routing table watch manager
    pub fn routing_watcher(&self) -> &RoutingTableWatcher {
        &self.routing_watcher
    }

    /// Save metadata
    pub async fn save(&self) -> Result<(), StorageError> {
        let metadata = self.metadata.read().await;
        self.storage.save(&metadata).await
    }

    /// Start heartbeat checker
    pub fn start_heartbeat_checker(&self) -> tokio::task::JoinHandle<()> {
        self.node_manager.clone().start_heartbeat_checker()
    }

    /// Start periodic save task
    pub fn start_periodic_save(self: Arc<Self>, interval_secs: u64) -> tokio::task::JoinHandle<()> {
        use tokio::time::{interval, Duration};
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                if let Err(e) = self.save().await {
                    tracing::error!("Failed to save metadata: {}", e);
                }
            }
        })
    }
}
