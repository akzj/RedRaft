//! Pilot - 分布式控制面
//!
//! 负责集群的分片管理、路由管理和数据迁移
//!
//! # 功能
//! - 节点注册与心跳管理
//! - 分片创建与放置
//! - 路由表维护与分发
//! - 数据迁移协调
//!
//! # 使用示例
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

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use metadata::ClusterMetadata;
use node_manager::{NodeManager, NodeManagerConfig};
use scheduler::Scheduler;
use storage::{FileStorage, StorageError};

/// Pilot 配置
#[derive(Debug, Clone)]
pub struct PilotConfig {
    /// 集群名称
    pub cluster_name: String,
    /// 数据目录
    pub data_dir: String,
    /// HTTP API 监听地址
    pub http_addr: String,
    /// 节点管理器配置
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

/// Pilot 控制面
pub struct Pilot {
    config: PilotConfig,
    storage: FileStorage,
    metadata: Arc<RwLock<ClusterMetadata>>,
    node_manager: Arc<NodeManager>,
    scheduler: Scheduler,
}

impl Pilot {
    /// 创建 Pilot 实例
    pub async fn new(config: PilotConfig) -> Result<Self, StorageError> {
        let storage = FileStorage::new(&config.data_dir);
        let metadata = storage.load_or_create(&config.cluster_name).await?;
        let metadata = Arc::new(RwLock::new(metadata));

        let node_manager = Arc::new(NodeManager::new(
            config.node_manager.clone(),
            metadata.clone(),
        ));

        let scheduler = Scheduler::new(metadata.clone());

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
        })
    }

    /// 获取配置
    pub fn config(&self) -> &PilotConfig {
        &self.config
    }

    /// 获取元数据（只读）
    pub async fn metadata(&self) -> ClusterMetadata {
        self.metadata.read().await.clone()
    }

    /// 获取元数据（可写）
    pub async fn metadata_mut(&self) -> tokio::sync::RwLockWriteGuard<'_, ClusterMetadata> {
        self.metadata.write().await
    }

    /// 获取路由表
    pub async fn routing_table(&self) -> metadata::RoutingTable {
        self.metadata.read().await.routing_table.clone()
    }

    /// 获取节点管理器
    pub fn node_manager(&self) -> &Arc<NodeManager> {
        &self.node_manager
    }

    /// 获取调度器
    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// 保存元数据
    pub async fn save(&self) -> Result<(), StorageError> {
        let metadata = self.metadata.read().await;
        self.storage.save(&metadata).await
    }

    /// 启动心跳检查
    pub fn start_heartbeat_checker(&self) -> tokio::task::JoinHandle<()> {
        self.node_manager.clone().start_heartbeat_checker()
    }

    /// 启动定期保存任务
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
