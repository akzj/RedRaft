//! Pilot 控制面客户端
//!
//! 负责与 Pilot 服务通信：注册、心跳、获取路由表

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Pilot 客户端错误
#[derive(Debug, thiserror::Error)]
pub enum PilotError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {0}")]
    Api(String),
    #[error("Not connected to pilot")]
    NotConnected,
}

/// 路由表（从 Pilot 获取）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingTable {
    /// 版本号
    pub version: u64,
    /// 槽位到分片的映射
    pub slots: Vec<Option<String>>,
    /// 分片到节点的映射（第一个为 leader）
    pub shard_nodes: HashMap<String, Vec<String>>,
    /// 节点地址映射
    pub node_addrs: HashMap<String, String>,
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self {
            version: 0,
            slots: vec![None; 16384],
            shard_nodes: HashMap::new(),
            node_addrs: HashMap::new(),
        }
    }
}

impl RoutingTable {
    /// 计算 key 的槽位
    pub fn slot_for_key(key: &[u8]) -> u32 {
        crc16(key) as u32 % 16384
    }

    /// 根据 key 获取目标节点地址
    pub fn get_node_addr_for_key(&self, key: &[u8]) -> Option<&String> {
        let slot = Self::slot_for_key(key);
        let shard_id = self.slots.get(slot as usize)?.as_ref()?;
        let nodes = self.shard_nodes.get(shard_id)?;
        let leader = nodes.first()?;
        self.node_addrs.get(leader)
    }

    /// 获取分片 ID
    pub fn get_shard_for_key(&self, key: &[u8]) -> Option<&String> {
        let slot = Self::slot_for_key(key);
        self.slots.get(slot as usize)?.as_ref()
    }
}

/// CRC16 实现（与 Redis Cluster 兼容）
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

/// Pilot 客户端配置
#[derive(Debug, Clone)]
pub struct PilotClientConfig {
    /// Pilot 服务地址
    pub pilot_addr: String,
    /// 心跳间隔（秒）
    pub heartbeat_interval_secs: u64,
    /// 路由表刷新间隔（秒）
    pub routing_refresh_interval_secs: u64,
    /// 请求超时（秒）
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

/// Pilot 客户端
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
    /// 创建 Pilot 客户端
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

    /// 获取路由表引用
    pub fn routing_table(&self) -> Arc<RwLock<RoutingTable>> {
        self.routing_table.clone()
    }

    /// 检查是否已连接
    pub fn is_connected(&self) -> bool {
        *self.connected.read()
    }

    /// 向 Pilot 注册节点
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

    /// 发送心跳
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

    /// 获取路由表
    pub async fn fetch_routing_table(&self) -> Result<RoutingTable, PilotError> {
        #[derive(Deserialize)]
        struct ApiResponse<T> {
            success: bool,
            data: Option<T>,
            error: Option<String>,
        }

        let url = format!("{}/api/v1/routing", self.config.pilot_addr);

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

    /// 刷新本地路由表
    pub async fn refresh_routing(&self) -> Result<bool, PilotError> {
        let new_table = self.fetch_routing_table().await?;
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

    /// 启动后台任务（心跳 + 路由刷新）
    pub fn start_background_tasks(self: Arc<Self>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();

        // 心跳任务
        let client = self.clone();
        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(
                client.config.heartbeat_interval_secs,
            ));
            
            loop {
                interval.tick().await;
                
                if let Err(e) = client.heartbeat().await {
                    warn!("Heartbeat failed: {}, attempting to re-register", e);
                    // 尝试重新注册
                    if let Err(e) = client.register().await {
                        error!("Re-registration failed: {}", e);
                    }
                }
            }
        });
        handles.push(heartbeat_handle);

        // 路由刷新任务
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

    /// 连接并初始化
    pub async fn connect(&self) -> Result<(), PilotError> {
        // 注册节点
        self.register().await?;
        
        // 获取初始路由表
        self.refresh_routing().await?;
        
        info!(
            "Connected to pilot at {}, routing version {}",
            self.config.pilot_addr,
            self.routing_table.read().version
        );
        
        Ok(())
    }
}
