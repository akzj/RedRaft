//! RedRaft 节点实现
//!
//! 集成 Multi-Raft、KV 状态机、路由和存储

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tracing::{debug, info};

use raft::{
    ClusterConfig, ClusterConfigStorage, Event,
    HardStateStorage, LogEntryStorage, Network, RaftCallbacks, RaftId, RaftState,
    RaftStateOptions, SnapshotStorage, StateMachine, Storage, StorageResult,
    TimerService, message::{PreVoteRequest, PreVoteResponse}, traits::ClientResult,
};

use crate::router::ShardRouter;
use crate::state_machine::KVStateMachine;
use redisstore::MemoryStore;
use resp::Command;

/// RedRaft 节点
pub struct RedRaftNode {
    /// 节点 ID
    node_id: String,
    /// Multi-Raft 驱动器
    driver: raft::multi_raft_driver::MultiRaftDriver,
    /// 存储后端
    storage: Arc<dyn Storage>,
    /// 网络层
    network: Arc<dyn Network>,
    /// Shard Router
    router: Arc<ShardRouter>,
    /// Raft 组状态机映射 (shard_id -> state_machine)
    state_machines: Arc<Mutex<HashMap<String, Arc<KVStateMachine>>>>,
}

impl RedRaftNode {
    pub fn new(
        node_id: String,
        storage: Arc<dyn Storage>,
        network: Arc<dyn Network>,
        shard_count: usize,
    ) -> Self {
        Self {
            node_id: node_id.clone(),
            driver: raft::multi_raft_driver::MultiRaftDriver::new(),
            storage,
            network,
            router: Arc::new(ShardRouter::new(shard_count)),
            state_machines: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 获取节点 ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// 获取路由器引用
    pub fn router(&self) -> Arc<ShardRouter> {
        self.router.clone()
    }

    /// 创建或获取 Raft 组
    pub async fn get_or_create_raft_group(
        &self,
        shard_id: String,
        nodes: Vec<String>,
    ) -> Result<RaftId, String> {
        // 检查是否已存在
        let raft_id = RaftId::new(shard_id.clone(), self.node_id.clone());
        
        if self.state_machines.lock().contains_key(&shard_id) {
            debug!("Raft group already exists: {}", raft_id);
            return Ok(raft_id);
        }

        // 创建状态机（使用内存存储，后续可替换为 RocksDB）
        let store = Arc::new(MemoryStore::new());
        let state_machine = Arc::new(KVStateMachine::new(store));
        self.state_machines
            .lock()
            .insert(shard_id.clone(), state_machine.clone());

        // 创建集群配置
        let voters: HashSet<RaftId> = nodes
            .iter()
            .map(|node| RaftId::new(shard_id.clone(), node.clone()))
            .collect();
        let config = ClusterConfig::simple(voters, 0);

        // 创建 Raft 状态
        let mut options = RaftStateOptions::default();
        options.id = raft_id.clone();
        let callbacks: Arc<dyn RaftCallbacks> = Arc::new(NodeCallbacks {
            node_id: self.node_id.clone(),
            storage: self.storage.clone(),
            network: self.network.clone(),
            state_machine: state_machine.clone(),
        });

        let mut raft_state = RaftState::new(options, callbacks.clone()).await
            .map_err(|e| e.to_string())?;

        // 加载持久化状态
        if let Ok(Some(hard_state)) = self
            .storage
            .load_hard_state(&raft_id)
            .await
        {
            raft_state.current_term = hard_state.term;
            raft_state.voted_for = hard_state.voted_for;
        }

        // 加载集群配置
        if let Ok(loaded_config) = self.storage.load_cluster_config(&raft_id).await {
            raft_state.config = loaded_config;
        } else {
            raft_state.config = config;
            self.storage
                .save_cluster_config(&raft_id, raft_state.config.clone())
                .await
                .map_err(|e| format!("Failed to save cluster config: {}", e))?;
        }

        // 注册到 MultiRaftDriver
        let raft_state_arc = Arc::new(tokio::sync::Mutex::new(raft_state));
        let handle_event = Box::new(RaftGroupHandler {
            raft_state: raft_state_arc.clone(),
        });

        self.driver.add_raft_group(raft_id.clone(), handle_event);

        // 更新路由表
        self.router.add_shard(shard_id, nodes);

        info!("Created Raft group: {}", raft_id);
        Ok(raft_id)
    }

    /// 处理客户端命令
    pub async fn handle_command(
        &self,
        command: Vec<String>,
    ) -> Result<Vec<u8>, String> {
        if command.is_empty() {
            return Err("Empty command".to_string());
        }

        let cmd = command[0].to_uppercase();
        match cmd.as_str() {
            "GET" => {
                if command.len() != 2 {
                    return Err("GET requires 1 argument".to_string());
                }
                self.handle_get(&command[1]).await
            }
            "SET" => {
                if command.len() != 3 {
                    return Err("SET requires 2 arguments".to_string());
                }
                self.handle_set(&command[1], &command[2]).await
            }
            "DEL" => {
                if command.len() < 2 {
                    return Err("DEL requires at least 1 argument".to_string());
                }
                self.handle_del(&command[1..]).await
            }
            "EXISTS" => {
                if command.len() != 2 {
                    return Err("EXISTS requires 1 argument".to_string());
                }
                self.handle_exists(&command[1]).await
            }
            "KEYS" => {
                self.handle_keys().await
            }
            "PING" => {
                Ok(b"PONG".to_vec())
            }
            _ => Err(format!("Unknown command: {}", cmd)),
        }
    }

    async fn handle_get(&self, key: &str) -> Result<Vec<u8>, String> {
        let key_bytes = key.as_bytes();
        let shard_id = self.router.route_key(key_bytes);
        
        // 获取或创建 Raft 组
        let nodes = self.router.get_shard_nodes(&shard_id)
            .unwrap_or_else(|| vec![self.node_id.clone()]);
        
        let raft_id = self.get_or_create_raft_group(shard_id, nodes).await?;
        
        // 从状态机读取（简化实现，实际应该通过 Raft 的 ReadIndex）
        let state_machines = self.state_machines.lock();
        if let Some(sm) = state_machines.get(&raft_id.group) {
            Ok(sm.store().get(key_bytes).unwrap_or_default())
        } else {
            Err("State machine not found".to_string())
        }
    }

    async fn handle_set(&self, key: &str, value: &str) -> Result<Vec<u8>, String> {
        let key_bytes = key.as_bytes();
        let shard_id = self.router.route_key(key_bytes);
        
        // 获取或创建 Raft 组
        let nodes = self.router.get_shard_nodes(&shard_id)
            .unwrap_or_else(|| vec![self.node_id.clone()]);
        
        let raft_id = self.get_or_create_raft_group(shard_id, nodes).await?;
        
        // 创建命令
        let cmd = Command::Set {
            key: key_bytes.to_vec(),
            value: value.as_bytes().to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize command: {}", e))?;

        // 发送事件到 Raft 组
        let event = Event::ClientPropose {
            cmd: command,
            request_id: raft::RequestId::new(),
        };

        match self.driver.dispatch_event(raft_id.clone(), event) {
            raft::multi_raft_driver::SendEventResult::Success => {}
            _ => return Err("Failed to dispatch event".to_string()),
        }

        // 等待结果（简化实现，实际应该等待 ApplyResult）
        Ok(b"OK".to_vec())
    }

    async fn handle_del(&self, keys: &[String]) -> Result<Vec<u8>, String> {
        let mut count = 0;
        for key in keys {
            let key_bytes = key.as_bytes();
            let shard_id = self.router.route_key(key_bytes);
            
            let nodes = self.router.get_shard_nodes(&shard_id)
                .unwrap_or_else(|| vec![self.node_id.clone()]);
            
            let raft_id = self.get_or_create_raft_group(shard_id, nodes).await?;
            
            let cmd = Command::Del {
                keys: vec![key_bytes.to_vec()],
            };
            let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard())
                .map_err(|e| format!("Failed to serialize command: {}", e))?;

            let event = Event::ClientPropose {
                cmd: command,
                request_id: raft::RequestId::new(),
            };

            if matches!(self.driver.dispatch_event(raft_id, event), raft::multi_raft_driver::SendEventResult::Success) {
                count += 1;
            }
        }
        Ok(count.to_string().into_bytes())
    }

    async fn handle_exists(&self, key: &str) -> Result<Vec<u8>, String> {
        let key_bytes = key.as_bytes();
        let shard_id = self.router.route_key(key_bytes);
        
        let state_machines = self.state_machines.lock();
        if let Some(sm) = state_machines.get(&shard_id) {
            let keys: &[&[u8]] = &[key_bytes];
            Ok(if sm.store().exists(keys) > 0 { b"1".to_vec() } else { b"0".to_vec() })
        } else {
            Ok(b"0".to_vec())
        }
    }

    async fn handle_keys(&self) -> Result<Vec<u8>, String> {
        // 收集所有 Shard 的键数量
        let state_machines = self.state_machines.lock();
        let mut total_keys = 0;
        for sm in state_machines.values() {
            total_keys += sm.store().dbsize();
        }
        Ok(format!("{}", total_keys).into_bytes())
    }

    /// 启动节点
    pub async fn start(&self) -> Result<(), String> {
        info!("Starting RedRaft node: {}", self.node_id);
        
        // 启动 MultiRaftDriver
        let driver = self.driver.clone();
        tokio::spawn(async move {
            driver.main_loop().await;
        });

        info!("RedRaft node started: {}", self.node_id);
        Ok(())
    }

    /// 停止节点
    pub fn stop(&self) {
        info!("Stopping RedRaft node: {}", self.node_id);
        self.driver.stop();
    }
}

/// Raft 组事件处理器
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

/// 节点回调实现
struct NodeCallbacks {
    node_id: String,
    storage: Arc<dyn Storage>,
    network: Arc<dyn Network>,
    state_machine: Arc<KVStateMachine>,
}

#[async_trait]
impl raft::traits::EventNotify for NodeCallbacks {
    async fn on_state_changed(&self, _from: &RaftId, _role: raft::Role) -> Result<(), raft::error::StateChangeError> {
        Ok(())
    }

    async fn on_node_removed(&self, _node_id: &RaftId) -> Result<(), raft::error::StateChangeError> {
        Ok(())
    }
}

#[async_trait]
impl raft::traits::Network for NodeCallbacks {
    async fn send_request_vote_request(
        &self,
        _from: &RaftId,
        _target: &RaftId,
        _args: raft::RequestVoteRequest,
    ) -> raft::RpcResult<()> {
        Ok(())
    }

    async fn send_request_vote_response(
        &self,
        _from: &RaftId,
        _target: &RaftId,
        _args: raft::RequestVoteResponse,
    ) -> raft::RpcResult<()> {
        Ok(())
    }

    async fn send_append_entries_request(
        &self,
        _from: &RaftId,
        _target: &RaftId,
        _args: raft::AppendEntriesRequest,
    ) -> raft::RpcResult<()> {
        Ok(())
    }

    async fn send_append_entries_response(
        &self,
        _from: &RaftId,
        _target: &RaftId,
        _args: raft::AppendEntriesResponse,
    ) -> raft::RpcResult<()> {
        Ok(())
    }

    async fn send_install_snapshot_request(
        &self,
        _from: &RaftId,
        _target: &RaftId,
        _args: raft::InstallSnapshotRequest,
    ) -> raft::RpcResult<()> {
        Ok(())
    }

    async fn send_install_snapshot_response(
        &self,
        _from: &RaftId,
        _target: &RaftId,
        _args: raft::InstallSnapshotResponse,
    ) -> raft::RpcResult<()> {
        Ok(())
    }

    async fn send_pre_vote_request(
        &self,
        _from: &RaftId,
        _target: &RaftId,
        _args: PreVoteRequest,
    ) -> raft::RpcResult<()> {
        Ok(())
    }

    async fn send_pre_vote_response(
        &self,
        _from: &RaftId,
        _target: &RaftId,
        _args: PreVoteResponse,
    ) -> raft::RpcResult<()> {
        Ok(())
    }
}

#[async_trait]
impl raft::traits::EventSender for NodeCallbacks {
    async fn send(&self, _target: RaftId, _event: raft::Event) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Storage for NodeCallbacks {}
#[async_trait]
impl HardStateStorage for NodeCallbacks {
    async fn save_hard_state(&self, from: &RaftId, hard_state: raft::HardState) -> StorageResult<()> {
        self.storage.save_hard_state(from, hard_state).await
    }

    async fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<raft::HardState>> {
        self.storage.load_hard_state(from).await
    }
}

#[async_trait]
impl SnapshotStorage for NodeCallbacks {
    async fn save_snapshot(&self, from: &RaftId, snap: raft::Snapshot) -> StorageResult<()> {
        self.storage.save_snapshot(from, snap).await
    }

    async fn load_snapshot(&self, from: &RaftId) -> StorageResult<Option<raft::Snapshot>> {
        self.storage.load_snapshot(from).await
    }
}

#[async_trait]
impl ClusterConfigStorage for NodeCallbacks {
    async fn save_cluster_config(&self, from: &RaftId, conf: ClusterConfig) -> StorageResult<()> {
        self.storage.save_cluster_config(from, conf).await
    }

    async fn load_cluster_config(&self, from: &RaftId) -> StorageResult<ClusterConfig> {
        self.storage.load_cluster_config(from).await
    }
}

#[async_trait]
impl LogEntryStorage for NodeCallbacks {
    async fn append_log_entries(&self, from: &RaftId, entries: &[raft::LogEntry]) -> StorageResult<()> {
        self.storage.append_log_entries(from, entries).await
    }

    async fn get_log_entries(&self, from: &RaftId, low: u64, high: u64) -> StorageResult<Vec<raft::LogEntry>> {
        self.storage.get_log_entries(from, low, high).await
    }

    async fn get_log_entries_term(&self, from: &RaftId, low: u64, high: u64) -> StorageResult<Vec<(u64, u64)>> {
        self.storage.get_log_entries_term(from, low, high).await
    }

    async fn truncate_log_suffix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        self.storage.truncate_log_suffix(from, idx).await
    }

    async fn truncate_log_prefix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        self.storage.truncate_log_prefix(from, idx).await
    }

    async fn get_last_log_index(&self, from: &RaftId) -> StorageResult<(u64, u64)> {
        self.storage.get_last_log_index(from).await
    }

    async fn get_log_term(&self, from: &RaftId, idx: u64) -> StorageResult<u64> {
        self.storage.get_log_term(from, idx).await
    }
}

#[async_trait]
impl StateMachine for NodeCallbacks {
    async fn apply_command(
        &self,
        from: &RaftId,
        index: u64,
        term: u64,
        cmd: raft::Command,
    ) -> raft::ApplyResult<()> {
        self.state_machine.apply_command(from, index, term, cmd).await
    }

    fn process_snapshot(
        &self,
        from: &RaftId,
        index: u64,
        term: u64,
        data: Vec<u8>,
        config: ClusterConfig,
        request_id: raft::RequestId,
        oneshot: tokio::sync::oneshot::Sender<raft::SnapshotResult<()>>,
    ) {
        self.state_machine.process_snapshot(from, index, term, data, config, request_id, oneshot)
    }

    async fn create_snapshot(
        &self,
        from: &RaftId,
        cluster_config: ClusterConfig,
        saver: std::sync::Arc<dyn SnapshotStorage>,
    ) -> StorageResult<(u64, u64)> {
        self.state_machine.create_snapshot(from, cluster_config, saver).await
    }

    async fn client_response(
        &self,
        from: &RaftId,
        request_id: raft::RequestId,
        result: ClientResult<u64>,
    ) -> ClientResult<()> {
        self.state_machine.client_response(from, request_id, result).await
    }

    async fn read_index_response(
        &self,
        from: &RaftId,
        request_id: raft::RequestId,
        result: ClientResult<u64>,
    ) -> ClientResult<()> {
        self.state_machine.read_index_response(from, request_id, result).await
    }
}

impl TimerService for NodeCallbacks {
    fn del_timer(&self, from: &RaftId, timer_id: raft::TimerId) {
        // 通过 MultiRaftDriver 的定时器服务删除
    }

    fn set_leader_transfer_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        0 // 简化实现
    }

    fn set_election_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        0 // 简化实现
    }

    fn set_heartbeat_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        0 // 简化实现
    }

    fn set_apply_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        0 // 简化实现
    }

    fn set_config_change_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        0 // 简化实现
    }
}

impl RaftCallbacks for NodeCallbacks {}

