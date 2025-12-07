//! RedRaft 节点实现
//!
//! 集成 Multi-Raft、KV 状态机、路由和存储

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tracing::{debug, info};

use raft::{
    ClusterConfig, ClusterConfigStorage, Event, RequestId,
    HardStateStorage, LogEntryStorage, Network, RaftCallbacks, RaftId, RaftState,
    RaftStateOptions, SnapshotStorage, StateMachine, Storage, StorageResult,
    TimerService, message::{PreVoteRequest, PreVoteResponse}, traits::ClientResult,
};

use crate::router::ShardRouter;
use crate::state_machine::KVStateMachine;
use redisstore::{ApplyResult as StoreApplyResult, MemoryStore};
use resp::{Command, CommandType, RespValue};

/// 等待中的请求追踪器
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

    /// 注册一个等待中的请求
    pub fn register(&self, request_id: RequestId, cmd: Command) -> oneshot::Receiver<StoreApplyResult> {
        let (tx, rx) = oneshot::channel();
        self.requests.lock().insert(request_id.into(), (cmd, tx));
        rx
    }

    /// 完成请求并发送结果
    pub fn complete(&self, request_id: RequestId, result: StoreApplyResult) -> bool {
        if let Some((_, tx)) = self.requests.lock().remove(&request_id.into()) {
            let _ = tx.send(result);
            true
        } else {
            false
        }
    }

    /// 获取等待中的命令（用于 apply 时执行）
    pub fn get_command(&self, request_id: RequestId) -> Option<Command> {
        self.requests.lock().get(&request_id.into()).map(|(cmd, _)| cmd.clone())
    }

    /// 移除超时的请求
    pub fn remove(&self, request_id: RequestId) {
        self.requests.lock().remove(&request_id.into());
    }
}

impl Default for PendingRequests {
    fn default() -> Self {
        Self::new()
    }
}

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
    /// 等待中的请求追踪器
    pending_requests: PendingRequests,
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
            pending_requests: PendingRequests::new(),
        }
    }
    
    /// 获取等待请求追踪器
    pub fn pending_requests(&self) -> &PendingRequests {
        &self.pending_requests
    }

    /// 获取节点 ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// 获取路由器引用
    pub fn router(&self) -> Arc<ShardRouter> {
        self.router.clone()
    }

    /// 获取已存在的 Raft 组
    /// 
    /// 用于业务请求路由，不会创建新的 Raft 组。
    /// 如果 shard 不存在，返回 None。
    pub fn get_raft_group(&self, shard_id: &str) -> Option<RaftId> {
        if self.state_machines.lock().contains_key(shard_id) {
            Some(RaftId::new(shard_id.to_string(), self.node_id.clone()))
        } else {
            None
        }
    }

    /// 创建 Raft 组（仅供 Pilot 控制面调用）
    /// 
    /// # 重要
    /// 此函数只应由 Pilot 控制面调用，不应在业务请求处理中调用。
    /// 业务请求应使用 `get_raft_group` 获取已存在的组。
    /// 
    /// # 参数
    /// - `shard_id`: 分片 ID
    /// - `nodes`: 该分片的所有节点 ID 列表
    pub async fn create_raft_group(
        &self,
        shard_id: String,
        nodes: Vec<String>,
    ) -> Result<RaftId, String> {
        let raft_id = RaftId::new(shard_id.clone(), self.node_id.clone());
        
        // 检查是否已存在
        if self.state_machines.lock().contains_key(&shard_id) {
            debug!("Raft group already exists: {}", raft_id);
            return Ok(raft_id);
        }

        // 创建状态机（使用内存存储，后续可替换为 RocksDB）
        let store = Arc::new(MemoryStore::new());
        let state_machine = Arc::new(KVStateMachine::with_pending_requests(
            store,
            self.pending_requests.clone(),
        ));
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
        let timers = self.driver.get_timer_service();
        let callbacks: Arc<dyn RaftCallbacks> = Arc::new(NodeCallbacks {
            node_id: self.node_id.clone(),
            storage: self.storage.clone(),
            network: self.network.clone(),
            state_machine: state_machine.clone(),
            timers,
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

    /// 从路由表同步 Raft 组
    /// 
    /// 检查路由表中本节点负责的分片，自动创建缺失的 Raft 组。
    /// 此方法应在路由表更新后调用。
    /// 
    /// # 返回
    /// 返回新创建的 Raft 组数量
    pub async fn sync_raft_groups_from_routing(&self, routing_table: &crate::pilot_client::RoutingTable) -> usize {
        let mut created_count = 0;

        for (shard_id, nodes) in &routing_table.shard_nodes {
            // 检查本节点是否是该分片的副本
            if !nodes.contains(&self.node_id) {
                continue;
            }

            // 检查 Raft 组是否已存在
            if self.get_raft_group(shard_id).is_some() {
                continue;
            }

            // 创建 Raft 组
            info!(
                "Creating Raft group for shard {} (nodes: {:?})",
                shard_id, nodes
            );

            match self.create_raft_group(shard_id.clone(), nodes.clone()).await {
                Ok(raft_id) => {
                    info!("Successfully created Raft group: {}", raft_id);
                    created_count += 1;
                }
                Err(e) => {
                    info!("Failed to create Raft group for shard {}: {}", shard_id, e);
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

    /// 处理客户端命令
    pub async fn handle_command(&self, cmd: Command) -> Result<RespValue, String> {
        debug!("Handling command: {:?}", cmd.name());
        
        match cmd.command_type() {
            CommandType::Read => self.handle_read(cmd).await,
            CommandType::Write => self.handle_write(cmd).await,
        }
    }

    /// 处理读命令 - 直接从状态机读取
    async fn handle_read(&self, cmd: Command) -> Result<RespValue, String> {
        // 获取路由 key
        let key = cmd.get_key();
        
        // 确定 shard
        let shard_id = match key {
            Some(k) => self.router.route_key(k),
            None => {
                // 无 key 命令（如 PING, DBSIZE）在本地执行
                return self.handle_global_read(cmd);
            }
        };
        
        // 获取 Raft 组（必须已存在，由 Pilot 创建）
        let raft_id = self.get_raft_group(&shard_id)
            .ok_or_else(|| format!("CLUSTERDOWN Shard {} not ready", shard_id))?;
        
        // 从状态机读取
        let state_machines = self.state_machines.lock();
        if let Some(sm) = state_machines.get(&raft_id.group) {
            let result = sm.store().apply(&cmd);
            Ok(apply_result_to_resp(result))
        } else {
            Err("State machine not found".to_string())
        }
    }

    /// 处理全局读命令（无特定 key）
    fn handle_global_read(&self, cmd: Command) -> Result<RespValue, String> {
        match cmd {
            Command::Ping { message } => {
                Ok(match message {
                    Some(msg) => RespValue::BulkString(Some(msg)),
                    None => RespValue::SimpleString("PONG".to_string()),
                })
            }
            Command::Echo { message } => {
                Ok(RespValue::BulkString(Some(message)))
            }
            Command::DbSize => {
                let state_machines = self.state_machines.lock();
                let total: i64 = state_machines.values()
                    .map(|sm| sm.store().dbsize() as i64)
                    .sum();
                Ok(RespValue::Integer(total))
            }
            Command::CommandInfo => {
                // 简化实现
                Ok(RespValue::Array(vec![]))
            }
            Command::Info { .. } => {
                Ok(RespValue::BulkString(Some(b"# Server\nredraft_version:0.1.0\n".to_vec())))
            }
            Command::Keys { pattern } => {
                // 收集所有 shard 的 keys
                let state_machines = self.state_machines.lock();
                let mut all_keys = Vec::new();
                for sm in state_machines.values() {
                    let keys = sm.store().keys(&pattern);
                    for key in keys {
                        all_keys.push(RespValue::BulkString(Some(key)));
                    }
                }
                Ok(RespValue::Array(all_keys))
            }
            Command::Scan { cursor, pattern, count } => {
                // 简化实现：只在 cursor=0 时返回所有 keys
                if cursor != 0 {
                    return Ok(RespValue::Array(vec![
                        RespValue::BulkString(Some(b"0".to_vec())),
                        RespValue::Array(vec![]),
                    ]));
                }
                let state_machines = self.state_machines.lock();
                let mut all_keys = Vec::new();
                let limit = count.unwrap_or(10) as usize;
                let pattern_bytes = pattern.as_ref().map(|p| p.as_slice()).unwrap_or(b"*");
                for sm in state_machines.values() {
                    let keys = sm.store().keys(pattern_bytes);
                    for key in keys {
                        all_keys.push(RespValue::BulkString(Some(key)));
                        if all_keys.len() >= limit {
                            break;
                        }
                    }
                    if all_keys.len() >= limit {
                        break;
                    }
                }
                Ok(RespValue::Array(vec![
                    RespValue::BulkString(Some(b"0".to_vec())),
                    RespValue::Array(all_keys),
                ]))
            }
            _ => Err(format!("Unsupported global read command: {}", cmd.name())),
        }
    }

    /// 处理写命令 - 通过 Raft 共识
    async fn handle_write(&self, cmd: Command) -> Result<RespValue, String> {
        // 获取路由 key
        let key = match cmd.get_key() {
            Some(k) => k,
            None => {
                // FlushDb 等全局写命令需要特殊处理
                return self.handle_global_write(cmd).await;
            }
        };
        
        // 确定 shard
        let shard_id = self.router.route_key(key);
        
        // 获取 Raft 组（必须已存在，由 Pilot 创建）
        let raft_id = self.get_raft_group(&shard_id)
            .ok_or_else(|| format!("CLUSTERDOWN Shard {} not ready", shard_id))?;
        
        // 序列化命令
        let serialized = bincode::serde::encode_to_vec(&cmd.clone(), bincode::config::standard())
            .map_err(|e| format!("Failed to serialize command: {}", e))?;

        // 生成请求 ID 并注册等待
        let request_id = RequestId::new();
        let result_rx = self.pending_requests.register(request_id, cmd);

        // 发送事件到 Raft 组
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

        // 等待 Raft 提交并返回结果
        match tokio::time::timeout(Duration::from_secs(5), result_rx).await {
            Ok(Ok(result)) => Ok(apply_result_to_resp(result)),
            Ok(Err(_)) => {
                // Channel closed - 可能是节点关闭
                Err("Request cancelled".to_string())
            }
            Err(_) => {
                // 超时
                self.pending_requests.remove(request_id);
                Err("Request timeout".to_string())
            }
        }
    }

    /// 处理全局写命令
    async fn handle_global_write(&self, cmd: Command) -> Result<RespValue, String> {
        match cmd {
            Command::FlushDb => {
                // 清空所有 shard
                let state_machines = self.state_machines.lock();
                for sm in state_machines.values() {
                    sm.store().flushdb();
                }
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            _ => Err(format!("Unsupported global write command: {}", cmd.name())),
        }
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
    #[allow(dead_code)]
    node_id: String,
    storage: Arc<dyn Storage>,
    network: Arc<dyn Network>,
    state_machine: Arc<KVStateMachine>,
    timers: raft::multi_raft_driver::Timers,
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
        from: &RaftId,
        target: &RaftId,
        args: raft::RequestVoteRequest,
    ) -> raft::RpcResult<()> {
        self.network.send_request_vote_request(from, target, args).await
    }

    async fn send_request_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::RequestVoteResponse,
    ) -> raft::RpcResult<()> {
        self.network.send_request_vote_response(from, target, args).await
    }

    async fn send_append_entries_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::AppendEntriesRequest,
    ) -> raft::RpcResult<()> {
        self.network.send_append_entries_request(from, target, args).await
    }

    async fn send_append_entries_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::AppendEntriesResponse,
    ) -> raft::RpcResult<()> {
        self.network.send_append_entries_response(from, target, args).await
    }

    async fn send_install_snapshot_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::InstallSnapshotRequest,
    ) -> raft::RpcResult<()> {
        self.network.send_install_snapshot_request(from, target, args).await
    }

    async fn send_install_snapshot_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::InstallSnapshotResponse,
    ) -> raft::RpcResult<()> {
        self.network.send_install_snapshot_response(from, target, args).await
    }

    async fn send_pre_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteRequest,
    ) -> raft::RpcResult<()> {
        self.network.send_pre_vote_request(from, target, args).await
    }

    async fn send_pre_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteResponse,
    ) -> raft::RpcResult<()> {
        self.network.send_pre_vote_response(from, target, args).await
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
    fn del_timer(&self, _from: &RaftId, timer_id: raft::TimerId) {
        self.timers.del_timer(timer_id);
    }

    fn set_leader_transfer_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        self.timers.add_timer(from, raft::Event::LeaderTransferTimeout, dur)
    }

    fn set_election_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        self.timers.add_timer(from, raft::Event::ElectionTimeout, dur)
    }

    fn set_heartbeat_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        self.timers.add_timer(from, raft::Event::HeartbeatTimeout, dur)
    }

    fn set_apply_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        self.timers.add_timer(from, raft::Event::ApplyLogTimeout, dur)
    }

    fn set_config_change_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        self.timers.add_timer(from, raft::Event::ConfigChangeTimeout, dur)
    }
}

impl RaftCallbacks for NodeCallbacks {}

/// 将 StoreApplyResult 转换为 RespValue
fn apply_result_to_resp(result: StoreApplyResult) -> RespValue {
    match result {
        StoreApplyResult::Ok => RespValue::SimpleString("OK".to_string()),
        StoreApplyResult::Pong(msg) => {
            match msg {
                Some(m) => RespValue::BulkString(Some(m)),
                None => RespValue::SimpleString("PONG".to_string()),
            }
        }
        StoreApplyResult::Integer(n) => RespValue::Integer(n),
        StoreApplyResult::Value(v) => {
            match v {
                Some(data) => RespValue::BulkString(Some(data)),
                None => RespValue::Null,
            }
        }
        StoreApplyResult::Array(items) => {
            RespValue::Array(
                items.into_iter()
                    .map(|item| match item {
                        Some(data) => RespValue::BulkString(Some(data)),
                        None => RespValue::Null,
                    })
                    .collect()
            )
        }
        StoreApplyResult::KeyValues(kvs) => {
            let mut result = Vec::with_capacity(kvs.len() * 2);
            for (k, v) in kvs {
                result.push(RespValue::BulkString(Some(k)));
                result.push(RespValue::BulkString(Some(v)));
            }
            RespValue::Array(result)
        }
        StoreApplyResult::Type(t) => {
            match t {
                Some(type_name) => RespValue::SimpleString(type_name.to_string()),
                None => RespValue::SimpleString("none".to_string()),
            }
        }
        StoreApplyResult::Error(e) => {
            RespValue::Error(format!("ERR {}", e))
        }
    }
}

