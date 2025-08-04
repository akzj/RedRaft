use async_trait::async_trait;
use core::error;
use log::{debug, error, info, warn};
use lru_cache::LruCache;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::sync::Arc;
use std::time::{self, Duration, Instant};
use thiserror::Error;

pub mod mock_network;
pub mod mock_storage;
pub mod mutl_raft_driver;

// 类型定义
pub type NodeId = String;
pub type Command = Vec<u8>;

type TimerId = u64; // 定时器ID类型

// === 统一错误处理机制 ===
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorSeverity {
    Fatal,       // 需要终止当前操作并可能进入只读模式
    Recoverable, // 可以重试或降级处理的错误
    Ignorable,   // 仅需记录日志的错误
}

pub trait ErrorHandler {
    fn severity(&self) -> ErrorSeverity;
    fn context(&self) -> String;
}

// 为RPC错误实现ErrorHandler
impl ErrorHandler for RpcError {
    fn severity(&self) -> ErrorSeverity {
        match self {
            RpcError::NodeNotFound(_) => ErrorSeverity::Recoverable,
            RpcError::Network(_) => ErrorSeverity::Recoverable,
            RpcError::Timeout => ErrorSeverity::Recoverable,
            RpcError::Serialization(_) => ErrorSeverity::Fatal,
            RpcError::Protocol(_) => ErrorSeverity::Fatal,
        }
    }

    fn context(&self) -> String {
        match self {
            RpcError::NodeNotFound(node) => format!("Target node {} not found", node),
            RpcError::Network(msg) => format!("Network error: {}", msg),
            RpcError::Timeout => "RPC timeout".to_string(),
            RpcError::Serialization(e) => format!("Serialization error: {}", e),
            RpcError::Protocol(msg) => format!("Protocol error: {}", msg),
        }
    }
}

// 为存储错误实现ErrorHandler
impl ErrorHandler for StorageError {
    fn severity(&self) -> ErrorSeverity {
        match self {
            StorageError::Io(_) => ErrorSeverity::Fatal,
            StorageError::LogNotFound(_) => ErrorSeverity::Recoverable,
            StorageError::SnapshotNotFound(_) => ErrorSeverity::Recoverable,
            StorageError::DataCorruption(_) => ErrorSeverity::Fatal,
            StorageError::StorageFull => ErrorSeverity::Fatal,
            StorageError::ConfigNotFound => ErrorSeverity::Recoverable,
            StorageError::Consistency(_) => ErrorSeverity::Fatal,
        }
    }

    fn context(&self) -> String {
        match self {
            StorageError::Io(e) => format!("IO error: {}", e),
            StorageError::LogNotFound(idx) => format!("Log entry at index {} not found", idx),
            StorageError::SnapshotNotFound(idx) => format!("Snapshot at index {} not found", idx),
            StorageError::DataCorruption(idx) => format!("Data corruption at index {}", idx),
            StorageError::StorageFull => "Storage full".to_string(),
            StorageError::ConfigNotFound => "Configuration not found".to_string(),
            StorageError::Consistency(msg) => format!("Consistency check failed: {}", msg),
        }
    }
}

// 为客户端错误实现ErrorHandler
impl ErrorHandler for ClientError {
    fn severity(&self) -> ErrorSeverity {
        match self {
            ClientError::RequestNotFound(_) => ErrorSeverity::Ignorable,
            ClientError::SessionExpired => ErrorSeverity::Ignorable,
            ClientError::NotLeader(_) => ErrorSeverity::Ignorable,
            ClientError::Timeout => ErrorSeverity::Recoverable,
            ClientError::Conflict(_) => ErrorSeverity::Ignorable,
            ClientError::Internal(_) => ErrorSeverity::Recoverable,
            ClientError::BadRequest(_) => ErrorSeverity::Recoverable,
        }
    }

    fn context(&self) -> String {
        match self {
            ClientError::RequestNotFound(req_id) => format!("Request {} not found", req_id),
            ClientError::SessionExpired => "Client session expired".to_string(),
            ClientError::NotLeader(leader) => format!("Not leader, leader: {:?}", leader),
            ClientError::Timeout => "Client timeout".to_string(),
            ClientError::Conflict(msg) => format!("Request conflict: {}", msg),
            ClientError::Internal(e) => format!("Internal error: {}", e),
            ClientError::BadRequest(e) => format!("Bad request: {}", e),
        }
    }
}

// 为定时器错误实现ErrorHandler
impl ErrorHandler for TimerError {
    fn severity(&self) -> ErrorSeverity {
        match self {
            TimerError::NotFound(_) => ErrorSeverity::Recoverable,
            TimerError::ServiceUnavailable => ErrorSeverity::Recoverable,
            TimerError::AlreadyExists(_) => ErrorSeverity::Ignorable,
            TimerError::InvalidDuration => ErrorSeverity::Recoverable,
        }
    }

    fn context(&self) -> String {
        match self {
            TimerError::NotFound(id) => format!("Timer {} not found", id),
            TimerError::ServiceUnavailable => "Timer service unavailable".to_string(),
            TimerError::AlreadyExists(id) => format!("Timer {} already exists", id),
            TimerError::InvalidDuration => "Invalid duration".to_string(),
        }
    }
}

// 为快照错误实现ErrorHandler
impl ErrorHandler for SnapshotError {
    fn severity(&self) -> ErrorSeverity {
        match self {
            SnapshotError::AlreadyExists(_) => ErrorSeverity::Ignorable,
            SnapshotError::TooOld(_) => ErrorSeverity::Ignorable,
            SnapshotError::DataCorrupted => ErrorSeverity::Fatal,
            SnapshotError::InProgress => ErrorSeverity::Recoverable,
            SnapshotError::SizeExceeded => ErrorSeverity::Recoverable,
            SnapshotError::NotSupported => ErrorSeverity::Fatal,
        }
    }

    fn context(&self) -> String {
        match self {
            SnapshotError::AlreadyExists(index) => {
                format!("Snapshot at index {} already exists", index)
            }
            SnapshotError::TooOld(index) => format!("Snapshot at index {} is too old", index),
            SnapshotError::DataCorrupted => "Snapshot data corrupted".to_string(),
            SnapshotError::InProgress => "Snapshot creation in progress".to_string(),
            SnapshotError::SizeExceeded => "Snapshot size exceeds limit".to_string(),
            SnapshotError::NotSupported => "Snapshot not supported".to_string(),
        }
    }
}

// 为状态变更错误实现ErrorHandler
impl ErrorHandler for StateChangeError {
    fn severity(&self) -> ErrorSeverity {
        match self {
            StateChangeError::InvalidTransition(_, _) => ErrorSeverity::Recoverable,
            StateChangeError::LeadershipTransferFailed => ErrorSeverity::Recoverable,
            StateChangeError::ConfigChangeInProgress => ErrorSeverity::Recoverable,
        }
    }

    fn context(&self) -> String {
        match self {
            StateChangeError::InvalidTransition(from, to) => {
                format!("Invalid state transition from {:?} to {:?}", from, to)
            }
            StateChangeError::LeadershipTransferFailed => "Leadership transfer failed".to_string(),
            StateChangeError::ConfigChangeInProgress => {
                "Configuration change in progress".to_string()
            }
        }
    }
}

pub struct CallbackErrorHandler {
    node_id: NodeId,
    readonly_mode: bool,
}

impl CallbackErrorHandler {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            readonly_mode: false,
        }
    }

    /// 统一处理回调错误
    pub async fn handle<T, E: ErrorHandler>(
        &mut self,
        result: Result<T, E>,
        operation: &str,
        target: Option<&NodeId>,
    ) -> Option<T> {
        match result {
            Ok(val) => Some(val),
            Err(e) => {
                let ctx = if let Some(t) = target {
                    format!("{} from {} to {}", operation, self.node_id, t)
                } else {
                    format!("{} on {}", operation, self.node_id)
                };

                match e.severity() {
                    ErrorSeverity::Fatal => {
                        error!(
                            "[FATAL] {} failed: {} - Entering readonly mode",
                            ctx,
                            e.context()
                        );
                        self.enter_readonly_mode().await;
                        None
                    }
                    ErrorSeverity::Recoverable => {
                        warn!("[RECOVERABLE] {} failed: {} - Will retry", ctx, e.context());
                        None
                    }
                    ErrorSeverity::Ignorable => {
                        debug!("[IGNORABLE] {} failed: {}", ctx, e.context());
                        None
                    }
                }
            }
        }
    }

    /// 处理不需要返回值的操作
    pub async fn handle_void<E: ErrorHandler>(
        &mut self,
        result: Result<(), E>,
        operation: &str,
        target: Option<&NodeId>,
    ) -> bool {
        self.handle(result, operation, target).await.is_some()
    }

    async fn enter_readonly_mode(&mut self) {
        if !self.readonly_mode {
            error!(
                "Node {} entering readonly mode due to critical failure",
                self.node_id
            );
            self.readonly_mode = true;
            // 这里可以添加进入只读模式的具体逻辑
        }
    }

    pub fn is_readonly(&self) -> bool {
        self.readonly_mode
    }
}
// 请求ID类型（用于过滤超时响应）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(u64);

impl RequestId {
    pub fn new() -> Self {
        Self(rand::random::<u64>())
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// === 事件定义（输入）===
#[derive(Debug)]
pub enum Event {
    // 定时器事件
    ElectionTimeout,     // 选举超时（Follower/Candidate 触发）
    HeartbeatTimeout,    // 心跳超时（Leader 触发日志同步）
    ApplyLogs,           // 定期将已提交日志应用到状态机
    ConfigChangeTimeout, // 配置变更超时

    // RPC 请求事件（来自其他节点）
    RequestVote(RequestVoteRequest),
    AppendEntries(AppendEntriesRequest),
    InstallSnapshot(InstallSnapshotRequest),

    // RPC 响应事件（其他节点对本节点请求的回复）
    RequestVoteReply(NodeId, RequestVoteResponse),
    AppendEntriesReply(NodeId, AppendEntriesResponse),
    InstallSnapshotReply(NodeId, InstallSnapshotResponse),

    // 领导人转移相关事件
    LeaderTransfer {
        target: NodeId,
        request_id: RequestId,
    },
    LeaderTransferTimeout,

    // 客户端事件
    ClientPropose {
        cmd: Command,
        request_id: RequestId, // 客户端请求ID，用于关联响应
    },
    // 配置变更事件
    ChangeConfig {
        new_voters: HashSet<NodeId>,
        request_id: RequestId,
    },

    // 快照生成
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Follower => write!(f, "Follower"),
            Role::Candidate => write!(f, "Candidate"),
            Role::Leader => write!(f, "Leader"),
        }
    }
}

/// 顶层Raft错误类型
#[derive(Debug, Error)]
pub enum RaftError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Timer error: {0}")]
    Timer(#[from] TimerError),

    #[error("Client error: {0}")]
    Client(#[from] ClientError),

    #[error("State change error: {0}")]
    StateChange(#[from] StateChangeError),

    #[error("Apply error: {0}")]
    Apply(#[from] ApplyError),

    #[error("Snapshot error: {0}")]
    Snapshot(#[from] SnapshotError),
}

/// RPC通信相关错误
#[derive(Debug, Error)]
pub enum RpcError {
    #[error("Target node {0} not found")]
    NodeNotFound(NodeId),

    #[error("Network error: {0}")]
    Network(String),

    #[error("RPC timeout")]
    Timeout,

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// 存储相关错误
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Log entry at index {0} not found")]
    LogNotFound(u64),

    #[error("Snapshot at index {0} not found")]
    SnapshotNotFound(u64),

    #[error("Corrupted data at index {0}")]
    DataCorruption(u64),

    #[error("Storage full")]
    StorageFull,

    #[error("Configuration not found")]
    ConfigNotFound,

    #[error("Consistency check failed: {0}")]
    Consistency(String),
}

/// 定时器相关错误
#[derive(Debug, Error)]
pub enum TimerError {
    #[error("Timer {0} not found")]
    NotFound(TimerId),

    #[error("Timer service unavailable")]
    ServiceUnavailable,

    #[error("Timer already exists: {0}")]
    AlreadyExists(TimerId),

    #[error("Invalid duration")]
    InvalidDuration,
}

/// 客户端相关错误
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Request {0} not found")]
    RequestNotFound(RequestId),

    #[error("Client session expired")]
    SessionExpired,

    #[error("Not leader (current leader: {0:?})")]
    NotLeader(Option<NodeId>),

    #[error("Request timeout")]
    Timeout,

    #[error("Request conflicted: {0}")]
    Conflict(anyhow::Error),

    #[error("Internal error {0}")]
    Internal(anyhow::Error),

    #[error("")]
    BadRequest(anyhow::Error),
}

/// 状态变更相关错误
#[derive(Debug, Error)]
pub enum StateChangeError {
    #[error("Invalid state transition from {0} to {1}")]
    InvalidTransition(Role, Role),

    #[error("Leadership transfer failed")]
    LeadershipTransferFailed,

    #[error("Configuration change in progress")]
    ConfigChangeInProgress,
}

/// 状态机应用相关错误
#[derive(Debug, Error)]
pub enum ApplyError {
    #[error("Command at index {0} already applied")]
    AlreadyApplied(u64),

    #[error("Command at index {0} term mismatch (expected {1}, got {2})")]
    TermMismatch(u64, u64, u64),

    #[error("State machine busy")]
    Busy,

    #[error("State machine error: {0}")]
    Internal(String),
}

/// 快照相关错误
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("Snapshot at index {0} already exists")]
    AlreadyExists(u64),

    #[error("Snapshot at index {0} is too old")]
    TooOld(u64),

    #[error("Snapshot data corrupted")]
    DataCorrupted,

    #[error("Snapshot creation in progress")]
    InProgress,

    #[error("Snapshot size exceeds limit")]
    SizeExceeded,

    #[error("Snapshot not supported")]
    NotSupported,
}

/// 配置变更相关错误
#[derive(Debug, Error, Serialize, Deserialize)]
pub enum ConfigError {
    #[error("Configuration is empty")]
    EmptyConfig,

    #[error("Already in joint configuration")]
    AlreadyInJoint,

    #[error("Not in joint configuration")]
    NotInJoint,

    #[error("New configuration not ready")]
    NewConfigNotReady,

    #[error("Invalid configuration transition")]
    InvalidTransition,
}

/// Quorum要求
#[derive(Debug, Clone, PartialEq)]
pub enum QuorumRequirement {
    Simple(usize),
    Joint { old: usize, new: usize },
}

// 结果类型别名
pub type RaftResult<T> = Result<T, RaftError>;
pub type RpcResult<T> = Result<T, RpcError>;
pub type StorageResult<T> = Result<T, StorageError>;
pub type TimerResult<T> = Result<T, TimerError>;
pub type ClientResult<T> = Result<T, ClientError>;
pub type ApplyResult<T> = Result<T, ApplyError>;
pub type SnapshotResult<T> = Result<T, SnapshotError>;

#[async_trait]
pub trait RaftCallbacks: Send + Sync {
    // 发送 RPC 回调
    async fn send_request_vote_request(
        &self,
        from: NodeId,
        target: NodeId,
        args: RequestVoteRequest,
    ) -> RpcResult<()>;

    async fn send_request_vote_response(
        &self,
        from: NodeId,
        target: NodeId,
        args: RequestVoteResponse,
    ) -> RpcResult<()>;

    async fn send_append_entries_request(
        &self,
        from: NodeId,
        target: NodeId,
        args: AppendEntriesRequest,
    ) -> RpcResult<()>;

    async fn send_append_entries_response(
        &self,
        from: NodeId,
        target: NodeId,
        args: AppendEntriesResponse,
    ) -> RpcResult<()>;

    async fn send_install_snapshot_request(
        &self,
        from: NodeId,
        target: NodeId,
        args: InstallSnapshotRequest,
    ) -> RpcResult<()>;

    async fn send_install_snapshot_reply(
        &self,
        from: NodeId,
        target: NodeId,
        args: InstallSnapshotResponse,
    ) -> RpcResult<()>;

    // 持久化回调
    async fn save_hard_state(
        &self,
        from: NodeId,
        term: u64,
        voted_for: Option<NodeId>,
    ) -> StorageResult<()>;

    async fn load_hard_state(&self, from: NodeId) -> StorageResult<Option<(u64, Option<NodeId>)>>;

    async fn append_log_entries(&self, from: NodeId, entries: &[LogEntry]) -> StorageResult<()>;

    async fn get_log_entries(
        &self,
        from: NodeId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>>;

    async fn truncate_log_suffix(&self, from: NodeId, idx: u64) -> StorageResult<()>;

    async fn truncate_log_prefix(&self, from: NodeId, idx: u64) -> StorageResult<()>;

    async fn get_last_log_index(&self, from: NodeId) -> StorageResult<u64>;

    async fn get_log_term(&self, from: NodeId, idx: u64) -> StorageResult<u64>;

    async fn save_snapshot(&self, from: NodeId, snap: Snapshot) -> StorageResult<()>;

    async fn load_snapshot(&self, from: NodeId) -> StorageResult<Option<Snapshot>>;

    async fn create_snapshot(&self, from: NodeId) -> StorageResult<(u64, u64)>;

    async fn save_cluster_config(&self, from: NodeId, conf: ClusterConfig) -> StorageResult<()>;

    async fn load_cluster_config(&self, from: NodeId) -> StorageResult<ClusterConfig>;

    // 定时器回调
    fn del_timer(&self, from: NodeId, timer_id: TimerId) -> ();
    fn set_leader_transfer_timer(&self, from: NodeId, dur: Duration) -> TimerId;
    fn set_election_timer(&self, from: NodeId, dur: Duration) -> TimerId;
    fn set_heartbeat_timer(&self, from: NodeId, dur: Duration) -> TimerId;
    fn set_apply_timer(&self, from: NodeId, dur: Duration) -> TimerId;
    fn set_config_change_timer(&self, from: NodeId, dur: Duration) -> TimerId;

    // 客户端响应回调
    async fn client_response(
        &self,
        from: NodeId,
        request_id: RequestId,
        result: ClientResult<u64>,
    ) -> ClientResult<()>;

    // 状态变更通知回调
    async fn state_changed(&self, from: NodeId, role: Role) -> Result<(), StateChangeError>;

    // 日志应用到状态机的回调
    async fn apply_command(
        &self,
        from: NodeId,
        index: u64,
        term: u64,
        cmd: Command,
    ) -> ApplyResult<()>;

    // 处理快照数据
    async fn process_snapshot(
        &self,
        from: NodeId,
        index: u64,
        term: u64,
        data: Vec<u8>,
        request_id: RequestId,
    ) -> SnapshotResult<()>;
}

// 为错误类型实现便捷构造函数
impl RpcError {
    pub fn network_err<S: Into<String>>(msg: S) -> Self {
        RpcError::Network(msg.into())
    }
}

impl StorageError {
    pub fn consistency_err<S: Into<String>>(msg: S) -> Self {
        StorageError::Consistency(msg.into())
    }
}

impl ApplyError {
    pub fn internal_err<S: Into<String>>(msg: S) -> Self {
        ApplyError::Internal(msg.into())
    }
}

// === 集群配置 ===
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub epoch: u64, // 配置版本号
    pub voters: HashSet<NodeId>,
    pub joint: Option<JointConfig>,
    // 新增：联合配置进入时间（用于超时判断）
    #[serde(skip)]
    pub joint_enter_time: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JointConfig {
    pub old: HashSet<NodeId>,
    pub new: HashSet<NodeId>,
}

impl ClusterConfig {
    pub fn empty() -> Self {
        Self {
            joint: None,
            epoch: 0,
            voters: HashSet::new(),
            joint_enter_time: None,
        }
    }

    pub fn simple(voters: HashSet<NodeId>) -> Self {
        Self {
            voters,
            epoch: 0,
            joint: None,
            joint_enter_time: None,
        }
    }

    pub fn enter_joint(
        &mut self,
        old: HashSet<NodeId>,
        new: HashSet<NodeId>,
    ) -> Result<(), ConfigError> {
        // 验证配置
        if old.is_empty() || new.is_empty() {
            return Err(ConfigError::EmptyConfig);
        }

        if self.joint.is_some() {
            return Err(ConfigError::AlreadyInJoint);
        }

        self.epoch += 1;
        self.joint = Some(JointConfig {
            old: old.clone(),
            new: new.clone(),
        });
        self.voters = old.union(&new).cloned().collect();
        // 记录进入联合配置的时间
        self.joint_enter_time = Some(Instant::now());
        Ok(())
    }

    pub fn leave_joint(&mut self) -> Result<Self, ConfigError> {
        match self.joint.take() {
            Some(j) => {
                self.voters = j.new.clone();
                Ok(Self {
                    epoch: self.epoch,
                    voters: j.new,
                    joint: None,
                    joint_enter_time: None,
                })
            }
            None => Err(ConfigError::NotInJoint),
        }
    }

    pub fn quorum(&self) -> QuorumRequirement {
        match &self.joint {
            Some(j) => QuorumRequirement::Joint {
                old: j.old.len() / 2 + 1,
                new: j.new.len() / 2 + 1,
            },
            None => QuorumRequirement::Simple(self.voters.len() / 2 + 1),
        }
    }

    pub fn joint_quorum(&self) -> Option<(usize, usize)> {
        self.joint
            .as_ref()
            .map(|j| (j.old.len() / 2 + 1, j.new.len() / 2 + 1))
    }

    pub fn contains(&self, id: &NodeId) -> bool {
        self.voters.contains(id)
    }

    pub fn majority(&self, votes: &HashSet<NodeId>) -> bool {
        if let Some(j) = &self.joint {
            votes.intersection(&j.old).count() >= j.old.len() / 2 + 1
                && votes.intersection(&j.new).count() >= j.new.len() / 2 + 1
        } else {
            votes.len() >= self.voters.len() / 2 + 1
        }
    }

    pub fn get_effective_voters(&self) -> &HashSet<NodeId> {
        &self.voters
    }

    // 验证配置是否合法
    pub fn is_valid(&self) -> bool {
        // 确保配置不会导致无法形成多数派
        if self.voters.is_empty() {
            return false;
        }

        // 对于联合配置，确保新旧配置都能形成多数派
        if let Some(joint) = &self.joint {
            if joint.old.is_empty() || joint.new.is_empty() {
                return false;
            }
        }

        true
    }
}

// === 网络接口 ===
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
    pub request_id: RequestId,
    // 空消息标记 - 用于探测安装状态
    pub is_probe: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InstallSnapshotState {
    Failed(String), // 失败，附带原因
    Installing,     // 正在安装
    Success,        // 成功完成
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: u64,
    pub request_id: RequestId,
    pub state: InstallSnapshotState,
}

// #[derive(Debug, Clone)]
// pub struct Error(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
    pub config: ClusterConfig,
}

// 快照探测计划结构
#[derive(Debug, Clone)]
struct SnapshotProbeSchedule {
    peer: NodeId,
    next_probe_time: Instant,
    interval: Duration, // 探测间隔
    max_attempts: u32,  // 最大尝试次数
    attempts: u32,      // 当前尝试次数
}

// === 核心状态与逻辑 ===

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
    pub is_config: bool,                      // 标记是否为配置变更日志
    pub client_request_id: Option<RequestId>, // 关联的客户端请求ID，用于去重
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub conflict_index: Option<u64>,
    pub conflict_term: Option<u64>, // 用于更高效的日志冲突处理
    pub request_id: RequestId,
}

// === 状态机（可变状态，无 Clone）===
pub struct RaftState {
    // 节点标识与配置
    id: NodeId,
    leader_id: Option<NodeId>,
    peers: Vec<NodeId>,
    config: ClusterConfig,

    // 核心状态
    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,

    // 日志与提交状态
    commit_index: u64,
    last_applied: u64,
    last_snapshot_index: u64, // 最后一个快照的索引
    last_snapshot_term: u64,  // 最后一个快照的任期

    // Leader 专用状态
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
    client_requests: HashMap<RequestId, u64>, // 客户端请求ID -> 日志索引
    recent_client_requests: lru_cache::LruCache<RequestId, u64>, // 最近的客户端请求ID，用于去重,会自动清理

    // 配置变更相关状态
    config_change_in_progress: bool,
    config_change_start_time: Option<Instant>,
    config_change_timeout: Duration,
    // 新配置确认计数器：记录已接受新配置的节点
    new_config_ack_nodes: HashSet<NodeId>,
    // 联合配置超时阈值（可配置，默认30秒）
    joint_timeout: Duration,

    // 定时器配置
    election_timeout: Duration,
    election_timeout_min: u64,
    election_timeout_max: u64,
    heartbeat_interval: Duration,
    heartbeat_interval_timer_id: Option<TimerId>,
    apply_interval: Duration, // 日志应用到状态机的间隔
    apply_interval_timer: Option<TimerId>,
    config_change_timer: Option<TimerId>,

    last_heartbeat: Instant,

    // 外部依赖
    callbacks: Arc<dyn RaftCallbacks>,

    // 统一错误处理器
    error_handler: CallbackErrorHandler,

    // 选举跟踪（仅 Candidate 状态有效）
    election_votes: HashMap<NodeId, bool>,
    election_max_term: u64,
    current_election_id: Option<RequestId>,

    // 快照请求跟踪（仅 Follower 有效）
    current_snapshot_request_id: Option<RequestId>,

    // 快照相关状态（Leader 用）
    follower_snapshot_states: HashMap<NodeId, InstallSnapshotState>,
    follower_last_snapshot_index: HashMap<NodeId, u64>,
    snapshot_probe_schedules: Vec<SnapshotProbeSchedule>,

    // 领导人转移相关状态
    leader_transfer_target: Option<NodeId>,
    leader_transfer_request_id: Option<RequestId>,
    leader_transfer_timeout: Duration,
    leader_transfer_start_time: Option<Instant>,

    election_timer: Option<TimerId>,        // 选举定时器ID
    leader_transfer_timer: Option<TimerId>, // 领导人转移定时器ID

    // 其他状态（可扩展）
    options: RaftStateOptions,
}

#[derive(Debug, Clone)]
pub struct RaftStateOptions {
    id: NodeId,
    peers: Vec<NodeId>,
    election_timeout_min: u64,
    election_timeout_max: u64,
    heartbeat_interval: u64,
    apply_interval: u64,
    config_change_timeout: Duration,
    leader_transfer_timeout: Duration,
    apply_batch_size: u64, // 每次应用到状态机的日志条数
}

impl RaftState {
    /// 初始化状态
    pub async fn new(options: RaftStateOptions, callbacks: Arc<dyn RaftCallbacks>) -> Self {
        // 从回调加载持久化状态
        let (current_term, voted_for) = match callbacks.load_hard_state(options.id.clone()).await {
            Ok(Some((term, voted_for))) => (term, voted_for),
            Ok(None) => (0, None), // 如果没有持久化状态，则初始化为0
            Err(err) => {
                // 加载失败时也初始化为0
                error!("Failed to load hard state: {}", err);
                (0, None)
            }
        };

        let loaded_config = match callbacks.load_cluster_config(options.id.clone()).await {
            Ok(conf) => conf,
            Err(err) => {
                error!("Failed to load cluster config: {}", err);
                // 如果加载失败，使用默认空配置
                ClusterConfig::empty()
            }
        };

        let snap = match callbacks.load_snapshot(options.id.clone()).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                error!("No snapshot found");
                Snapshot {
                    index: 0,
                    term: 0,
                    data: vec![],
                    config: loaded_config.clone(),
                }
            }
            Err(err) => {
                error!("Failed to load snapshot: {}", err);
                // 如果加载失败，使用默认快照
                Snapshot {
                    index: 0,
                    term: 0,
                    data: vec![],
                    config: loaded_config.clone(),
                }
            }
        };
        let timeout = options.election_timeout_min
            + rand::random::<u64>()
                % (options.election_timeout_max - options.election_timeout_min + 1);

        RaftState {
            error_handler: CallbackErrorHandler::new(options.id.clone()),
            options: options.clone(),
            leader_id: None,
            leader_transfer_request_id: None,
            leader_transfer_start_time: None,
            leader_transfer_target: None,
            leader_transfer_timeout: options.leader_transfer_timeout,
            id: options.id,
            peers: options.peers,
            config: loaded_config,
            role: Role::Follower,
            current_term,
            voted_for,
            commit_index: snap.index, // 提交索引从快照开始
            last_applied: snap.index,
            last_snapshot_index: snap.index,
            last_snapshot_term: snap.term,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            client_requests: HashMap::new(),
            recent_client_requests: LruCache::new(1000),
            config_change_in_progress: false,
            config_change_start_time: None,
            heartbeat_interval_timer_id: None,
            config_change_timeout: options.config_change_timeout,
            new_config_ack_nodes: HashSet::new(),
            joint_timeout: Duration::from_secs(30),
            election_timeout: Duration::from_millis(timeout),
            election_timeout_min: options.election_timeout_min,
            election_timeout_max: options.election_timeout_max,
            heartbeat_interval: Duration::from_millis(options.heartbeat_interval),
            apply_interval: Duration::from_millis(options.apply_interval),
            apply_interval_timer: None,
            config_change_timer: None,
            last_heartbeat: Instant::now(),
            callbacks: callbacks,
            election_votes: HashMap::new(),
            election_max_term: current_term,
            current_election_id: None,
            current_snapshot_request_id: None,
            follower_snapshot_states: HashMap::new(),
            follower_last_snapshot_index: HashMap::new(),
            snapshot_probe_schedules: Vec::new(),
            election_timer: None,
            leader_transfer_timer: None,
        }
    }

    /// 处理事件（主入口）
    pub async fn handle_event(&mut self, event: Event) {
        match event {
            Event::ElectionTimeout => self.handle_election_timeout().await,
            Event::RequestVote(args) => self.handle_request_vote(args).await,
            Event::AppendEntries(args) => self.handle_append_entries(args).await,
            Event::RequestVoteReply(peer, reply) => {
                self.handle_request_vote_response(peer, reply).await
            }
            Event::AppendEntriesReply(peer, reply) => {
                self.handle_append_entries_response(peer, reply).await
            }
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout().await,
            Event::ClientPropose { cmd, request_id } => {
                self.handle_client_propose(cmd, request_id).await
            }
            Event::InstallSnapshot(args) => self.handle_install_snapshot(args).await,
            Event::InstallSnapshotReply(peer, reply) => {
                self.handle_install_snapshot_response(peer, reply).await
            }
            Event::ApplyLogs => self.apply_committed_logs().await,
            Event::ConfigChangeTimeout => self.handle_config_change_timeout().await,
            Event::ChangeConfig {
                new_voters,
                request_id,
            } => self.handle_change_config(new_voters, request_id).await,
            Event::LeaderTransfer { target, request_id } => {
                self.handle_leader_transfer(target, request_id).await;
            }
            Event::LeaderTransferTimeout => {
                self.handle_leader_transfer_timeout().await;
            }
            Event::Snapshot => self.create_snapshot().await,
        }
    }

    // === 选举相关逻辑 ===
    async fn handle_election_timeout(&mut self) {
        if self.role == Role::Leader {
            return; // Leader 不处理选举超时
        }

        // 切换为 Candidate 并递增任期
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());
        match self
            .callbacks
            .save_hard_state(self.id.clone(), self.current_term, self.voted_for.clone())
            .await
        {
            Ok(_) => debug!("save hard state success"),
            Err(err) => {
                // fall back to follow
                self.current_term -= 1;
                self.role = Role::Follower;
                self.voted_for = None;
                log::error!("Failed to save hard state: {}", err);
                return;
            }
        }

        // 重置选举定时器
        self.reset_election().await;

        // 生成新选举ID并初始化跟踪状态
        let election_id = RequestId::new();
        self.current_election_id = Some(election_id);
        self.election_votes.clear();
        self.election_votes.insert(self.id.clone(), true);
        self.election_max_term = self.current_term;

        // 发送投票请求
        let last_log_index = self.get_last_log_index().await;
        let last_log_term = self.get_last_log_term().await;
        let req = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
            request_id: election_id,
        };

        for peer in self.config.get_effective_voters() {
            if *peer != self.id {
                let target = peer.clone();
                let args = req.clone();

                let _ = self
                    .error_handler
                    .handle(
                        self.callbacks
                            .send_request_vote_request(self.id.clone(), target.clone(), args)
                            .await,
                        "send_request_vote_request",
                        Some(&target),
                    )
                    .await;
            }
        }

        let _ = self
            .error_handler
            .handle_void(
                self.callbacks
                    .state_changed(self.id.clone(), Role::Candidate)
                    .await,
                "state_changed",
                None,
            )
            .await;
    }

    async fn handle_request_vote(&mut self, args: RequestVoteRequest) {
        let mut vote_granted = false;

        // 处理更高任期
        if args.term > self.current_term {
            self.current_term = args.term;
            self.role = Role::Follower;
            self.voted_for = None;
            if let Err(err) = self
                .callbacks
                .save_hard_state(self.id.clone(), self.current_term, self.voted_for.clone())
                .await
            {
                log::error!("Failed to save hard state: {}", err);
            }
            if let Err(err) = self
                .callbacks
                .state_changed(self.id.clone(), Role::Follower)
                .await
            {
                log::error!(
                    "node {}: failed to change state to Follower: {}",
                    self.id,
                    err
                );
            }
        }

        // 日志最新性检查 - 优化版本
        let log_ok = self
            .is_log_up_to_date(args.last_log_index, args.last_log_term)
            .await;

        // 投票条件：同任期、未投票或投给同一人、日志最新
        if args.term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(args.candidate_id.clone()))
            && log_ok
        {
            self.voted_for = Some(args.candidate_id.clone());
            vote_granted = true;
            if let Err(err) = self
                .callbacks
                .save_hard_state(self.id.clone(), self.current_term, self.voted_for.clone())
                .await
            {
                log::error!("node {} Failed to save hard state: {}", self.id, err);
            }
        }

        // 发送响应
        let resp = RequestVoteResponse {
            term: self.current_term,
            vote_granted,
            request_id: args.request_id,
        };
        if let Err(err) = self
            .callbacks
            .send_request_vote_response(self.id.clone(), args.candidate_id.clone(), resp)
            .await
        {
            log::error!(
                "node {}: failed to send RequestVoteResponse to {}: {}",
                self.id,
                args.candidate_id,
                err
            );
        }
    }

    // 优化的日志最新性检查
    async fn is_log_up_to_date(&self, candidate_last_index: u64, candidate_last_term: u64) -> bool {
        let self_last_term = self.get_last_log_term().await;
        let self_last_index = self.get_last_log_index().await;

        candidate_last_term > self_last_term
            || (candidate_last_term == self_last_term && candidate_last_index >= self_last_index)
    }

    async fn handle_request_vote_response(&mut self, peer: NodeId, response: RequestVoteResponse) {
        // 过滤非候选人状态或过期请求
        debug!("node {}: received vote response: {:?}", self.id, response);
        if self.role != Role::Candidate || self.current_election_id != Some(response.request_id) {
            return;
        }

        // 过滤无效投票者
        if !self.config.contains(&peer) {
            warn!(
                "node {}: received vote response from unknown peer {}",
                self.id, peer
            );
            return;
        }

        // 处理更高任期
        if response.term > self.current_term {
            info!(
                "Stepping down from candidate due to higher term {} from peer {} (current term {})",
                response.term, peer, self.current_term
            );
            self.current_term = response.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.election_votes.clear();
            self.reset_election().await;

            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .save_hard_state(self.id.clone(), self.current_term, self.voted_for.clone())
                        .await,
                    "save_hard_state",
                    None,
                )
                .await;

            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .state_changed(self.id.clone(), Role::Follower)
                        .await,
                    "state_changed",
                    None,
                )
                .await;
            return;
        }

        // 记录投票结果
        if response.term == self.current_term {
            self.election_votes.insert(peer, response.vote_granted);
        }

        // 检查是否赢得选举
        self.check_election_result().await;
    }

    async fn check_election_result(&mut self) {
        debug!(
            "Node {}: check_election_result, votes: {:?}, current_term: {}, role: {:?}, election_id: {:?}",
            self.id, self.election_votes, self.current_term, self.role, self.current_election_id
        );

        let granted_votes: HashSet<NodeId> = self
            .election_votes
            .iter()
            .filter_map(|(id, &granted)| if granted { Some(id.clone()) } else { None })
            .collect();

        let win = self.config.majority(&granted_votes);
        if win {
            info!(
                "Node {} becomes leader for term {}",
                self.id, self.current_term
            );
            self.become_leader().await;
        }
    }

    // 启动心跳和日志应用定时器
    async fn reset_heartbeat_timer(&mut self) {
        if let Some(timer_id) = self.heartbeat_interval_timer_id {
            self.callbacks.del_timer(self.id.clone(), timer_id);
        }

        self.heartbeat_interval_timer_id = Some(
            self.callbacks
                .set_heartbeat_timer(self.id.clone(), self.heartbeat_interval),
        );
    }

    async fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.current_election_id = None;

        // 初始化复制状态
        let last_log_index = self.get_last_log_index().await;
        self.next_index.clear();
        self.match_index.clear();
        self.follower_snapshot_states.clear();
        self.follower_last_snapshot_index.clear();
        self.snapshot_probe_schedules.clear();

        for peer in &self.peers {
            self.next_index.insert(peer.clone(), last_log_index + 1);
            self.match_index.insert(peer.clone(), 0);
        }

        self.reset_heartbeat_timer().await;

        if let Err(err) = self
            .callbacks
            .state_changed(self.id.clone(), Role::Leader)
            .await
        {
            log::error!(
                "Failed to change state to Leader for node {}: {}",
                self.id,
                err
            );
        }

        // 立即发送心跳
        self.broadcast_append_entries().await;

        // 启动日志应用定时器
        self.adjust_apply_interval().await;
    }

    async fn reset_election(&mut self) {
        self.current_election_id = None;
        let new_timeout = self.election_timeout_min
            + rand::random::<u64>() % (self.election_timeout_max - self.election_timeout_min + 1);
        self.election_timeout = Duration::from_millis(new_timeout);

        if let Some(timer_id) = self.election_timer {
            self.callbacks.del_timer(self.id.clone(), timer_id);
        }

        self.election_timer = Some(
            self.callbacks
                .set_election_timer(self.id.clone(), self.election_timeout),
        );
    }

    // === 日志同步相关逻辑 ===
    async fn handle_heartbeat_timeout(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        self.broadcast_append_entries().await;

        // 重置心跳定时器
        self.reset_heartbeat_timer().await;

        // 定期检查联合配置状态
        if self.config.joint.is_some() {
            self.check_joint_exit_condition().await;
        }
    }

    async fn broadcast_append_entries(&mut self) {
        let current_term = self.current_term;
        let leader_id = self.id.clone();
        let leader_commit = self.commit_index;
        let last_log_index = self.get_last_log_index().await;
        let max_batch_size = 100;
        let now = Instant::now();

        // 检查并执行到期的快照探测计划
        self.process_pending_probes(now).await;

        // 向所有节点发送日志或探测消息
        let peers = self.get_effective_peers();
        for peer in peers {
            if peer == self.id {
                continue;
            }

            // 检查Follower是否正在安装快照
            if let Some(state) = self.follower_snapshot_states.get(&peer) {
                match state {
                    InstallSnapshotState::Installing => {
                        // 探测计划已处理，此处不需要额外操作
                        continue;
                    }
                    InstallSnapshotState::Failed(_) => {
                        // 失败状态，尝试重新发送快照
                        self.send_snapshot_to(peer.clone()).await;
                        continue;
                    }
                    _ => {}
                }
            }

            // 检查是否需要发送快照（日志差距过大）
            let next_idx = *self.next_index.get(&peer).unwrap_or(&1);
            if next_idx <= self.last_snapshot_index {
                self.send_snapshot_to(peer.clone()).await;
                continue;
            }

            // 构造AppendEntries请求
            let prev_log_index = next_idx - 1;
            let prev_log_term = if prev_log_index == 0 {
                0
            } else if prev_log_index <= self.last_snapshot_index {
                self.last_snapshot_term
            } else {
                self.callbacks
                    .get_log_term(self.id.clone(), prev_log_index)
                    .await
                    .unwrap_or(0)
            };

            let entries = match self
                .callbacks
                .get_log_entries(self.id.clone(), next_idx, last_log_index + 1)
                .await
            {
                Ok(entries) => entries.into_iter().take(max_batch_size).collect(),
                Err(_) => vec![],
            };

            let req = AppendEntriesRequest {
                term: current_term,
                leader_id: leader_id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                request_id: RequestId::new(),
            };

            if let Err(err) = self
                .callbacks
                .send_append_entries_request(self.id.clone(), peer, req)
                .await
            {
                log::warn!(
                    "node {}: send append entries request failed: {}",
                    self.id,
                    err
                );
            }
        }
    }

    async fn handle_append_entries(&mut self, request: AppendEntriesRequest) {
        // 处理更低任期的请求
        if request.term < self.current_term {
            let conflict_term = if request.prev_log_index <= self.last_snapshot_index {
                Some(self.last_snapshot_term)
            } else {
                self.callbacks
                    .get_log_term(self.id.clone(), request.prev_log_index)
                    .await
                    .ok()
            };

            let resp = AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_index: Some(self.get_last_log_index().await + 1),
                conflict_term,
                request_id: request.request_id,
            };
            if let Err(err) = self
                .callbacks
                .send_append_entries_response(self.id.clone(), request.leader_id, resp)
                .await
            {
                log::error!(
                    "node {}: failed to send AppendEntriesResponse: {}",
                    self.id,
                    err
                );
            }
            return;
        }

        // 切换为Follower并重置定时器
        self.role = Role::Follower;
        self.leader_id = Some(request.leader_id.clone());
        self.current_term = request.term;
        self.last_heartbeat = Instant::now();

        // 重置超时选举定时器
        self.reset_election().await;

        // 日志连续性检查
        let prev_log_ok = if request.prev_log_index == 0 {
            true // 从0开始的日志无需检查
        } else if request.prev_log_index <= self.last_snapshot_index {
            // 快照覆盖的日志，检查任期是否匹配快照
            request.prev_log_term == self.last_snapshot_term
        } else {
            // 检查日志任期是否匹配
            self.callbacks
                .get_log_term(self.id.clone(), request.prev_log_index)
                .await
                .unwrap_or(0)
                == request.prev_log_term
        };

        if !prev_log_ok {
            let conflict_idx = if request.prev_log_index > self.get_last_log_index().await {
                self.get_last_log_index().await + 1
            } else {
                request.prev_log_index
            };

            let conflict_term = if request.prev_log_index <= self.last_snapshot_index {
                Some(self.last_snapshot_term)
            } else {
                self.callbacks
                    .get_log_term(self.id.clone(), request.prev_log_index)
                    .await
                    .ok()
            };

            let resp = AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_index: Some(conflict_idx),
                conflict_term,
                request_id: request.request_id,
            };
            if let Err(err) = self
                .callbacks
                .send_append_entries_response(self.id.clone(), request.leader_id, resp)
                .await
            {
                log::error!(
                    "node {}: failed to send AppendEntriesResponse: {}",
                    self.id,
                    err
                );
            }
            return;
        }

        // 截断冲突日志并追加新日志
        if request.prev_log_index < self.get_last_log_index().await {
            let _ = self
                .callbacks
                .truncate_log_suffix(self.id.clone(), request.prev_log_index + 1)
                .await;
        }

        // 验证并追加新日志
        if !request.entries.is_empty() {
            // 验证日志条目连续性和有效性
            if let Some(first_entry) = request.entries.first() {
                if first_entry.index != request.prev_log_index + 1 {
                    let resp = AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        conflict_index: Some(request.prev_log_index + 1),
                        conflict_term: None,
                        request_id: request.request_id,
                    };
                    if let Err(err) = self
                        .callbacks
                        .send_append_entries_response(self.id.clone(), request.leader_id, resp)
                        .await
                    {
                        log::error!(
                            "node {}: failed to send AppendEntriesResponse: {}",
                            self.id,
                            err
                        );
                    }
                    return;
                }

                // 检查是否有重复的客户端请求
                for entry in &request.entries {
                    if let Some(_) = entry.client_request_id {
                        // 已提交的日志不能被修改
                        if entry.index <= self.commit_index {
                            let resp = AppendEntriesResponse {
                                term: self.current_term,
                                success: false,
                                conflict_index: Some(entry.index),
                                conflict_term: Some(entry.term),
                                request_id: request.request_id,
                            };
                            if let Err(err) = self
                                .callbacks
                                .send_append_entries_response(
                                    self.id.clone(),
                                    request.leader_id,
                                    resp,
                                )
                                .await
                            {
                                log::error!(
                                    "node {}: failed to send AppendEntriesResponse: {}",
                                    self.id,
                                    err
                                );
                            }
                            return;
                        }
                    }
                }
            }

            let _ = self
                .callbacks
                .append_log_entries(self.id.clone(), &request.entries)
                .await;
        }

        // 更新提交索引
        if request.leader_commit > self.commit_index {
            self.commit_index =
                std::cmp::min(request.leader_commit, self.get_last_log_index().await);
        }

        // 发送成功响应
        let resp = AppendEntriesResponse {
            term: self.current_term,
            success: true,
            conflict_index: None,
            conflict_term: None,
            request_id: request.request_id,
        };
        if let Err(err) = self
            .callbacks
            .send_append_entries_response(self.id.clone(), request.leader_id, resp)
            .await
        {
            log::error!(
                "node {}: failed to send AppendEntriesResponse: {}",
                self.id,
                err
            );
        }

        self.apply_committed_logs().await;
    }

    async fn handle_append_entries_response(
        &mut self,
        peer: NodeId,
        response: AppendEntriesResponse,
    ) {
        if self.role != Role::Leader {
            return;
        }

        // 处理更高任期
        if response.term > self.current_term {
            self.current_term = response.term;
            self.role = Role::Follower;
            self.voted_for = None;
            if let Err(err) = self
                .callbacks
                .save_hard_state(self.id.clone(), self.current_term, self.voted_for.clone())
                .await
            {
                log::error!("node {}: Failed to save hard state: {}", self.id, err);
            }
            if let Err(err) = self
                .callbacks
                .state_changed(self.id.clone(), Role::Follower)
                .await
            {
                log::error!(
                    "node {}: Failed to change state to Follower: {}",
                    self.id,
                    err
                );
            }
            return;
        }

        // 更新复制状态
        if response.success {
            let req_next_idx = self.next_index.get(&peer).copied().unwrap_or(1);
            let entries_len = self
                .callbacks
                .get_log_entries(
                    self.id.clone(),
                    req_next_idx,
                    self.get_last_log_index().await + 1,
                )
                .await
                .unwrap_or_default()
                .len() as u64;
            let new_match_idx = req_next_idx + entries_len - 1;
            let new_next_idx = new_match_idx + 1;

            self.match_index.insert(peer.clone(), new_match_idx);
            self.next_index.insert(peer.clone(), new_next_idx);

            // 尝试更新commit_index
            self.update_commit_index().await;
        } else {
            // 日志冲突，更高效的回退策略
            let current_next = self.next_index.get(&peer).copied().unwrap_or(1);
            let new_next = if let (Some(conflict_term), Some(conflict_index)) =
                (response.conflict_term, response.conflict_index)
            {
                // 查找冲突任期的最后一个日志索引
                let mut idx = conflict_index - 1;
                while idx > 0 {
                    let term = if idx <= self.last_snapshot_index {
                        self.last_snapshot_term
                    } else {
                        match self.callbacks.get_log_term(self.id.clone(), idx).await {
                            Ok(t) => t,
                            Err(e) => {
                                log::error!(
                                    "node {}: failed to get log term for index {}: {}",
                                    self.id,
                                    idx,
                                    e
                                );
                                break;
                            }
                        }
                    };

                    if term < conflict_term {
                        break;
                    } else {
                        info!(
                            "Term {} at index {} matches conflict_term {}",
                            term, idx, conflict_term
                        );
                    }

                    idx -= 1;
                }
                idx + 1
            } else {
                response.conflict_index.unwrap_or(current_next - 1)
            };
            self.next_index.insert(peer.clone(), new_next.max(1));
        }

        // 检查领导权转移状态
        let reply_for_transfer = if let Some(transfer_target) = &self.leader_transfer_target {
            if &peer == transfer_target && response.term == self.current_term {
                Some(response.clone())
            } else {
                None
            }
        } else {
            None
        };

        // 若当前处于联合配置，更新确认状态
        if self.config.joint.is_some() && response.success {
            self.check_joint_exit_condition().await;
        }

        // 处理领导权转移
        if let Some(transfer_reply) = reply_for_transfer {
            self.process_leader_transfer_target_response(peer, transfer_reply)
                .await;
        }
    }

    /// 处理目标节点的日志响应
    async fn process_leader_transfer_target_response(
        &mut self,
        peer: NodeId,
        reply: AppendEntriesResponse,
    ) {
        if reply.success {
            // 检查目标节点日志是否最新
            let target_match_index = self.match_index.get(&peer).copied().unwrap_or(0);
            let last_log_index = self.get_last_log_index().await;

            if target_match_index >= last_log_index {
                // 目标节点日志最新，可以转移领导权
                self.transfer_leadership_to(peer).await;
                return;
            }
        }

        // 目标节点日志不够新，需要发送更多日志
        self.send_heartbeat_to(peer).await;
    }

    /// 转移领导权给目标节点
    async fn transfer_leadership_to(&mut self, target: NodeId) {
        // 发送超时投票请求，让目标节点立即开始选举
        let req = RequestVoteRequest {
            term: self.current_term + 1, // 使用更高任期触发选举
            candidate_id: target.clone(),
            last_log_index: self.get_last_log_index().await,
            last_log_term: self.get_last_log_term().await,
            request_id: RequestId::new(),
        };

        if let Err(err) = self
            .callbacks
            .send_request_vote_request(self.id.clone(), target.clone(), req)
            .await
        {
            log::error!(
                "Failed to send request vote request to {} during leader transfer: {}",
                target,
                err
            );
        }

        // 完成转移，响应客户端
        if let Some(request_id) = self.leader_transfer_request_id.take() {
            if let Err(err) = self
                .callbacks
                .client_response(
                    self.id.clone(),
                    request_id,
                    Ok(0), // 成功代码
                )
                .await
            {
                log::error!(
                    "Failed to send client response for leader transfer completion: {}",
                    err
                );
            }
        }

        // 清理转移状态
        self.leader_transfer_target = None;
        self.leader_transfer_start_time = None;
    }

    // === 快照相关逻辑 ===
    async fn send_snapshot_to(&mut self, target: NodeId) {
        let snap = match self.callbacks.load_snapshot(self.id.clone()).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                error!("没有可用的快照，无法发送");
                return;
            }
            Err(e) => {
                error!("加载快照失败: {}", e);
                return;
            }
        };

        // 验证快照与当前日志的一致性
        if !self.verify_snapshot_consistency(&snap).await {
            tracing::error!("快照与当前日志不一致，无法发送");
            return;
        }

        let req = InstallSnapshotRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            last_included_index: snap.index,
            last_included_term: snap.term,
            data: snap.data.clone(),
            request_id: RequestId::new(),
            is_probe: false,
        };

        // 记录发送的快照信息
        self.follower_last_snapshot_index
            .insert(target.clone(), snap.index);
        self.follower_snapshot_states
            .insert(target.clone(), InstallSnapshotState::Installing);

        // 为这个Follower创建探测计划
        self.schedule_snapshot_probe(target.clone(), Duration::from_secs(10), 30);

        if let Err(err) = self
            .callbacks
            .send_install_snapshot_request(self.id.clone(), target, req)
            .await
        {
            log::error!(
                "node {}: failed to send InstallSnapshotRequest: {}",
                self.id,
                err
            );
        }
    }

    // 验证快照与当前日志的一致性
    async fn verify_snapshot_consistency(&self, snap: &Snapshot) -> bool {
        // 检查快照的最后一条日志是否与当前日志匹配
        if snap.index == 0 {
            return true; // 空快照总是有效的
        }

        // 检查快照索引是否小于等于最后一条日志索引
        let last_log_index = self.get_last_log_index().await;
        if snap.index > last_log_index {
            return false;
        }

        // 检查快照的任期是否与对应日志的任期匹配
        let log_term = if snap.index <= self.last_snapshot_index {
            self.last_snapshot_term
        } else {
            match self
                .callbacks
                .get_log_term(self.id.clone(), snap.index)
                .await
            {
                Ok(term) => term,
                Err(_) => return false,
            }
        };

        snap.term == log_term
    }

    // 发送探测消息检查快照安装状态（Leader端）
    async fn probe_snapshot_status(&mut self, target: NodeId) {
        debug!(
            "Probing snapshot status for follower {} at term {}, last_snapshot_index {}",
            target,
            self.current_term,
            self.follower_last_snapshot_index.get(&target).unwrap_or(&0)
        );

        let last_snap_index = self
            .follower_last_snapshot_index
            .get(&target)
            .copied()
            .unwrap_or(0);

        let req = InstallSnapshotRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            last_included_index: last_snap_index,
            last_included_term: 0, // 探测消息不需要实际任期
            data: vec![],          // 空数据
            request_id: RequestId::new(),
            is_probe: true,
        };

        let _ = self
            .error_handler
            .handle(
                self.callbacks
                    .send_install_snapshot_request(self.id.clone(), target.clone(), req)
                    .await,
                "send_install_snapshot_request",
                Some(&target),
            )
            .await;
    }

    async fn handle_install_snapshot(&mut self, request: InstallSnapshotRequest) {
        // 处理更低任期的请求
        if request.term < self.current_term {
            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: InstallSnapshotState::Failed("Term too low".into()),
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_reply(
                            self.id.clone(),
                            request.leader_id.clone(),
                            resp,
                        )
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        // 切换为Follower并更新状态
        self.role = Role::Follower;
        self.current_term = request.term;
        self.last_heartbeat = Instant::now();

        // 重置选举超时
        self.reset_election().await;

        // 处理空探测消息
        if request.is_probe {
            // 返回当前快照安装状态
            let current_state = if let Some(req_id) = &self.current_snapshot_request_id {
                // 如果是正在处理的那个快照请求
                if *req_id == request.request_id {
                    InstallSnapshotState::Installing
                } else {
                    InstallSnapshotState::Failed("No such snapshot in progress".into())
                }
            } else {
                // 没有正在处理的快照
                InstallSnapshotState::Success
            };

            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: current_state,
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_reply(
                            self.id.clone(),
                            request.leader_id.clone(),
                            resp,
                        )
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        // 仅处理比当前快照更新的快照
        if request.last_included_index <= self.last_snapshot_index {
            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: InstallSnapshotState::Success,
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_reply(
                            self.id.clone(),
                            request.leader_id.clone(),
                            resp,
                        )
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        // 验证快照配置与当前配置的兼容性
        if !self.verify_snapshot_config_compatibility(&request).await {
            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: InstallSnapshotState::Failed(
                    "Snapshot config incompatible with current config".into(),
                ),
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_reply(
                            self.id.clone(),
                            request.leader_id.clone(),
                            resp,
                        )
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        // 记录当前正在处理的快照请求
        self.current_snapshot_request_id = Some(request.request_id);

        // 立即返回正在安装状态，不等待实际处理完成
        let resp = InstallSnapshotResponse {
            term: self.current_term,
            request_id: request.request_id,
            state: InstallSnapshotState::Installing,
        };
        self.error_handler
            .handle_void(
                self.callbacks
                    .send_install_snapshot_reply(self.id.clone(), request.leader_id.clone(), resp)
                    .await,
                "send_install_snapshot_reply",
                Some(&request.leader_id),
            )
            .await;

        // 将快照数据交给业务层处理（异步）
        // 注意：这里不阻塞Raft状态机，实际处理由业务层完成
        self.error_handler
            .handle_void(
                self.callbacks
                    .process_snapshot(
                        self.id.clone(),
                        request.last_included_index,
                        request.last_included_term,
                        request.data,
                        request.request_id,
                    )
                    .await,
                "process_snapshot",
                None,
            )
            .await;
    }

    // 验证快照配置与当前配置的兼容性
    async fn verify_snapshot_config_compatibility(&self, req: &InstallSnapshotRequest) -> bool {
        // 检查快照配置是否与当前配置兼容
        // 简单检查：快照中的配置应该是当前配置的祖先或相同
        // 实际实现可能需要更复杂的检查逻辑

        // 对于空配置，总是兼容的
        if self.config.voters.is_empty() {
            return true;
        }

        // 检查快照中的配置是否是当前配置的子集或相同
        let snap_config = match bincode::deserialize::<ClusterConfig>(&req.data) {
            Ok(conf) => conf,
            Err(_) => return false,
        };

        // 基本兼容性检查
        snap_config.voters.iter().all(|id| self.config.contains(id))
    }

    // 业务层完成快照处理后调用此方法更新状态（Follower端）
    pub async fn complete_snapshot_installation(
        &mut self,
        request_id: RequestId,
        success: bool,
        _reason: Option<String>,
        index: u64,
        term: u64,
    ) {
        // 检查是否是当前正在处理的快照
        if self.current_snapshot_request_id != Some(request_id) {
            return;
        }

        // 更新快照状态
        if success {
            // 验证快照索引对应的日志任期是否正确
            let expected_term = if index <= self.last_snapshot_index {
                self.last_snapshot_term
            } else {
                match self.callbacks.get_log_term(self.id.clone(), index).await {
                    Ok(t) => t,
                    Err(_) => {
                        tracing::error!("无法验证快照任期，安装失败");
                        self.current_snapshot_request_id = None;
                        return;
                    }
                }
            };

            if term != expected_term {
                tracing::error!("快照任期不匹配，安装失败");
                self.current_snapshot_request_id = None;
                return;
            }

            self.last_snapshot_index = index;
            self.last_snapshot_term = term;
            self.commit_index = self.commit_index.max(index);
            self.last_applied = self.last_applied.max(index);
        }

        // 清除当前处理标记
        self.current_snapshot_request_id = None;
    }

    async fn handle_install_snapshot_response(
        &mut self,
        peer: NodeId,
        reply: InstallSnapshotResponse,
    ) {
        if self.role != Role::Leader {
            return;
        }

        // 处理更高任期
        if reply.term > self.current_term {
            self.current_term = reply.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.error_handler
                .handle_void(
                    self.callbacks
                        .save_hard_state(self.id.clone(), self.current_term, self.voted_for.clone())
                        .await,
                    "save_hard_state",
                    None,
                )
                .await;
            self.error_handler
                .handle_void(
                    self.callbacks
                        .state_changed(self.id.clone(), Role::Follower)
                        .await,
                    "state_changed",
                    None,
                )
                .await;
            // 清除该节点的探测计划
            self.remove_snapshot_probe(&peer);
            return;
        }

        // 更新Follower的快照状态
        self.follower_snapshot_states
            .insert(peer.clone(), reply.state.clone());

        match reply.state {
            InstallSnapshotState::Success => {
                // 快照安装成功，更新复制状态
                let snap_index = self
                    .follower_last_snapshot_index
                    .get(&peer)
                    .copied()
                    .unwrap_or(0);
                self.next_index.insert(peer.clone(), snap_index + 1);
                self.match_index.insert(peer.clone(), snap_index);
                tracing::info!("Follower {} completed snapshot installation", peer);
                // 清除探测计划
                self.remove_snapshot_probe(&peer);
            }
            InstallSnapshotState::Installing => {
                // 仍在安装中，更新探测计划（延长尝试次数）
                tracing::debug!("Follower {} is still installing snapshot", peer);
                self.extend_snapshot_probe(&peer);
            }
            InstallSnapshotState::Failed(reason) => {
                tracing::warn!("Follower {} snapshot install failed: {}", peer, reason);
                // 清除探测计划
                self.remove_snapshot_probe(&peer);
                // 可以安排重试
                self.schedule_snapshot_retry(peer).await;
            }
        }
    }

    // 安排快照状态探测（无内部线程）
    fn schedule_snapshot_probe(&mut self, peer: NodeId, interval: Duration, max_attempts: u32) {
        // 先移除可能存在的旧计划
        self.remove_snapshot_probe(&peer);

        // 添加新的探测计划
        self.snapshot_probe_schedules.push(SnapshotProbeSchedule {
            peer: peer.clone(),
            next_probe_time: Instant::now() + interval,
            interval,
            max_attempts,
            attempts: 0,
        });
    }

    // 延长快照探测计划
    fn extend_snapshot_probe(&mut self, peer: &NodeId) {
        if let Some(schedule) = self
            .snapshot_probe_schedules
            .iter_mut()
            .find(|s| &s.peer == peer)
        {
            if schedule.attempts < schedule.max_attempts {
                schedule.attempts += 1;
                schedule.next_probe_time = Instant::now() + schedule.interval;
            } else {
                // 达到最大尝试次数，标记为失败
                self.follower_snapshot_states.insert(
                    peer.clone(),
                    InstallSnapshotState::Failed("Max probe attempts reached".into()),
                );
                self.remove_snapshot_probe(peer);
            }
        }
    }

    // 移除快照探测计划
    fn remove_snapshot_probe(&mut self, peer: &NodeId) {
        self.snapshot_probe_schedules.retain(|s| &s.peer != peer);
    }

    // 处理到期的探测计划
    async fn process_pending_probes(&mut self, now: Instant) {
        // 收集需要执行的探测
        let pending_peers: Vec<NodeId> = self
            .snapshot_probe_schedules
            .iter()
            .filter(|s| s.next_probe_time <= now)
            .map(|s| s.peer.clone())
            .collect();

        // 执行每个到期的探测
        for peer in pending_peers {
            self.probe_snapshot_status(peer).await;
        }
    }

    // 安排快照重发（无内部线程）
    async fn schedule_snapshot_retry(&mut self, peer: NodeId) {
        // 直接在当前事件循环中延迟发送，而非启动新线程
        // 实际实现中可根据需要调整重试延迟
        self.send_snapshot_to(peer).await;
    }

    // === 客户端请求与日志应用 ===
    async fn handle_client_propose(&mut self, cmd: Command, request_id: RequestId) {
        if self.role != Role::Leader {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::NotLeader(self.leader_id.clone())),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 检查是否是重复请求
        if self.recent_client_requests.contains_key(&request_id) {
            // 查找已存在的日志索引
            if let Some(&index) = self.client_requests.get(&request_id) {
                // 如果已经提交，直接返回结果
                if index <= self.commit_index {
                    self.error_handler
                        .handle_void(
                            self.callbacks
                                .client_response(self.id.clone(), request_id, Ok(index))
                                .await,
                            "client_response",
                            None,
                        )
                        .await;
                    return;
                }
                // 否则等待已存在的日志提交
                return;
            }
        }

        // 生成日志条目
        let last_idx = self.get_last_log_index().await;
        let new_entry = LogEntry {
            term: self.current_term,
            index: last_idx + 1,
            command: cmd,
            is_config: false,                    // 普通命令
            client_request_id: Some(request_id), // 关联客户端请求ID
        };

        // 追加日志
        let result = self
            .callbacks
            .append_log_entries(self.id.clone(), &[new_entry.clone()])
            .await;
        if result.is_err() {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "failed to append log"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 记录客户端请求与日志索引的映射
        self.client_requests.insert(request_id, last_idx + 1);
        self.recent_client_requests.insert(request_id, last_idx + 1);

        // 立即同步日志
        self.broadcast_append_entries().await;
    }

    async fn apply_committed_logs(&mut self) {
        // 应用已提交但未应用的日志
        if self.last_applied >= self.commit_index {
            return;
        }

        let start = self.last_applied + 1;

        let end = std::cmp::min(
            self.commit_index,
            self.last_applied + self.options.apply_batch_size,
        );

        let entries = match self
            .callbacks
            .get_log_entries(self.id.clone(), start, end + 1)
            .await
        {
            Ok(entries) => entries,
            Err(e) => {
                tracing::error!("读取日志失败: {}", e);
                return;
            }
        };

        // 逐个应用日志
        for entry in entries {
            // 验证日志尚未被应用且已提交
            if entry.index <= self.last_applied || entry.index > self.commit_index {
                continue;
            }
            if entry.is_config {
                if let Ok(new_config) = bincode::deserialize::<ClusterConfig>(&entry.command) {
                    if !new_config.is_valid() {
                        tracing::error!("invalid cluster config received, ignoring");
                        continue;
                    }
                    // 记录节点对新配置的确认
                    if new_config.joint.is_some() {
                        // 联合配置：记录当前节点确认
                        self.new_config_ack_nodes.insert(self.id.clone());
                    }

                    self.config = new_config;
                    let _ = self
                        .callbacks
                        .save_cluster_config(self.id.clone(), self.config.clone())
                        .await;

                    // 检查是否可以退出联合配置
                    if self.config.joint.is_some() {
                        self.check_joint_exit_condition().await;
                    }
                }
            } else {
                let _ = self
                    .callbacks
                    .apply_command(self.id.clone(), entry.index, entry.term, entry.command)
                    .await;
                self.last_applied = entry.index;

                // 如果是客户端请求，返回响应
                self.check_client_response(entry.index).await;
            }
        }

        // 继续定时应用
        self.adjust_apply_interval().await;
    }

    async fn adjust_apply_interval(&mut self) {
        // 根据负载动态调整应用间隔
        let current_load = self.commit_index - self.last_applied;

        self.apply_interval = if current_load > 1000 {
            Duration::from_micros(100) // 高负载时更频繁
        } else {
            Duration::from_millis(10) // 低负载时减少频率
        };

        if let Some(timer) = self.apply_interval_timer {
            self.callbacks.del_timer(self.id.clone(), timer);
        }

        self.apply_interval_timer = Some(
            self.callbacks
                .set_apply_timer(self.id.clone(), self.apply_interval),
        );
    }

    async fn check_client_response(&mut self, log_index: u64) {
        // 查找该日志索引对应的客户端请求并响应
        let mut completed = vec![];
        for (req_id, idx) in &self.client_requests {
            if *idx == log_index {
                self.error_handler
                    .handle_void(
                        self.callbacks
                            .client_response(self.id.clone(), *req_id, Ok(log_index))
                            .await,
                        "client_response",
                        None,
                    )
                    .await;
                completed.push(*req_id);
            }
        }
        // 移除已响应的请求
        for req_id in completed {
            self.client_requests.remove(&req_id);
        }
    }

    // === 集群配置变更 ===
    async fn handle_change_config(&mut self, new_voters: HashSet<NodeId>, request_id: RequestId) {
        if self.role != Role::Leader {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::NotLeader(self.leader_id.clone())),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 检查是否已有配置变更在进行中
        if self.config_change_in_progress {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::Conflict(anyhow::anyhow!(
                                "config change in progress"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 验证新配置的合法性
        let new_config = ClusterConfig::simple(new_voters.clone());
        if !new_config.is_valid() {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "invalid cluster config"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 创建联合配置
        let old_voters = self.config.voters.clone();
        let mut joint_config = self.config.clone();
        if let Err(e) = joint_config.enter_joint(old_voters, new_voters) {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "Failed to enter joint config: {}",
                                e
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 生成配置变更日志
        let last_idx = self.get_last_log_index().await;
        let config_data = bincode::serialize(&joint_config).unwrap();
        let new_entry = LogEntry {
            term: self.current_term,
            index: last_idx + 1,
            command: config_data,
            is_config: true,
            client_request_id: Some(request_id),
        };

        // 追加配置变更日志
        let result = self
            .callbacks
            .append_log_entries(self.id.clone(), &[new_entry.clone()])
            .await;
        if result.is_err() {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "failed to append config log"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 记录配置变更状态
        self.config_change_in_progress = true;
        self.config_change_start_time = Some(Instant::now());
        self.client_requests.insert(request_id, last_idx + 1);

        // 设置配置变更超时定时器
        self.config_change_timer = Some(
            self.callbacks
                .set_config_change_timer(self.id.clone(), self.config_change_timeout),
        );

        // 立即同步日志
        self.broadcast_append_entries().await;
    }

    async fn handle_config_change_timeout(&mut self) {
        if !self.config_change_in_progress || self.role != Role::Leader {
            return;
        }

        // 检查是否超时
        let start_time = match self.config_change_start_time {
            Some(t) => t,
            None => {
                self.config_change_in_progress = false;
                return;
            }
        };

        if start_time.elapsed() < self.config_change_timeout {
            // 未超时，重新设置定时器
            self.config_change_timer = Some(
                self.callbacks
                    .set_config_change_timer(self.id.clone(), self.config_change_timeout),
            );

            return;
        }

        // 配置变更超时，回滚到旧配置
        tracing::warn!("config change timed out, rolling back");

        // 生成回滚配置日志
        let last_idx = self.get_last_log_index().await;
        let old_config = ClusterConfig::simple(self.config.voters.clone());
        let config_data = bincode::serialize(&old_config).unwrap();
        let new_entry = LogEntry {
            term: self.current_term,
            index: last_idx + 1,
            command: config_data,
            is_config: true,
            client_request_id: None,
        };

        // 追加回滚配置日志
        let _ = self
            .callbacks
            .append_log_entries(self.id.clone(), &[new_entry.clone()])
            .await;

        // 重置配置变更状态
        self.config_change_in_progress = false;
        self.config_change_start_time = None;

        // 立即同步日志
        self.broadcast_append_entries().await;
    }

    // 检查联合配置退出条件
    async fn check_joint_exit_condition(&mut self) {
        if self.role != Role::Leader || self.config.joint.is_none() {
            return;
        }

        let joint = self.config.joint.as_ref().unwrap();
        let new_quorum = joint.new.len() / 2 + 1;

        // 条件1：新配置已获得多数派确认
        let new_config_acked = self
            .new_config_ack_nodes
            .iter()
            .filter(|id| joint.new.contains(*id))
            .count()
            >= new_quorum;

        // 条件2：新配置日志已提交
        let config_log_committed = self.config.joint_enter_time.is_some()
            && self.get_last_log_index().await >= self.commit_index;

        if new_config_acked && config_log_committed {
            // 正常退出联合配置
            self.exit_joint_config(false).await;
            return;
        }

        // 检查是否超时
        self.check_joint_timeout().await;
    }

    // 联合配置超时检查
    async fn check_joint_timeout(&mut self) {
        if let (Some(enter_time), Some(joint)) = (self.config.joint_enter_time, &self.config.joint)
        {
            if enter_time.elapsed() >= self.joint_timeout {
                tracing::warn!("joint config timeout, forcing exit");
                // 强制退出：检查新配置是否至少有一个节点确认
                let new_config_has_ack = self
                    .new_config_ack_nodes
                    .iter()
                    .any(|id| joint.new.contains(id));

                self.exit_joint_config(!new_config_has_ack).await;
            }
        }
    }

    // 退出联合配置实现
    async fn exit_joint_config(&mut self, rollback: bool) {
        let new_config = if rollback {
            // 回滚到旧配置
            if let Some(joint) = &self.config.joint {
                ClusterConfig::simple(joint.old.clone())
            } else {
                return; // 不在联合配置中，无需操作
            }
        } else {
            // 正常退出到新配置
            match self.config.leave_joint() {
                Ok(config) => config,
                Err(_) => return, // 退出失败，保持当前状态
            }
        };

        // 记录退出联合配置的日志
        let config_data = bincode::serialize(&new_config).unwrap();
        let last_idx = self.get_last_log_index().await;
        let exit_entry = LogEntry {
            term: self.current_term,
            index: last_idx + 1,
            command: config_data,
            is_config: true,
            client_request_id: None,
        };

        // 追加日志并同步
        let _ = self
            .callbacks
            .append_log_entries(self.id.clone(), &[exit_entry])
            .await;
        self.broadcast_append_entries().await;

        // 清理状态
        self.config = new_config;
        self.new_config_ack_nodes.clear();
        self.config_change_in_progress = false;
        self.error_handler
            .handle_void(
                self.callbacks
                    .save_cluster_config(self.id.clone(), self.config.clone())
                    .await,
                "save_cluster_config",
                None,
            )
            .await;
    }

    // === 辅助方法 ===
    fn get_effective_voters(&self) -> HashSet<NodeId> {
        match &self.config.joint {
            Some(j) => j.old.union(&j.new).cloned().collect(),
            None => self.config.voters.clone(),
        }
    }

    fn get_effective_peers(&self) -> Vec<NodeId> {
        self.get_effective_voters()
            .into_iter()
            .filter(|id| *id != self.id)
            .collect()
    }

    async fn get_last_log_index(&self) -> u64 {
        self.callbacks
            .get_last_log_index(self.id.clone())
            .await
            .unwrap_or(0)
            .max(self.last_snapshot_index)
    }

    async fn get_last_log_term(&self) -> u64 {
        let last_log_idx = self
            .callbacks
            .get_last_log_index(self.id.clone())
            .await
            .unwrap_or(0);
        if last_log_idx == 0 {
            self.last_snapshot_term
        } else {
            self.callbacks
                .get_log_term(self.id.clone(), last_log_idx)
                .await
                .unwrap_or(self.last_snapshot_term)
        }
    }

    /// 处理领导权转移请求
    pub async fn handle_leader_transfer(&mut self, target: NodeId, request_id: RequestId) {
        if self.role != Role::Leader {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::NotLeader(self.leader_id.clone())),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        if self.leader_transfer_target.is_some() {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "leader transfer already in progress"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        if !self.config.contains(&target) {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "target node not in cluster"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        if target == self.id {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "cannot transfer to self"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 记录转移状态
        self.leader_transfer_target = Some(target.clone());
        self.leader_transfer_request_id = Some(request_id);
        self.leader_transfer_start_time = Some(Instant::now());

        // 立即发送心跳重置目标节点选举计时器
        self.send_heartbeat_to(target).await;

        // 设置超时定时器

        self.leader_transfer_timer = Some(
            self.callbacks
                .set_leader_transfer_timer(self.id.clone(), self.leader_transfer_timeout),
        );
    }

    /// 发送心跳到特定节点
    async fn send_heartbeat_to(&mut self, target: NodeId) {
        let prev_log_index = self.next_index[&target] - 1;
        let prev_log_term = if prev_log_index == 0 {
            0
        } else if prev_log_index <= self.last_snapshot_index {
            self.last_snapshot_term
        } else {
            self.callbacks
                .get_log_term(self.id.clone(), prev_log_index)
                .await
                .unwrap_or(0)
        };

        let req = AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index,
            prev_log_term,
            entries: vec![], // 空日志表示心跳
            leader_commit: self.commit_index,
            request_id: RequestId::new(),
        };

        let _ = self
            .error_handler
            .handle(
                self.callbacks
                    .send_append_entries_request(self.id.clone(), target, req)
                    .await,
                "send_append_entries_request",
                None,
            )
            .await;
    }

    /// 生成快照并持久化
    async fn create_snapshot(&mut self) {
        let begin = Instant::now();

        // 1. 检查是否有需要快照的日志（避免重复快照）
        if self.commit_index <= self.last_snapshot_index {
            info!(
                "No new committed logs to snapshot (commit_index: {}, last_snapshot_index: {})",
                self.commit_index, self.last_snapshot_index
            );
            return;
        }

        // 2. 获取快照所需的元数据
        let snapshot_index = self.last_applied;
        let _snapshot_term = match self
            .callbacks
            .get_log_term(self.id.clone(), snapshot_index)
            .await
        {
            Ok(term) => term,
            Err(e) => {
                error!(
                    "Failed to get log term for snapshot at index {}: {}",
                    snapshot_index, e
                );
                return;
            }
        };

        // 3. 获取当前集群配置
        let config = self.config.clone();

        // 4. 让业务层生成快照数据
        let (snap_index, snap_term) = match self
            .error_handler
            .handle(
                self.callbacks.create_snapshot(self.id.clone()).await,
                "create_snapshot",
                None,
            )
            .await
        {
            Some((idx, term)) => (idx, term),
            None => {
                error!("Failed to create snapshot via callback");
                return;
            }
        };

        // 5. 读取快照数据（业务层应已持久化）
        let snap = match self.callbacks.load_snapshot(self.id.clone()).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                error!("No snapshot found after creation");
                return;
            }
            Err(e) => {
                error!("Failed to load snapshot after creation: {}", e);
                return;
            }
        };

        // 6. 校验快照元数据
        if snap.index != snap_index || snap.term != snap_term {
            error!(
                "Snapshot metadata mismatch: expected (idx={}, term={}), got (idx={}, term={})",
                snap_index, snap_term, snap.index, snap.term
            );
            return;
        }

        if snap.config != config {
            error!(
                "Snapshot config mismatch: expected {:?}, got {:?}",
                config, snap.config
            );
            return;
        }

        // 7. 截断日志前缀
        if let Err(e) = self
            .callbacks
            .truncate_log_prefix(self.id.clone(), snap_index + 1)
            .await
        {
            error!("Failed to truncate log prefix after snapshot: {}", e);
            // 不 return，继续更新状态
        }

        // 9. 更新本地状态
        self.last_snapshot_index = snap_index;
        self.last_snapshot_term = snap_term;
        self.last_applied = self.last_applied.max(snap_index);
        self.commit_index = self.commit_index.max(snap_index);

        let elapsed = begin.elapsed();
        info!(
            "Snapshot created at index {}, term {} (elapsed: {:?})",
            snap_index, snap_term, elapsed
        );
    }

    /// 处理领导权转移超时
    async fn handle_leader_transfer_timeout(&mut self) {
        if let (Some(_target), Some(request_id)) = (
            self.leader_transfer_target.take(),
            self.leader_transfer_request_id.take(),
        ) {
            self.leader_transfer_start_time = None;

            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            self.id.clone(),
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "leader transfer timeout"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
        }
    }

    async fn update_commit_index(&mut self) {
        if self.role != Role::Leader {
            return;
        }

        // 获取各节点的match index（包含自身）
        let mut match_indices: Vec<u64> = self.match_index.values().cloned().collect();
        match_indices.push(self.get_last_log_index().await);
        match_indices.sort_unstable_by(|a, b| b.cmp(a));

        // 计算候选提交索引
        let candidate_index = match self.config.quorum() {
            QuorumRequirement::Joint { old, new } => {
                // 联合配置需要同时满足两个quorum
                let old_majority = self
                    .find_majority_index(
                        &match_indices,
                        &self.config.joint.as_ref().unwrap().old,
                        old,
                    )
                    .await;
                let new_majority = self
                    .find_majority_index(
                        &match_indices,
                        &self.config.joint.as_ref().unwrap().new,
                        new,
                    )
                    .await;

                // 取两个配置的交集最小值
                match (old_majority, new_majority) {
                    (Some(old_idx), Some(new_idx)) => std::cmp::min(old_idx, new_idx),
                    _ => return, // 未达到双重要求
                }
            }
            QuorumRequirement::Simple(quorum) => {
                if match_indices.len() >= quorum {
                    match_indices[quorum - 1]
                } else {
                    return;
                }
            }
        };

        // 任期检查
        if candidate_index > self.commit_index {
            let candidate_term = match self
                .error_handler
                .handle(
                    self.callbacks
                        .get_log_term(self.id.clone(), candidate_index)
                        .await,
                    "get_log_term",
                    None,
                )
                .await
            {
                Some(term) => term,
                None => return,
            };

            if candidate_term == self.current_term {
                self.commit_index = candidate_index;
            } else {
                // 检查是否有当前任期的日志被提交
                let mut has_current_term = false;
                for i in (self.commit_index + 1)..=candidate_index {
                    if self
                        .callbacks
                        .get_log_term(self.id.clone(), i)
                        .await
                        .unwrap_or(0)
                        == self.current_term
                    {
                        has_current_term = true;
                        break;
                    }
                }

                if has_current_term {
                    self.commit_index = candidate_index;
                }
            }
        }
    }

    // 辅助方法：在指定配置中找到多数派索引
    async fn find_majority_index(
        &self,
        _sorted_indices: &[u64],
        voters: &HashSet<NodeId>,
        quorum: usize,
    ) -> Option<u64> {
        let mut voter_indices: Vec<u64> = Vec::new();

        // 收集该配置中所有节点的match index
        for voter in voters {
            if voter == &self.id {
                voter_indices.push(self.get_last_log_index_sync().await); // Leader自身
            } else {
                voter_indices.push(self.match_index.get(voter).copied().unwrap_or(0));
            }
        }

        voter_indices.sort_unstable_by(|a, b| b.cmp(a));

        if voter_indices.len() >= quorum {
            Some(voter_indices[quorum - 1])
        } else {
            None
        }
    }

    async fn get_last_log_index_sync(&self) -> u64 {
        // 返回快照索引和日志索引的最大值
        let last_idx = self.get_last_log_index().await;
        self.last_snapshot_index.max(
            // match_index 只在 Leader 端有意义，这里用于 Leader 自身
            // 但 Leader 自身的日志索引应由存储层维护
            // 这里假设 last_applied >= last_snapshot_index
            last_idx,
        )
    }
}
