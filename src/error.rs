use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info, warn};

use crate::{RaftId, RequestId, Role, TimerId};

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
    NodeNotFound(RaftId),

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

    #[error("Snapshot creation failed: {0}")]
    SnapshotCreationFailed(String),

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
    NotLeader(Option<RaftId>),

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
    DataCorrupted(anyhow::Error),

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

    #[error("Invalid joint configuration: {0}")]
    InvalidJoint(String),
}

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
            StorageError::SnapshotCreationFailed(_) => ErrorSeverity::Recoverable,
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
            StorageError::SnapshotCreationFailed(msg) => {
                format!("Snapshot creation failed: {}", msg)
            }
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
            SnapshotError::DataCorrupted(_) => ErrorSeverity::Fatal,
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
            SnapshotError::DataCorrupted(e) => format!("Snapshot data corrupted: {}", e),
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
    node_id: RaftId,
    readonly_mode: bool,
}

impl CallbackErrorHandler {
    pub fn new(node_id: RaftId) -> Self {
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
        target: Option<&RaftId>,
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
                        info!("[IGNORABLE] {} failed: {}", ctx, e.context());
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
        target: Option<&RaftId>,
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
