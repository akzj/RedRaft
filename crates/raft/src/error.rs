use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info, warn};

use crate::{RaftId, RequestId, Role, TimerId};

/// Top-level Raft error type
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

/// RPC communication related error
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

/// Storage related error
#[derive(Clone, Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] Arc<anyhow::Error>),

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

    #[error("Channel closed")]
    ChannelClosed,
}

/// Timer related error
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

/// Client related error
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

/// State change related error
#[derive(Debug, Error)]
pub enum StateChangeError {
    #[error("Invalid state transition from {0} to {1}")]
    InvalidTransition(Role, Role),

    #[error("Leadership transfer failed")]
    LeadershipTransferFailed,

    #[error("Configuration change in progress")]
    ConfigChangeInProgress,
}

/// State machine application related error
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

    #[error("Command deserialization failed: {0}")]
    DeserializationFailed(anyhow::Error),
}

/// Snapshot related error
#[derive(Debug, Clone, Error)]
pub enum SnapshotError {
    #[error("Snapshot at index {0} already exists")]
    AlreadyExists(u64),

    #[error("Snapshot at index {0} is too old")]
    TooOld(u64),

    #[error("Snapshot data corrupted")]
    DataCorrupted(Arc<anyhow::Error>),

    #[error("Snapshot creation in progress")]
    InProgress,

    #[error("Snapshot size exceeds limit")]
    SizeExceeded,

    #[error("Snapshot not supported")]
    NotSupported,

    #[error("Snapshot error: Unknown")]
    Unknown,
}

/// Configuration change related error
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

// === Unified Error Handling Mechanism ===
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorSeverity {
    Fatal,       // Need to terminate current operation and possibly enter readonly mode
    Recoverable, // Can retry or degrade processing
    Ignorable,   // Only need to log
}

pub trait ErrorHandler {
    fn severity(&self) -> ErrorSeverity;
    fn context(&self) -> String;
}

// Implement ErrorHandler for RPC errors
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

// Implement ErrorHandler for storage errors
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
            StorageError::ChannelClosed => ErrorSeverity::Fatal,
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
            StorageError::ChannelClosed => "Storage channel closed".to_string(),
        }
    }
}

// Implement ErrorHandler for client errors
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

// Implement ErrorHandler for timer errors
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

// Implement ErrorHandler for snapshot errors
impl ErrorHandler for SnapshotError {
    fn severity(&self) -> ErrorSeverity {
        match self {
            SnapshotError::AlreadyExists(_) => ErrorSeverity::Ignorable,
            SnapshotError::TooOld(_) => ErrorSeverity::Ignorable,
            SnapshotError::DataCorrupted(_) => ErrorSeverity::Fatal,
            SnapshotError::InProgress => ErrorSeverity::Recoverable,
            SnapshotError::SizeExceeded => ErrorSeverity::Recoverable,
            SnapshotError::NotSupported => ErrorSeverity::Fatal,
            SnapshotError::Unknown => ErrorSeverity::Fatal,
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
            SnapshotError::Unknown => "Unknown snapshot error".to_string(),
        }
    }
}

// Implement ErrorHandler for state change errors
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

    /// Unified callback error handling
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

    /// Handle operations that don't need return values
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
            // Specific logic for entering readonly mode can be added here
        }
    }

    pub fn is_readonly(&self) -> bool {
        self.readonly_mode
    }
}

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
