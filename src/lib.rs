//! Raft-Lite: A lightweight Raft consensus implementation
//!
//! This crate provides a modular implementation of the Raft consensus algorithm.
//!
//! ## Module Structure
//!
//! - `types` - Basic type definitions (RaftId, RequestId, etc.)
//! - `event` - Event and Role enums
//! - `state` - Raft state machine implementation
//! - `message` - Message types for Raft communication
//! - `traits` - Trait definitions for callbacks and storage
//! - `error` - Error types
//! - `cluster_config` - Cluster configuration management
//! - `pipeline` - Pipeline state management for feedback control
//! - `network` - Network communication
//! - `storage` - Storage implementations

// Core modules
pub mod cluster_config;
pub mod error;
pub mod event;
pub mod message;
pub mod multi_raft_driver;
pub mod network;
pub mod pipeline;
pub mod state;
pub mod storage;
pub mod tests;
pub mod traits;
pub mod types;

// Re-exports for convenience and backward compatibility
pub use event::{Event, Role};
pub use state::{RaftState, RaftStateOptions};
pub use types::{Command, GroupId, NodeId, RaftId, RequestId, TimerId};

// Re-export message types for backward compatibility
pub use message::{
    AppendEntriesRequest, AppendEntriesResponse, HardState, HardStateMap, InstallSnapshotRequest,
    InstallSnapshotResponse, InstallSnapshotState, LogEntry, RequestVoteRequest,
    RequestVoteResponse, Snapshot,
};

// Re-export cluster config types
pub use cluster_config::{ClusterConfig, JointConfig};

// Re-export error types
pub use error::{
    ApplyError, ClientError, RaftError, RpcError, SnapshotError, StorageError, TimerError,
};

// Re-export traits
pub use traits::{
    ApplyResult, ClusterConfigStorage, EventNotify, EventSender, HardStateStorage, LogEntryStorage,
    Network, RaftCallbacks, RpcResult, SnapshotResult, SnapshotStorage, StateMachine, Storage,
    StorageResult, TimerService,
};
