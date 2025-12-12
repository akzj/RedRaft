//! Core routing and shard management
//!
//! Provides routing functionality for distributed storage:
//! - Key to shard_id mapping
//! - Shard_id to leader node_id mapping
//! - Raft group information management

pub mod routing;
pub mod shard;

// Re-export commonly used types
pub use routing::{RaftGroup, RaftRole, RoutingTable};
pub use shard::{ShardId, ShardRouting, TOTAL_SLOTS};

