//! Cluster metadata definitions
//!
//! Contains core data structures such as nodes, shards, and routing tables

mod node;
mod shard;
mod routing;
mod cluster;

pub use node::{NodeId, NodeInfo, NodeStatus};
pub use shard::{
    KeyRange, ShardId, ShardInfo, ShardSplitState, ShardStatus,
    SplitProgress, SplitRole, SplitStatus, SplitTask, TOTAL_SLOTS,
};
pub use routing::{RoutingTable, SplitPhase, SplittingShardInfo};
pub use cluster::{ClusterMetadata, ClusterStats};
