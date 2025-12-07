//! 集群元数据定义
//!
//! 包含节点、分片、路由表等核心数据结构

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
