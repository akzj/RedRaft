//! 集群元数据定义
//!
//! 包含节点、分片、路由表等核心数据结构

mod node;
mod shard;
mod routing;
mod cluster;

pub use node::{NodeId, NodeInfo, NodeStatus};
pub use shard::{ShardId, ShardInfo, ShardStatus, KeyRange, TOTAL_SLOTS};
pub use routing::RoutingTable;
pub use cluster::{ClusterMetadata, ClusterStats};
