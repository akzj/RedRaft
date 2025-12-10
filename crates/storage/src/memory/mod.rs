//! Memory storage implementation
//!
//! Provides in-memory storage for Redis data structures:
//! - List: Fast deque-based list operations
//! - Set: HashSet-based set operations
//! - ZSet: Skip list + HashMap for sorted sets
//! - Pub/Sub: Channel-based publish/subscribe
//!
//! All structures support sharding for better performance and scalability

mod list;
mod pubsub;
mod set;
mod shard;
mod shard_data;
mod zset;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Re-export data structures
pub use list::ListData;
pub use set::{SetData, SetDataCow};
pub use zset::{OrderedFloat, ZSetData, ZSetDataCow};

pub use pubsub::PubSubStore;
pub use shard::{shard_for_key, shard_slot_range, slot_for_key, ShardId, TOTAL_SLOTS};
pub use shard_data::{LockedShardStore, ShardMetadata, ShardStore};
