//! Memory storage implementation
//!
//! Provides in-memory storage for Redis data structures:
//! - List: Fast deque-based list operations
//! - Set: HashSet-based set operations
//! - ZSet: Skip list + HashMap for sorted sets
//! - Pub/Sub: Channel-based publish/subscribe
//!
//! All structures support sharding for better performance and scalability

mod pubsub;
mod redis_impl;
mod shard;
mod shard_data;
mod snapshot;
mod store;

// Forward declare RedisValue for use in shard_data
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

/// List data structure (VecDeque for O(1) head/tail operations)
pub type ListData = VecDeque<Vec<u8>>;

/// Set data structure (HashSet for O(1) operations)
pub type SetData = HashSet<Vec<u8>>;

/// ZSet data structure - serializable version for snapshots
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZSetData {
    /// member -> score mapping
    pub dict: HashMap<Vec<u8>, f64>,
    /// We don't serialize sorted BTreeMap directly, rebuild on deserialize
    #[serde(skip)]
    pub sorted: BTreeMap<(OrderedFloat<f64>, Vec<u8>), ()>,
}

impl ZSetData {
    pub fn new() -> Self {
        Self {
            dict: HashMap::new(),
            sorted: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.dict.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }

    /// Rebuild sorted index from dict (called after deserialization)
    pub fn rebuild_sorted_index(&mut self) {
        self.sorted.clear();
        for (member, score) in &self.dict {
            self.sorted
                .insert((OrderedFloat(*score), member.clone()), ());
        }
    }

    /// Create from dict, rebuilding sorted index
    pub fn from_dict(dict: HashMap<Vec<u8>, f64>) -> Self {
        let mut zset = Self {
            dict,
            sorted: BTreeMap::new(),
        };
        zset.rebuild_sorted_index();
        zset
    }
}

impl Default for ZSetData {
    fn default() -> Self {
        Self::new()
    }
}

/// Redis value type for memory storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedisValue {
    String(Vec<u8>),
    List(ListData),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(SetData),
    ZSet(ZSetData),
}

impl RedisValue {
    /// Get the type name of the value
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Hash(_) => "hash",
            RedisValue::Set(_) => "set",
            RedisValue::ZSet(_) => "zset",
        }
    }

    /// Get the size/length of the value
    pub fn len(&self) -> usize {
        match self {
            RedisValue::String(s) => s.len(),
            RedisValue::List(l) => l.len(),
            RedisValue::Hash(h) => h.len(),
            RedisValue::Set(s) => s.len(),
            RedisValue::ZSet(z) => z.len(),
        }
    }

    /// Check if the value is empty
    pub fn is_empty(&self) -> bool {
        match self {
            RedisValue::String(s) => s.is_empty(),
            RedisValue::List(l) => l.is_empty(),
            RedisValue::Hash(h) => h.is_empty(),
            RedisValue::Set(s) => s.is_empty(),
            RedisValue::ZSet(z) => z.is_empty(),
        }
    }

    /// Try to get as String value
    pub fn as_string(&self) -> Option<&Vec<u8>> {
        match self {
            RedisValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as mutable String value
    pub fn as_string_mut(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            RedisValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as List value
    pub fn as_list(&self) -> Option<&ListData> {
        match self {
            RedisValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Try to get as mutable List value
    pub fn as_list_mut(&mut self) -> Option<&mut ListData> {
        match self {
            RedisValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Try to get as Hash value
    pub fn as_hash(&self) -> Option<&HashMap<Vec<u8>, Vec<u8>>> {
        match self {
            RedisValue::Hash(h) => Some(h),
            _ => None,
        }
    }

    /// Try to get as mutable Hash value
    pub fn as_hash_mut(&mut self) -> Option<&mut HashMap<Vec<u8>, Vec<u8>>> {
        match self {
            RedisValue::Hash(h) => Some(h),
            _ => None,
        }
    }

    /// Try to get as Set value
    pub fn as_set(&self) -> Option<&SetData> {
        match self {
            RedisValue::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as mutable Set value
    pub fn as_set_mut(&mut self) -> Option<&mut SetData> {
        match self {
            RedisValue::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as ZSet value
    pub fn as_zset(&self) -> Option<&ZSetData> {
        match self {
            RedisValue::ZSet(z) => Some(z),
            _ => None,
        }
    }

    /// Try to get as mutable ZSet value
    pub fn as_zset_mut(&mut self) -> Option<&mut ZSetData> {
        match self {
            RedisValue::ZSet(z) => Some(z),
            _ => None,
        }
    }

    /// Check if the value is a String
    pub fn is_string(&self) -> bool {
        matches!(self, RedisValue::String(_))
    }

    /// Check if the value is a List
    pub fn is_list(&self) -> bool {
        matches!(self, RedisValue::List(_))
    }

    /// Check if the value is a Hash
    pub fn is_hash(&self) -> bool {
        matches!(self, RedisValue::Hash(_))
    }

    /// Check if the value is a Set
    pub fn is_set(&self) -> bool {
        matches!(self, RedisValue::Set(_))
    }

    /// Check if the value is a ZSet
    pub fn is_zset(&self) -> bool {
        matches!(self, RedisValue::ZSet(_))
    }

    /// Convert String value to Vec<u8>, returns None if not a String
    pub fn into_string(self) -> Option<Vec<u8>> {
        match self {
            RedisValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Convert List value, returns None if not a List
    pub fn into_list(self) -> Option<ListData> {
        match self {
            RedisValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Convert Hash value, returns None if not a Hash
    pub fn into_hash(self) -> Option<HashMap<Vec<u8>, Vec<u8>>> {
        match self {
            RedisValue::Hash(h) => Some(h),
            _ => None,
        }
    }

    /// Convert Set value, returns None if not a Set
    pub fn into_set(self) -> Option<SetData> {
        match self {
            RedisValue::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Convert ZSet value, returns None if not a ZSet
    pub fn into_zset(self) -> Option<ZSetData> {
        match self {
            RedisValue::ZSet(z) => Some(z),
            _ => None,
        }
    }

    /// Estimate memory size in bytes (approximate)
    pub fn estimated_size(&self) -> usize {
        match self {
            RedisValue::String(s) => s.len(),
            RedisValue::List(l) => {
                l.iter().map(|v| v.len()).sum::<usize>() + l.len() * std::mem::size_of::<Vec<u8>>()
            }
            RedisValue::Hash(h) => {
                h.iter()
                    .map(|(k, v)| k.len() + v.len() + std::mem::size_of::<Vec<u8>>() * 2)
                    .sum::<usize>()
            }
            RedisValue::Set(s) => {
                s.iter().map(|v| v.len()).sum::<usize>() + s.len() * std::mem::size_of::<Vec<u8>>()
            }
            RedisValue::ZSet(z) => {
                // dict: HashMap overhead + key/value sizes
                let dict_size = z.dict.iter().map(|(k, _)| k.len() + 8).sum::<usize>();
                // sorted: BTreeMap overhead + key/value sizes
                let sorted_size = z
                    .sorted
                    .iter()
                    .map(|((_, k), _)| k.len() + 8)
                    .sum::<usize>();
                dict_size + sorted_size
            }
        }
    }
}

pub use pubsub::PubSubStore;
pub use shard::{shard_for_key, shard_slot_range, slot_for_key, ShardId, TOTAL_SLOTS};
pub use shard_data::{LockedShardData, ShardData, ShardMetadata};
pub use store::MemoryStore;
