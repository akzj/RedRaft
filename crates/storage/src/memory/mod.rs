//! Memory storage implementation
//!
//! Provides in-memory storage for Redis data structures:
//! - List: Fast deque-based list operations
//! - Set: HashSet-based set operations
//! - ZSet: Skip list + HashMap for sorted sets
//! - Pub/Sub: Channel-based publish/subscribe

mod list;
mod set;
mod zset;
mod pubsub;
mod store;

pub use list::ListStore;
pub use set::SetStore;
pub use zset::ZSetStore;
pub use pubsub::PubSubStore;
pub use store::MemoryStore;

/// Redis value type for memory storage
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(Vec<u8>),
    List(list::ListData),
    Set(set::SetData),
    ZSet(zset::ZSetData),
}

impl RedisValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Set(_) => "set",
            RedisValue::ZSet(_) => "zset",
        }
    }
}

