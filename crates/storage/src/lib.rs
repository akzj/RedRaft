pub mod memory;
pub mod rocksdb;
pub mod shard;
pub mod snapshot;
pub mod store;
pub mod traits;

// Re-export commonly used types
pub use traits::{
    ApplyResult, RedisStore, SnapshotStore, SnapshotStoreEntry, StoreError, StoreResult,
};

// Type alias for backward compatibility (used in tests)
// In production, use HybridStore directly
#[cfg(test)]
pub type MemoryStore = store::HybridStore;
