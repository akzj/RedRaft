//! Shard utilities for memory storage
//!
//! Re-exports shard types from rr-core for backward compatibility.

// Re-export from rr-core
pub use rr_core::routing::RoutingTable;
pub use rr_core::shard::{ShardId, ShardRouting, TOTAL_SLOTS};

/// Calculate slot for key using CRC16 algorithm (compatible with Redis Cluster)
///
/// This is a convenience function that delegates to RoutingTable::slot_for_key.
/// For new code, prefer using RoutingTable::slot_for_key directly.
pub fn slot_for_key(key: &[u8]) -> u32 {
    RoutingTable::slot_for_key(key)
}
