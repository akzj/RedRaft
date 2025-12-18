//! Redis Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{ApplyResult, RedisStore};
use resp::Command;

// HybridStore implements RedisStore trait (uses default apply method from trait)
impl RedisStore for HybridStore {
    /// Override apply_with_index to use WAL logging
    fn apply_with_index(&self, read_index: u64, apply_index: u64, cmd: &Command) -> ApplyResult {
        // Use the existing apply_with_index implementation
        HybridStore::apply_with_index(self, read_index, apply_index, cmd)
    }
}
