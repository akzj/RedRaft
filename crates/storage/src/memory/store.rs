//! Unified memory store implementation
//!
//! Integrates all memory-based data structures:
//! - List, Set, ZSet, Pub/Sub

use super::{list::ListStore, pubsub::PubSubStore, set::SetStore, zset::ZSetStore};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Unified memory store
#[derive(Clone)]
pub struct MemoryStore {
    /// List storage
    lists: Arc<ListStore>,
    /// Set storage
    sets: Arc<SetStore>,
    /// ZSet storage
    zsets: Arc<ZSetStore>,
    /// Pub/Sub storage
    pubsub: Arc<PubSubStore>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            lists: Arc::new(ListStore::new()),
            sets: Arc::new(SetStore::new()),
            zsets: Arc::new(ZSetStore::new()),
            pubsub: Arc::new(PubSubStore::new()),
        }
    }

    /// Get list store
    pub fn lists(&self) -> &ListStore {
        &self.lists
    }

    /// Get set store
    pub fn sets(&self) -> &SetStore {
        &self.sets
    }

    /// Get zset store
    pub fn zsets(&self) -> &ZSetStore {
        &self.zsets
    }

    /// Get pubsub store
    pub fn pubsub(&self) -> &PubSubStore {
        &self.pubsub
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

