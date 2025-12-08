//! Routing table watch mechanism
//!
//! Supports clients watching routing table updates via version numbers

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Notify};
use tracing::debug;

/// Routing table watch manager
#[derive(Clone)]
pub struct RoutingTableWatcher {
    /// Watches for specific versions (version -> Notify)
    /// A single Notify can support multiple nodes waiting simultaneously
    watches: Arc<RwLock<HashMap<u64, Arc<Notify>>>>,
}

impl RoutingTableWatcher {
    /// Create a new watch manager
    pub fn new() -> Self {
        Self {
            watches: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a watch, waiting for version updates
    /// 
    /// # Arguments
    /// - `current_version`: Client's current version number
    /// 
    /// # Returns
    /// - `Notify`: Will be notified when routing table version updates
    /// 
    /// Note: Multiple nodes can share the same Notify for a version
    pub async fn watch(&self, current_version: u64) -> Arc<Notify> {
        let mut watches = self.watches.write().await;
        
        // If this version already has a Notify, return it (shared by multiple nodes)
        // Otherwise create a new Notify
        let notify = watches
            .entry(current_version)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone();
        
        debug!("Registered watch for routing table version {} (shared notify)", current_version);
        notify
    }

    /// Notify all watches waiting for the specified version
    /// 
    /// # Arguments
    /// - `version`: Updated version number
    pub async fn notify_version(&self, version: u64) {
        let mut watches = self.watches.write().await;
        
        // Notify all watches waiting for versions less than current
        let versions_to_notify: Vec<u64> = watches
            .keys()
            .filter(|&v| *v < version)
            .copied()
            .collect();
        
        for v in versions_to_notify {
            if let Some(notify) = watches.remove(&v) {
                debug!("Notifying watches for version {} (current: {})", v, version);
                // A single Notify can wake up all tasks waiting on it
                notify.notify_waiters();
            }
        }
    }

    /// Clean up expired watches (optional, for preventing memory leaks)
    pub async fn cleanup(&self) {
        // Can implement cleanup logic here, e.g., remove watches older than a certain time
        // Currently simple implementation, relies on automatic cleanup when clients disconnect
    }
}

impl Default for RoutingTableWatcher {
    fn default() -> Self {
        Self::new()
    }
}
