//! Pending request tracker
//!
//! Tracks pending client requests and their corresponding result channels.
//! Also manages apply result cache for matching Raft commit results with apply results.
//! Each shard has its own PendingRequests instance to avoid single hotspot.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use raft::RequestId;
use storage::traits::ApplyResult as StoreApplyResult;
use tokio::sync::oneshot;

/// Pending request tracker
#[derive(Clone)]
pub struct PendingRequests {
    /// request_id -> result_sender
    requests: Arc<Mutex<HashMap<u64, oneshot::Sender<StoreApplyResult>>>>,
    /// Apply result cache (queue of (index, result)), used to return actual results in client_response
    /// Uses queue because index is monotonically increasing, allowing easy cleanup of expired entries
    apply_results: Arc<Mutex<VecDeque<(u64, StoreApplyResult)>>>,
}

impl PendingRequests {
    pub fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(HashMap::new())),
            apply_results: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Register a pending request
    pub fn register(
        &self,
        request_id: RequestId,
    ) -> oneshot::Receiver<StoreApplyResult> {
        let (tx, rx) = oneshot::channel();
        self.requests.lock().insert(request_id.into(), tx);
        rx
    }

    /// Complete request and send result
    pub fn complete(&self, request_id: RequestId, result: StoreApplyResult) {
        if let Some(tx) = self.requests.lock().remove(&request_id.into()) {
            let _ = tx.send(result);
        }
    }

    /// Remove timed out request
    pub fn remove(&self, request_id: RequestId) {
        self.requests.lock().remove(&request_id.into());
    }

    /// Cache apply result for later matching in client_response
    /// Called when a command is applied to the state machine
    pub fn cache_apply_result(&self, index: u64, result: StoreApplyResult) {
        self.apply_results.lock().push_back((index, result));
    }

    /// Find and remove apply result for the given Raft index
    /// Also cleans up expired entries (index < current_apply_index)
    /// Returns the cached result, or StoreApplyResult::Ok if not found
    pub fn find_apply_result(&self, raft_index: u64, current_apply_index: u64) -> StoreApplyResult {
        let mut cache = self.apply_results.lock();

        // Process queue: clean up expired entries and find matching entry in one pass
        // Since index is monotonically increasing, we can process from front
        let mut found_result = None;
        while let Some((cached_index, cached_result)) = cache.front().cloned() {
            if cached_index < current_apply_index {
                // Expired entry, remove it
                cache.pop_front();
            } else if cached_index == raft_index {
                // Found matching entry, remove and return it
                found_result = Some(cached_result);
                cache.pop_front();
                break;
            } else {
                // cached_index >= current_apply_index && cached_index != raft_index
                // Since queue is ordered and index is monotonically increasing,
                // if we haven't found the match yet, it doesn't exist
                break;
            }
        }

        found_result.unwrap_or(StoreApplyResult::Ok)
    }

    /// Clear all cached apply results
    /// Called when snapshot is installed to clear stale cache
    pub fn clear_apply_results(&self) {
        self.apply_results.lock().clear();
    }
}

impl Default for PendingRequests {
    fn default() -> Self {
        Self::new()
    }
}
