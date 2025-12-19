//! Pending request tracker
//!
//! Tracks pending client requests and their corresponding result channels.
//! Each shard has its own PendingRequests instance to avoid single hotspot.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use raft::RequestId;
use resp::Command;
use storage::traits::ApplyResult as StoreApplyResult;
use tokio::sync::oneshot;

/// Pending request tracker
#[derive(Clone)]
pub struct PendingRequests {
    /// request_id -> (command, result_sender)
    requests: Arc<Mutex<HashMap<u64, (Command, oneshot::Sender<StoreApplyResult>)>>>,
}

impl PendingRequests {
    pub fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a pending request
    pub fn register(
        &self,
        request_id: RequestId,
        cmd: Command,
    ) -> oneshot::Receiver<StoreApplyResult> {
        let (tx, rx) = oneshot::channel();
        self.requests.lock().insert(request_id.into(), (cmd, tx));
        rx
    }

    /// Complete request and send result
    pub fn complete(&self, request_id: RequestId, result: StoreApplyResult) {
        if let Some((_, tx)) = self.requests.lock().remove(&request_id.into()) {
            let _ = tx.send(result);
        }
    }

    /// Get pending command (for execution during apply)
    pub fn get_command(&self, request_id: RequestId) -> Option<Command> {
        self.requests
            .lock()
            .get(&request_id.into())
            .map(|(cmd, _)| cmd.clone())
    }

    /// Remove timed out request
    pub fn remove(&self, request_id: RequestId) {
        self.requests.lock().remove(&request_id.into());
    }
}

impl Default for PendingRequests {
    fn default() -> Self {
        Self::new()
    }
}

