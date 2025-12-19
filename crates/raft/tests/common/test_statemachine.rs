use raft::{RaftId, RequestId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::debug;

// --- Business Command Definitions ---
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum KvCommand {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

impl KvCommand {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Failed to serialize KvCommand")
    }

    pub fn decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

// --- Simple In-Memory KV Store ---
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleKvStore {
    // Use RwLock to protect the internal HashMap
    // In actual RaftCallbacks, apply operations are serial, so the overhead of read-write locks is acceptable
    // Alternatively, a lock-free structure could be used, but this requires more careful design
    data: HashMap<String, String>,
}

impl SimpleKvStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).cloned()
    }

    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    pub fn delete(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }

    // Get all data for verification
    pub fn get_all_data(&self) -> HashMap<String, String> {
        self.data.clone()
    }
}

// --- Implement RaftCallbacks ---
// TestStateMachine will contain KvStore and other components needed for Raft interaction

#[derive(Clone)]
pub struct TestStateMachine {
    id: RaftId,
    pub store: Arc<RwLock<SimpleKvStore>>,
    // Track applied log index and term
    last_applied_index: Arc<RwLock<u64>>,
    last_applied_term: Arc<RwLock<u64>>,
}

impl TestStateMachine {
    pub fn new(id: RaftId) -> Self {
        Self {
            id,
            store: Arc::new(RwLock::new(SimpleKvStore::new())),
            last_applied_index: Arc::new(RwLock::new(0)),
            last_applied_term: Arc::new(RwLock::new(0)),
        }
    }

    // Required apply command handler
    pub async fn apply_command(
        &self,
        _from: &RaftId,
        index: u64,
        term: u64,
        cmd: raft::Command,
    ) -> raft::traits::ApplyResult<()> {
        debug!(
            "node {:?} TestStateMachine apply_command called: index={}, term={}, cmd_len={}",
            self.id,
            index,
            term,
            cmd.len()
        );

        // Decode and execute KV command
        let kv_cmd = KvCommand::decode(&cmd).map_err(|e| {
            raft::error::ApplyError::internal_err(format!("Failed to decode command: {}", e))
        })?;

        debug!("node {:?} Applying command: {:?}", self.id, kv_cmd);

        match kv_cmd {
            KvCommand::Set { key, value } => {
                debug!("node {:?} Setting key={}, value={}", self.id, key, value);
                self.store.write().unwrap().set(key.clone(), value.clone());
                //     println!("Current store state: {:?}", self.store.read().unwrap().data);
            }
            KvCommand::Get { key: _ } => {
                assert!(false, "Get operation not passed to state machine");
            }
            KvCommand::Delete { key } => {
                self.store.write().unwrap().delete(&key);
            }
        }

        // Update applied index and term
        *self.last_applied_index.write().unwrap() = index;
        *self.last_applied_term.write().unwrap() = term;

        Ok(())
    }

    // create snapshot
    pub fn create_snapshot(
        &self,
        _from: RaftId,
    ) -> raft::traits::SnapshotResult<(u64, u64, Vec<u8>)> {
        // Create snapshot using applied index and term
        let applied_index = *self.last_applied_index.read().unwrap();
        let applied_term = *self.last_applied_term.read().unwrap();

        let data = serde_json::to_vec(&self.store.read().unwrap().clone())
            .map_err(|e| raft::error::SnapshotError::DataCorrupted(Arc::new(e.into())))?;

        debug!(
            "node {:?} created snapshot at index={}, term={}, data_len={}",
            _from,
            applied_index,
            applied_term,
            data.len()
        );

        Ok((applied_index, applied_term, data))
    }

    // Required snapshot processor
    pub fn install_snapshot(
        &self,
        _from: RaftId,
        _index: u64,
        _term: u64,
        data: Vec<u8>,
        _request_id: RequestId,
    ) -> raft::traits::SnapshotResult<()> {
        let store: SimpleKvStore = serde_json::from_slice(&data)
            .map_err(|e| raft::error::SnapshotError::DataCorrupted(Arc::new(e.into())))?;
        self.store.write().unwrap().data = store.data;
        Ok(())
    }

    // Get all stored data for verification
    pub fn get_all_data(&self) -> HashMap<String, String> {
        self.store.read().unwrap().get_all_data()
    }

    // Get a specific key value for verification
    pub fn get_value(&self, key: &str) -> Option<String> {
        self.store.read().unwrap().get(key)
    }

    // Directly modify state machine (for testing purposes, bypassing Raft protocol)
    // This simulates direct state machine modification, e.g., during split operations
    pub fn direct_set(&self, key: String, value: String) {
        self.store.write().unwrap().set(key, value);
    }

    // Directly delete from state machine (for testing purposes)
    pub fn direct_delete(&self, key: &str) {
        self.store.write().unwrap().delete(key);
    }
}
