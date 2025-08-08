use raft_lite::{RaftId, RequestId};
use serde::{Deserialize, Serialize};
use tracing::info;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// --- 业务命令定义 ---
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

// --- 简单的内存 KV 存储 ---
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleKvStore {
    // 使用 RwLock 保护内部 HashMap
    // 在实际 RaftCallbacks 中，应用操作是串行的，所以读写锁的开销可以接受
    // 或者可以使用无锁结构，但这需要更仔细的设计
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

// --- 实现 RaftCallbacks ---
// TestStateMachine 将包含 KvStore 和与 Raft 交互所需的其他组件
pub struct TestStateMachine {
    id : RaftId,
    pub store: Arc<RwLock<SimpleKvStore>>,
}

impl TestStateMachine {
    pub fn new(id: RaftId) -> Self {
        Self {
            id,
            store: Arc::new(RwLock::new(SimpleKvStore::new())),
        }
    }

    // Required apply command handler
    pub async fn apply_command(
        &self,
        _from: RaftId,
        _index: u64,
        _term: u64,
        cmd: raft_lite::Command,
    ) -> raft_lite::ApplyResult<()> {
        info!(
            "node {:?} TestStateMachine apply_command called: index={}, term={}, cmd_len={}",
            self.id,
            _index,
            _term,
            cmd.len()
        );

        // Decode and execute KV command
        let kv_cmd = KvCommand::decode(&cmd).map_err(|e| {
            raft_lite::ApplyError::internal_err(format!("Failed to decode command: {}", e))
        })?;

        info!("node {:?} Applying command: {:?}", self.id, kv_cmd);

        match kv_cmd {
            KvCommand::Set { key, value } => {
                info!("node {:?} Setting key={}, value={}",self.id, key, value);
                self.store.write().unwrap().set(key.clone(), value.clone());
                //     println!("Current store state: {:?}", self.store.read().unwrap().data);
            }
            KvCommand::Get { key } => {
                assert!(false, "Get operation not passed to state machine");
            }
            KvCommand::Delete { key } => {
                self.store.write().unwrap().delete(&key);
            }
        }
        Ok(())
    }

    // create snapshot
    pub fn create_snapshot(
        &self,
        _from: RaftId,
        _index: u64,
        _term: u64,
    ) -> raft_lite::SnapshotResult<Vec<u8>> {
        let data = serde_json::to_vec(&self.store.read().unwrap().clone())
            .map_err(|e| raft_lite::SnapshotError::DataCorrupted(e.into()))?;
        Ok(data)
    }

    // Required snapshot processor
    pub fn install_snapshot(
        &self,
        _from: RaftId,
        _index: u64,
        _term: u64,
        data: Vec<u8>,
        _request_id: RequestId,
    ) -> raft_lite::SnapshotResult<()> {
        let store: SimpleKvStore = serde_json::from_slice(&data)
            .map_err(|e| raft_lite::SnapshotError::DataCorrupted(e.into()))?;
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
}
