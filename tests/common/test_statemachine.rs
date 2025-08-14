use raft_lite::{RaftId, RequestId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::debug;

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
    id: RaftId,
    pub store: Arc<RwLock<SimpleKvStore>>,
    // 跟踪已应用的日志索引和任期
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
        _from: RaftId,
        index: u64,
        term: u64,
        cmd: raft_lite::Command,
    ) -> raft_lite::traits::ApplyResult<()> {
        debug!(
            "node {:?} TestStateMachine apply_command called: index={}, term={}, cmd_len={}",
            self.id,
            index,
            term,
            cmd.len()
        );

        // Decode and execute KV command
        let kv_cmd = KvCommand::decode(&cmd).map_err(|e| {
            raft_lite::error::ApplyError::internal_err(format!("Failed to decode command: {}", e))
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

        // 更新已应用的索引和任期
        *self.last_applied_index.write().unwrap() = index;
        *self.last_applied_term.write().unwrap() = term;

        Ok(())
    }

    // create snapshot
    pub fn create_snapshot(
        &self,
        _from: RaftId,
    ) -> raft_lite::traits::SnapshotResult<(u64, u64, Vec<u8>)> {
        // 使用已应用的索引和任期创建快照
        let applied_index = *self.last_applied_index.read().unwrap();
        let applied_term = *self.last_applied_term.read().unwrap();

        let data = serde_json::to_vec(&self.store.read().unwrap().clone())
            .map_err(|e| raft_lite::error::SnapshotError::DataCorrupted(e.into()))?;

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
    ) -> raft_lite::traits::SnapshotResult<()> {
        let store: SimpleKvStore = serde_json::from_slice(&data)
            .map_err(|e| raft_lite::error::SnapshotError::DataCorrupted(e.into()))?;
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
