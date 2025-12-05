//! KV 状态机实现
//!
//! 将 Raft 日志应用到键值存储，使用 RedisStore trait 进行存储

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info, warn};

use raft::{ApplyResult, ClusterConfig, RaftId, SnapshotStorage, StateMachine, StorageResult};
use redisstore::{KVOperation, RedisStore};

/// KV 状态机
#[derive(Clone)]
pub struct KVStateMachine {
    /// 存储后端（支持内存或持久化存储）
    store: Arc<dyn RedisStore>,
    /// 版本号（单调递增）
    version: Arc<std::sync::atomic::AtomicU64>,
}

impl KVStateMachine {
    /// 创建新的 KV 状态机，使用指定的存储后端
    pub fn new(store: Arc<dyn RedisStore>) -> Self {
        Self {
            store,
            version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// 获取存储后端引用（用于读操作）
    pub fn store(&self) -> &Arc<dyn RedisStore> {
        &self.store
    }

    /// 获取键值对数量
    pub fn size(&self) -> usize {
        self.store.dbsize()
    }

    fn inc_version(&self) {
        self.version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl StateMachine for KVStateMachine {
    async fn apply_command(
        &self,
        _from: &RaftId,
        index: u64,
        term: u64,
        cmd: raft::Command,
    ) -> ApplyResult<()> {
        let op: KVOperation = match bincode::deserialize(&cmd) {
            Ok(op) => op,
            Err(e) => {
                warn!("Failed to deserialize command at index {}: {}", index, e);
                return Err(raft::ApplyError::Internal(format!(
                    "Invalid command: {}",
                    e
                )));
            }
        };

        debug!(
            "Applying command at index {}, term {}: {:?}",
            index, term, op
        );

        match op {
            // ==================== String 操作 ====================
            KVOperation::Set { key, value } => {
                self.store.set(key, value);
                self.inc_version();
            }
            KVOperation::SetNx { key, value } => {
                self.store.setnx(key, value);
                self.inc_version();
            }
            KVOperation::SetEx {
                key,
                value,
                ttl_secs,
            } => {
                self.store.setex(key, value, ttl_secs);
                self.inc_version();
            }
            KVOperation::MSet { kvs } => {
                self.store.mset(kvs);
                self.inc_version();
            }
            KVOperation::Incr { key } => {
                let _ = self.store.incr(&key);
                self.inc_version();
            }
            KVOperation::IncrBy { key, delta } => {
                let _ = self.store.incrby(&key, delta);
                self.inc_version();
            }
            KVOperation::Decr { key } => {
                let _ = self.store.decr(&key);
                self.inc_version();
            }
            KVOperation::DecrBy { key, delta } => {
                let _ = self.store.decrby(&key, delta);
                self.inc_version();
            }
            KVOperation::Append { key, value } => {
                self.store.append(&key, &value);
                self.inc_version();
            }
            KVOperation::GetSet { key, value } => {
                self.store.getset(key, value);
                self.inc_version();
            }

            // ==================== List 操作 ====================
            KVOperation::LPush { key, values } => {
                self.store.lpush(&key, values);
                self.inc_version();
            }
            KVOperation::RPush { key, values } => {
                self.store.rpush(&key, values);
                self.inc_version();
            }
            KVOperation::LPop { key } => {
                self.store.lpop(&key);
                self.inc_version();
            }
            KVOperation::RPop { key } => {
                self.store.rpop(&key);
                self.inc_version();
            }
            KVOperation::LSet { key, index, value } => {
                let _ = self.store.lset(&key, index, value);
                self.inc_version();
            }

            // ==================== Hash 操作 ====================
            KVOperation::HSet { key, field, value } => {
                self.store.hset(&key, field, value);
                self.inc_version();
            }
            KVOperation::HMSet { key, fvs } => {
                self.store.hmset(&key, fvs);
                self.inc_version();
            }
            KVOperation::HDel { key, fields } => {
                let fields_refs: Vec<&[u8]> = fields.iter().map(|f| f.as_slice()).collect();
                self.store.hdel(&key, &fields_refs);
                self.inc_version();
            }
            KVOperation::HIncrBy { key, field, delta } => {
                let _ = self.store.hincrby(&key, &field, delta);
                self.inc_version();
            }

            // ==================== Set 操作 ====================
            KVOperation::SAdd { key, members } => {
                self.store.sadd(&key, members);
                self.inc_version();
            }
            KVOperation::SRem { key, members } => {
                let members_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
                self.store.srem(&key, &members_refs);
                self.inc_version();
            }

            // ==================== 通用操作 ====================
            KVOperation::Del { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                self.store.del(&keys_refs);
                self.inc_version();
            }
            KVOperation::Expire { key, ttl_secs } => {
                self.store.expire(&key, ttl_secs);
                self.inc_version();
            }
            KVOperation::Persist { key } => {
                self.store.persist(&key);
                self.inc_version();
            }
            KVOperation::Rename { key, new_key } => {
                let _ = self.store.rename(&key, new_key);
                self.inc_version();
            }
            KVOperation::FlushDb => {
                self.store.flushdb();
                self.inc_version();
            }

            KVOperation::NoOp => {
                // 配置变更等操作，不需要返回值
            }
        }

        Ok(())
    }

    fn process_snapshot(
        &self,
        _from: &RaftId,
        _index: u64,
        _term: u64,
        data: Vec<u8>,
        _config: ClusterConfig,
        _request_id: raft::RequestId,
        oneshot: tokio::sync::oneshot::Sender<raft::SnapshotResult<()>>,
    ) {
        match self.store.restore_from_snapshot(&data) {
            Ok(()) => {
                let _ = oneshot.send(Ok(()));
            }
            Err(e) => {
                let _ = oneshot.send(Err(raft::SnapshotError::DataCorrupted(Arc::new(
                    anyhow::anyhow!(e),
                ))));
            }
        }
    }

    async fn create_snapshot(
        &self,
        from: &RaftId,
        config: ClusterConfig,
        saver: Arc<dyn SnapshotStorage>,
    ) -> StorageResult<(u64, u64)> {
        let snapshot_data = self.store.create_snapshot().map_err(|e| {
            raft::StorageError::SnapshotCreationFailed(format!("Failed to create snapshot: {}", e))
        })?;

        // 获取当前版本作为快照索引
        let version = self.version.load(std::sync::atomic::Ordering::SeqCst);
        let last_index = version; // 使用版本号作为索引

        // 创建快照
        let snapshot = raft::Snapshot {
            index: last_index,
            term: 0, // 快照不包含 term 信息
            data: snapshot_data,
            config,
        };

        saver.save_snapshot(from, snapshot).await?;

        info!(
            "Created snapshot for {} at index {}, {} keys",
            from,
            last_index,
            self.size()
        );

        Ok((last_index, 0))
    }

    async fn client_response(
        &self,
        _from: &RaftId,
        _request_id: raft::RequestId,
        _result: raft::ClientResult<u64>,
    ) -> raft::ClientResult<()> {
        Ok(())
    }

    async fn read_index_response(
        &self,
        _from: &RaftId,
        _request_id: raft::RequestId,
        _result: raft::ClientResult<u64>,
    ) -> raft::ClientResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redisstore::MemoryStore;

    #[tokio::test]
    async fn test_set_and_get() {
        let store = Arc::new(MemoryStore::new());
        let sm = KVStateMachine::new(store);
        let raft_id = RaftId::new("test".to_string(), "node1".to_string());

        let op = KVOperation::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };
        let command = bincode::serialize(&op).unwrap();

        let result = sm.apply_command(&raft_id, 1, 1, command).await;
        assert!(result.is_ok());

        assert_eq!(sm.store().get(b"key1"), Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_del() {
        let store = Arc::new(MemoryStore::new());
        let sm = KVStateMachine::new(store);
        let raft_id = RaftId::new("test".to_string(), "node1".to_string());

        // 先插入
        let op = KVOperation::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };
        let command = bincode::serialize(&op).unwrap();
        sm.apply_command(&raft_id, 1, 1, command).await.unwrap();

        // 删除
        let op = KVOperation::Del {
            keys: vec![b"key1".to_vec()],
        };
        let command = bincode::serialize(&op).unwrap();
        sm.apply_command(&raft_id, 2, 1, command).await.unwrap();

        assert_eq!(sm.store().get(b"key1"), None);
    }

    #[tokio::test]
    async fn test_list_operations() {
        let store = Arc::new(MemoryStore::new());
        let sm = KVStateMachine::new(store);
        let raft_id = RaftId::new("test".to_string(), "node1".to_string());

        let op = KVOperation::RPush {
            key: b"list".to_vec(),
            values: vec![b"a".to_vec(), b"b".to_vec()],
        };
        let command = bincode::serialize(&op).unwrap();
        sm.apply_command(&raft_id, 1, 1, command).await.unwrap();

        assert_eq!(
            sm.store().lrange(b"list", 0, -1),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
    }

    #[tokio::test]
    async fn test_hash_operations() {
        let store = Arc::new(MemoryStore::new());
        let sm = KVStateMachine::new(store);
        let raft_id = RaftId::new("test".to_string(), "node1".to_string());

        let op = KVOperation::HSet {
            key: b"hash".to_vec(),
            field: b"field1".to_vec(),
            value: b"value1".to_vec(),
        };
        let command = bincode::serialize(&op).unwrap();
        sm.apply_command(&raft_id, 1, 1, command).await.unwrap();

        assert_eq!(
            sm.store().hget(b"hash", b"field1"),
            Some(b"value1".to_vec())
        );
    }
}
