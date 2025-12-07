//! KV 状态机实现
//!
//! 将 Raft 日志应用到键值存储，使用 RedisStore trait 进行存储

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info, warn};

use raft::{ApplyResult, ClusterConfig, RaftId, RequestId, SnapshotStorage, StateMachine, StorageResult, traits::ClientResult};
use redisstore::{ApplyResult as StoreApplyResult, RedisStore};
use resp::Command;

use crate::node::PendingRequests;

/// KV 状态机
#[derive(Clone)]
pub struct KVStateMachine {
    /// 存储后端（支持内存或持久化存储）
    store: Arc<dyn RedisStore>,
    /// 版本号（单调递增）
    version: Arc<std::sync::atomic::AtomicU64>,
    /// 等待中的请求追踪器
    pending_requests: Option<PendingRequests>,
    /// 应用结果缓存 (index -> result)，用于在 client_response 中返回真实结果
    apply_results: Arc<parking_lot::Mutex<std::collections::HashMap<u64, StoreApplyResult>>>,
}

impl KVStateMachine {
    /// 创建新的 KV 状态机，使用指定的存储后端
    pub fn new(store: Arc<dyn RedisStore>) -> Self {
        Self {
            store,
            version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            pending_requests: None,
            apply_results: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// 创建带有请求追踪的 KV 状态机
    pub fn with_pending_requests(store: Arc<dyn RedisStore>, pending_requests: PendingRequests) -> Self {
        Self {
            store,
            version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            pending_requests: Some(pending_requests),
            apply_results: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
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
        // 反序列化为 Command
        let command: Command = match bincode::serde::decode_from_slice(&cmd, bincode::config::standard()) {
            Ok((cmd, _)) => cmd,
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
            index, term, command
        );

        // 执行命令并保存结果
        let result = self.store.apply(&command);
        
        // 缓存结果，供 client_response 使用
        self.apply_results.lock().insert(index, result);
        self.inc_version();

        Ok(())
    }

    fn process_snapshot(
        &self,
        from: &RaftId,
        index: u64,
        term: u64,
        data: Vec<u8>,
        _config: ClusterConfig,
        request_id: RequestId,
        oneshot: tokio::sync::oneshot::Sender<raft::SnapshotResult<()>>,
    ) {
        let store = self.store.clone();
        let version = self.version.clone();
        let apply_results = self.apply_results.clone();
        let from = from.clone();

        // 使用 spawn_blocking 避免阻塞异步运行时
        tokio::task::spawn_blocking(move || {
            info!(
                "Installing snapshot for {} at index {}, term {}, request_id: {:?}, data_size: {} bytes",
                from, index, term, request_id, data.len()
            );

            match store.restore_from_snapshot(&data) {
                Ok(()) => {
                    // 更新版本号为快照索引
                    version.store(index, std::sync::atomic::Ordering::SeqCst);
                    
                    // 清理旧的 apply 结果缓存（快照之前的结果已经无意义）
                    apply_results.lock().clear();
                    
                    info!(
                        "Snapshot installed successfully for {} at index {}, {} keys restored",
                        from, index, store.dbsize()
                    );
                    
                    let _ = oneshot.send(Ok(()));
                }
                Err(e) => {
                    warn!(
                        "Failed to install snapshot for {} at index {}: {}",
                        from, index, e
                    );
                    let _ = oneshot.send(Err(raft::SnapshotError::DataCorrupted(Arc::new(
                        anyhow::anyhow!(e),
                    ))));
                }
            }
        });
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
        request_id: RequestId,
        result: ClientResult<u64>,
    ) -> ClientResult<()> {
        // 当 Raft 提交完成时通知等待的客户端
        if let Some(ref pending) = self.pending_requests {
            let store_result = match result {
                Ok(index) => {
                    // 从缓存中获取 apply 结果
                    self.apply_results
                        .lock()
                        .remove(&index)
                        .unwrap_or(StoreApplyResult::Ok)
                }
                Err(e) => {
                    StoreApplyResult::Error(redisstore::StoreError::Internal(format!("{:?}", e)))
                }
            };
            pending.complete(request_id, store_result);
        }
        Ok(())
    }

    async fn read_index_response(
        &self,
        _from: &RaftId,
        _request_id: raft::RequestId,
        _result: ClientResult<u64>,
    ) -> ClientResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redisstore::MemoryStore;
    use resp::Command;

    #[tokio::test]
    async fn test_set_and_get() {
        let store = Arc::new(MemoryStore::new());
        let sm = KVStateMachine::new(store);
        let raft_id = RaftId::new("test".to_string(), "node1".to_string());

        let cmd = Command::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();

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
        let cmd = Command::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        sm.apply_command(&raft_id, 1, 1, command).await.unwrap();

        // 删除
        let cmd = Command::Del {
            keys: vec![b"key1".to_vec()],
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        sm.apply_command(&raft_id, 2, 1, command).await.unwrap();

        assert_eq!(sm.store().get(b"key1"), None);
    }

    #[tokio::test]
    async fn test_list_operations() {
        let store = Arc::new(MemoryStore::new());
        let sm = KVStateMachine::new(store);
        let raft_id = RaftId::new("test".to_string(), "node1".to_string());

        let cmd = Command::RPush {
            key: b"list".to_vec(),
            values: vec![b"a".to_vec(), b"b".to_vec()],
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
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

        let cmd = Command::HSet {
            key: b"hash".to_vec(),
            fvs: vec![(b"field1".to_vec(), b"value1".to_vec())],
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        sm.apply_command(&raft_id, 1, 1, command).await.unwrap();

        assert_eq!(
            sm.store().hget(b"hash", b"field1"),
            Some(b"value1".to_vec())
        );
    }
}
