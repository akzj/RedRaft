use async_trait::async_trait;

use crate::{ClusterConfig, LogEntry, RaftId, Snapshot, Storage, StorageResult};
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, RwLock},
};

#[derive(Clone)]
pub struct SnapshotStorage {
    snapshots: Arc<RwLock<HashMap<RaftId, Snapshot>>>,
}

impl SnapshotStorage {
    pub fn new() -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn save_snapshot(&self, raft_id: RaftId, snapshot: Snapshot) {
        self.snapshots.write().unwrap().insert(raft_id, snapshot);
    }

    pub fn load_snapshot(&self, raft_id: RaftId) -> Option<Snapshot> {
        self.snapshots.read().unwrap().get(&raft_id).cloned()
    }
}

pub struct MockStorageInner {
    hard_state: RwLock<(u64, Option<RaftId>)>, // (当前任期, 投票对象)
    log: RwLock<Vec<LogEntry>>,                // 日志条目列表（按索引递增）
    snapshot: RwLock<Option<Snapshot>>,        // 最新快照
    config: RwLock<Option<ClusterConfig>>,     // 集群配置
    snapshot_storage: Option<SnapshotStorage>,
}

/// 内存存储实现（用于测试和单机场景）
#[derive(Clone)]
pub struct MockStorage {
    inner: Arc<MockStorageInner>,
}

impl MockStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MockStorageInner {
                hard_state: RwLock::new((0, None)),
                log: RwLock::new(Vec::new()),
                snapshot: RwLock::new(None),
                config: RwLock::new(None),
                snapshot_storage: None,
            }),
        }
    }

    pub fn new_with_snapshot_storage(snapshot_storage: SnapshotStorage) -> Self {
        let inner = MockStorageInner {
            hard_state: RwLock::new((0, None)),
            log: RwLock::new(Vec::new()),
            snapshot: RwLock::new(None),
            config: RwLock::new(None),
            snapshot_storage: Some(snapshot_storage),
        };
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl Deref for MockStorage {
    type Target = MockStorageInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl Storage for MockStorage {
    async fn get_log_entries_term(
        &self,
        _from: RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        let log = self.log.read().unwrap();
        let entries: Vec<(u64, u64)> = log
            .iter()
            .filter(|e| e.index >= low && e.index < high)
            .map(|e| (e.index, e.term))
            .collect();
        Ok(entries)
    }

    async fn save_hard_state(
        &self,
        _from: RaftId,
        term: u64,
        voted_for: Option<RaftId>,
    ) -> StorageResult<()> {
        let mut hs = self.hard_state.write().unwrap();
        *hs = (term, voted_for);
        Ok(())
    }

    async fn load_hard_state(&self, _from: RaftId) -> StorageResult<Option<(u64, Option<RaftId>)>> {
        let hs = self.hard_state.read().unwrap();
        // Return None if term is 0 (uninitialized), otherwise Some
        if hs.0 == 0 {
            Ok(None)
        } else {
            Ok(Some((hs.0, hs.1.clone())))
        }
    }

    async fn append_log_entries(&self, _from: RaftId, entries: &[LogEntry]) -> StorageResult<()> {
        let mut log = self.log.write().unwrap();
        log.extend_from_slice(entries);
        Ok(())
    }

    async fn get_log_entries(
        &self,
        _from: RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>> {
        let log = self.log.read().unwrap();
        let entries: Vec<LogEntry> = log
            .iter()
            .filter(|e| e.index >= low && e.index < high)
            .cloned()
            .collect();
        Ok(entries)
    }

    async fn truncate_log_suffix(&self, _from: RaftId, idx: u64) -> StorageResult<()> {
        let mut log = self.log.write().unwrap();
        if let Some(pos) = log.iter().position(|e| e.index == idx) {
            log.truncate(pos);
        }
        Ok(())
    }

    async fn truncate_log_prefix(&self, _from: RaftId, idx: u64) -> StorageResult<()> {
        let mut log = self.log.write().unwrap();
        let pos = log.iter().position(|e| e.index >= idx).unwrap_or(log.len());
        log.drain(0..pos);
        Ok(())
    }

    async fn save_snapshot(&self, from: RaftId, snap: Snapshot) -> StorageResult<()> {
        let mut snapshot = self.snapshot.write().unwrap();
        *snapshot = Some(snap.clone());
        if let Some(snapshot_storage) = &self.snapshot_storage {
            snapshot_storage.save_snapshot(from, snap);
        }
        Ok(())
    }

    async fn create_snapshot(&self, _from: RaftId) -> StorageResult<(u64, u64)> {
        let log = self.log.read().unwrap();
        if let Some(entry) = log.last() {
            Ok((entry.index, entry.term))
        } else {
            Ok((0, 0))
        }
    }

    async fn get_last_log_index(&self, _from: RaftId) -> StorageResult<(u64, u64)> {
        let log = self.log.read().unwrap();
        if let Some(entry) = log.last() {
            Ok((entry.index, entry.term))
        } else {
            Ok((0, 0))
        }
    }

    async fn get_log_term(&self, _from: RaftId, idx: u64) -> StorageResult<u64> {
        if idx == 0 {
            return Ok(0); // 索引 0 的任期总是 0
        }
        
        let log = self.log.read().unwrap();
        for entry in log.iter() {
            if entry.index == idx {
                return Ok(entry.term);
            }
        }
        
        // 如果日志条目不存在，返回错误而不是 0
        // 这样可以避免日志一致性检查的混淆
        Err(crate::StorageError::LogNotFound(idx))
    }

    async fn load_snapshot(&self, _from: RaftId) -> StorageResult<Option<Snapshot>> {
        let snapshot = self.snapshot.read().unwrap();
        Ok(snapshot.clone())
    }

    async fn save_cluster_config(&self, _from: RaftId, conf: ClusterConfig) -> StorageResult<()> {
        let mut config = self.config.write().unwrap();
        *config = Some(conf);
        Ok(())
    }

    async fn load_cluster_config(&self, _from: RaftId) -> StorageResult<ClusterConfig> {
        let config = self.config.read().unwrap();
        Ok(config.as_ref().unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RequestId;
    use std::collections::HashSet;

    fn create_test_raft_id(group: &str, node: &str) -> RaftId {
        RaftId::new(group.to_string(), node.to_string())
    }

    fn create_test_log_entry(index: u64, term: u64, command: &str) -> LogEntry {
        LogEntry {
            term,
            index,
            command: command.as_bytes().to_vec(),
            is_config: false,
            client_request_id: Some(RequestId::new()),
        }
    }

    fn create_test_config(voters: Vec<(&str, &str)>) -> ClusterConfig {
        let voter_set: HashSet<RaftId> = voters
            .into_iter()
            .map(|(group, node)| create_test_raft_id(group, node))
            .collect();
        ClusterConfig::simple(voter_set, 0)
    }

    fn create_test_snapshot(index: u64, term: u64, data: &str) -> Snapshot {
        Snapshot {
            index,
            term,
            data: data.as_bytes().to_vec(),
            config: create_test_config(vec![
                ("group1", "node1"),
                ("group1", "node2"),
                ("group1", "node3"),
            ]),
        }
    }

    #[tokio::test]
    async fn test_new_storage_initialization() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");

        // 验证初始状态
        let hard_state = storage.load_hard_state(node_id.clone()).await.unwrap();
        assert_eq!(hard_state, None);

        let (last_index, last_term) = storage.get_last_log_index(node_id.clone()).await.unwrap();
        assert_eq!(last_index, 0);
        assert_eq!(last_term, 0);

        let snapshot = storage.load_snapshot(node_id).await.unwrap();
        assert_eq!(snapshot, None);
    }

    #[tokio::test]
    async fn test_hard_state_operations() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");
        let candidate_id = create_test_raft_id("group1", "candidate1");

        // 测试保存和加载硬状态
        storage
            .save_hard_state(node_id.clone(), 5, Some(candidate_id.clone()))
            .await
            .unwrap();

        let loaded = storage.load_hard_state(node_id.clone()).await.unwrap();
        assert_eq!(loaded, Some((5, Some(candidate_id))));

        // 测试更新硬状态
        storage
            .save_hard_state(node_id.clone(), 10, None)
            .await
            .unwrap();

        let updated = storage.load_hard_state(node_id.clone()).await.unwrap();
        assert_eq!(updated, Some((10, None)));

        // 测试零任期返回None
        storage
            .save_hard_state(node_id.clone(), 0, None)
            .await
            .unwrap();
        let zero_term = storage.load_hard_state(node_id).await.unwrap();
        assert_eq!(zero_term, None);
    }

    #[tokio::test]
    async fn test_log_entry_operations() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");

        // 测试追加日志条目
        let entries = vec![
            create_test_log_entry(1, 1, "command1"),
            create_test_log_entry(2, 1, "command2"),
            create_test_log_entry(3, 2, "command3"),
        ];

        storage
            .append_log_entries(node_id.clone(), &entries)
            .await
            .unwrap();

        // 测试获取最后日志索引
        let (last_index, last_term) = storage.get_last_log_index(node_id.clone()).await.unwrap();
        assert_eq!(last_index, 3);
        assert_eq!(last_term, 2);

        // 测试获取日志条目范围
        let retrieved = storage
            .get_log_entries(node_id.clone(), 1, 3)
            .await
            .unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0].index, 1);
        assert_eq!(retrieved[1].index, 2);

        // 测试获取全部日志
        let all_entries = storage
            .get_log_entries(node_id.clone(), 1, 4)
            .await
            .unwrap();
        assert_eq!(all_entries.len(), 3);

        // 测试空范围
        let empty = storage.get_log_entries(node_id, 5, 10).await.unwrap();
        assert_eq!(empty.len(), 0);
    }

    #[tokio::test]
    async fn test_get_log_term() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");

        let entries = vec![
            create_test_log_entry(1, 1, "command1"),
            create_test_log_entry(2, 2, "command2"),
            create_test_log_entry(3, 2, "command3"),
        ];

        storage
            .append_log_entries(node_id.clone(), &entries)
            .await
            .unwrap();

        // 测试获取存在的日志任期
        assert_eq!(storage.get_log_term(node_id.clone(), 1).await.unwrap(), 1);
        assert_eq!(storage.get_log_term(node_id.clone(), 2).await.unwrap(), 2);
        assert_eq!(storage.get_log_term(node_id.clone(), 3).await.unwrap(), 2);

        // 测试获取不存在的日志任期 - 现在应该返回错误
        assert!(storage.get_log_term(node_id, 5).await.is_err());
    }

    #[tokio::test]
    async fn test_truncate_log_suffix() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");

        let entries = vec![
            create_test_log_entry(1, 1, "command1"),
            create_test_log_entry(2, 1, "command2"),
            create_test_log_entry(3, 2, "command3"),
            create_test_log_entry(4, 2, "command4"),
        ];

        storage
            .append_log_entries(node_id.clone(), &entries)
            .await
            .unwrap();

        // 截断索引3及之后的日志
        storage
            .truncate_log_suffix(node_id.clone(), 3)
            .await
            .unwrap();

        let remaining = storage
            .get_log_entries(node_id.clone(), 1, 10)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].index, 1);
        assert_eq!(remaining[1].index, 2);

        let (last_index, _) = storage.get_last_log_index(node_id.clone()).await.unwrap();
        assert_eq!(last_index, 2);

        // 测试截断不存在的索引
        storage
            .truncate_log_suffix(node_id.clone(), 10)
            .await
            .unwrap();
        let unchanged = storage.get_log_entries(node_id, 1, 10).await.unwrap();
        assert_eq!(unchanged.len(), 2);
    }

    #[tokio::test]
    async fn test_truncate_log_prefix() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");

        let entries = vec![
            create_test_log_entry(1, 1, "command1"),
            create_test_log_entry(2, 1, "command2"),
            create_test_log_entry(3, 2, "command3"),
            create_test_log_entry(4, 2, "command4"),
        ];

        storage
            .append_log_entries(node_id.clone(), &entries)
            .await
            .unwrap();

        // 截断索引3之前的日志
        storage
            .truncate_log_prefix(node_id.clone(), 3)
            .await
            .unwrap();

        let remaining = storage
            .get_log_entries(node_id.clone(), 1, 10)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].index, 3);
        assert_eq!(remaining[1].index, 4);

        // 测试截断所有日志
        storage
            .truncate_log_prefix(node_id.clone(), 5)
            .await
            .unwrap();
        let empty = storage
            .get_log_entries(node_id.clone(), 1, 10)
            .await
            .unwrap();
        assert_eq!(empty.len(), 0);

        let (last_index, last_term) = storage.get_last_log_index(node_id).await.unwrap();
        assert_eq!(last_index, 0);
        assert_eq!(last_term, 0);
    }

    #[tokio::test]
    async fn test_snapshot_operations() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");

        // 测试初始快照状态
        let initial = storage.load_snapshot(node_id.clone()).await.unwrap();
        assert_eq!(initial, None);

        // 测试保存和加载快照
        let snapshot = create_test_snapshot(10, 3, "snapshot_data");
        storage
            .save_snapshot(node_id.clone(), snapshot.clone())
            .await
            .unwrap();

        let loaded = storage.load_snapshot(node_id.clone()).await.unwrap();
        assert_eq!(loaded, Some(snapshot));

        // 测试覆盖快照
        let new_snapshot = create_test_snapshot(20, 5, "new_snapshot_data");
        storage
            .save_snapshot(node_id.clone(), new_snapshot.clone())
            .await
            .unwrap();

        let updated = storage.load_snapshot(node_id).await.unwrap();
        assert_eq!(updated, Some(new_snapshot));
    }

    #[tokio::test]
    async fn test_create_snapshot() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");

        // 测试空日志的快照创建
        let (index, term) = storage.create_snapshot(node_id.clone()).await.unwrap();
        assert_eq!(index, 0);
        assert_eq!(term, 0);

        // 添加日志后测试快照创建
        let entries = vec![
            create_test_log_entry(1, 1, "command1"),
            create_test_log_entry(2, 2, "command2"),
        ];
        storage
            .append_log_entries(node_id.clone(), &entries)
            .await
            .unwrap();

        let (index, term) = storage.create_snapshot(node_id).await.unwrap();
        assert_eq!(index, 2);
        assert_eq!(term, 2);
    }

    #[tokio::test]
    async fn test_cluster_config_operations() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");

        // 测试保存和加载集群配置
        let config = create_test_config(vec![
            ("group1", "node1"),
            ("group1", "node2"),
            ("group1", "node3"),
        ]);
        storage
            .save_cluster_config(node_id.clone(), config.clone())
            .await
            .unwrap();

        let loaded = storage.load_cluster_config(node_id.clone()).await.unwrap();
        assert_eq!(loaded, config);

        // 测试更新配置
        let new_config = create_test_config(vec![
            ("group1", "node1"),
            ("group1", "node2"),
            ("group1", "node3"),
            ("group1", "node4"),
        ]);
        storage
            .save_cluster_config(node_id.clone(), new_config.clone())
            .await
            .unwrap();

        let updated = storage.load_cluster_config(node_id).await.unwrap();
        assert_eq!(updated, new_config);
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        use std::sync::Arc;
        use tokio::task;

        let storage = Arc::new(MockStorage::new());
        let node_id = create_test_raft_id("group1", "node1");

        // 并发写入日志条目
        let mut handles = vec![];
        for i in 1..=10 {
            let storage_clone = storage.clone();
            let node_id_clone = node_id.clone();
            let handle = task::spawn(async move {
                let entry = create_test_log_entry(i, 1, &format!("command{}", i));
                storage_clone
                    .append_log_entries(node_id_clone, &[entry])
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        // 等待所有任务完成
        for handle in handles {
            handle.await.unwrap();
        }

        // 验证所有日志都被正确写入
        let all_entries = storage
            .get_log_entries(node_id.clone(), 1, 11)
            .await
            .unwrap();
        assert_eq!(all_entries.len(), 10);

        // 验证日志顺序（可能因并发而乱序，但所有条目都应该存在）
        let mut indices: Vec<u64> = all_entries.iter().map(|e| e.index).collect();
        indices.sort();
        let expected: Vec<u64> = (1..=10).collect();
        assert_eq!(indices, expected);
    }

    #[tokio::test]
    async fn test_edge_cases() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");

        // 测试边界范围查询
        let entries = vec![
            create_test_log_entry(1, 1, "command1"),
            create_test_log_entry(2, 1, "command2"),
            create_test_log_entry(3, 2, "command3"),
        ];
        storage
            .append_log_entries(node_id.clone(), &entries)
            .await
            .unwrap();

        // 测试相等的low和high
        let same_range = storage
            .get_log_entries(node_id.clone(), 2, 2)
            .await
            .unwrap();
        assert_eq!(same_range.len(), 0);

        // 测试超出范围的查询
        let out_of_range = storage
            .get_log_entries(node_id.clone(), 10, 20)
            .await
            .unwrap();
        assert_eq!(out_of_range.len(), 0);

        // 测试倒序范围（high < low）
        let reverse_range = storage
            .get_log_entries(node_id.clone(), 3, 1)
            .await
            .unwrap();
        assert_eq!(reverse_range.len(), 0);

        // 测试截断不存在的索引
        storage
            .truncate_log_suffix(node_id.clone(), 100)
            .await
            .unwrap();
        let unchanged = storage
            .get_log_entries(node_id.clone(), 1, 10)
            .await
            .unwrap();
        assert_eq!(unchanged.len(), 3);

        // 测试获取不存在索引的任期
        let missing_term = storage.get_log_term(node_id, 100).await.unwrap();
        assert_eq!(missing_term, 0);
    }

    #[tokio::test]
    async fn test_multiple_nodes() {
        let storage = MockStorage::new();
        let node1 = create_test_raft_id("group1", "node1");
        let node2 = create_test_raft_id("group1", "node2");
        let candidate1 = create_test_raft_id("group1", "candidate1");
        let candidate2 = create_test_raft_id("group1", "candidate2");

        // 测试不同节点的独立操作（虽然MockStorage是共享的，但接口设计为支持多节点）
        storage
            .save_hard_state(node1.clone(), 5, Some(candidate1))
            .await
            .unwrap();
        storage
            .save_hard_state(node2.clone(), 3, Some(candidate2.clone()))
            .await
            .unwrap();

        // 由于MockStorage是共享存储，两次保存会覆盖
        let state1 = storage.load_hard_state(node1).await.unwrap();
        let state2 = storage.load_hard_state(node2).await.unwrap();

        // 最后一次保存的状态会被保留
        assert_eq!(state1, state2);
        assert_eq!(state1, Some((3, Some(candidate2))));
    }

    #[tokio::test]
    async fn test_append_multiple_batches() {
        let storage = MockStorage::new();
        let node_id = create_test_raft_id("group1", "node1");

        // 分批追加日志
        let batch1 = vec![
            create_test_log_entry(1, 1, "command1"),
            create_test_log_entry(2, 1, "command2"),
        ];
        storage
            .append_log_entries(node_id.clone(), &batch1)
            .await
            .unwrap();

        let batch2 = vec![
            create_test_log_entry(3, 2, "command3"),
            create_test_log_entry(4, 2, "command4"),
        ];
        storage
            .append_log_entries(node_id.clone(), &batch2)
            .await
            .unwrap();

        // 验证所有日志都存在
        let all = storage
            .get_log_entries(node_id.clone(), 1, 5)
            .await
            .unwrap();
        assert_eq!(all.len(), 4);
        assert_eq!(all[0].index, 1);
        assert_eq!(all[3].index, 4);

        let (last_index, last_term) = storage.get_last_log_index(node_id).await.unwrap();
        assert_eq!(last_index, 4);
        assert_eq!(last_term, 2);
    }
}
