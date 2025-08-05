use async_trait::async_trait;

use crate::{RaftId, StorageResult};

use super::{ClusterConfig, LogEntry, Snapshot};
use std::sync::RwLock;

#[async_trait]
pub trait Storage {
    async fn save_hard_state(
        &self,
        from: RaftId,
        term: u64,
        voted_for: Option<RaftId>,
    ) -> StorageResult<()>;

    async fn load_hard_state(&self, from: RaftId) -> StorageResult<(u64, Option<RaftId>)>;

    async fn append_log_entries(&self, from: RaftId, entries: &[LogEntry]) -> StorageResult<()>;

    async fn get_log_entries(
        &self,
        from: RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>>;

    async fn truncate_log_suffix(&self, from: RaftId, idx: u64) -> StorageResult<()>;

    async fn truncate_log_prefix(&self, from: RaftId, idx: u64) -> StorageResult<()>;

    async fn get_last_log_index(&self, from: RaftId) -> StorageResult<u64>;

    async fn get_log_term(&self, from: RaftId, idx: u64) -> StorageResult<u64>;

    async fn save_snapshot(&self, from: RaftId, snap: Snapshot) -> StorageResult<()>;

    async fn load_snapshot(&self, from: RaftId) -> StorageResult<Snapshot>;

    async fn save_cluster_config(&self, from: RaftId, conf: ClusterConfig) -> StorageResult<()>;

    async fn load_cluster_config(&self, from: RaftId) -> StorageResult<ClusterConfig>;
}

/// 内存存储实现（用于测试和单机场景）
pub struct MockStorage {
    hard_state: RwLock<(u64, Option<RaftId>)>, // (当前任期, 投票对象)
    log: RwLock<Vec<LogEntry>>,                // 日志条目列表（按索引递增）
    snapshot: RwLock<Option<Snapshot>>,        // 最新快照
    config: RwLock<Option<ClusterConfig>>,     // 集群配置
}

impl MockStorage {
    pub fn new() -> Self {
        Self {
            hard_state: RwLock::new((0, None)),
            log: RwLock::new(Vec::new()),
            snapshot: RwLock::new(None),
            config: RwLock::new(None),
        }
    }

    /// 辅助方法：获取日志索引到向量下标的映射（优化查询）
    fn log_index_to_pos(&self, index: u64) -> Option<usize> {
        let log = self.log.read().unwrap();
        log.binary_search_by_key(&index, |e| e.index).ok()
    }
}

#[async_trait]
#[async_trait]
impl Storage for MockStorage {
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

    async fn load_hard_state(&self, _from: RaftId) -> StorageResult<(u64, Option<RaftId>)> {
        let hs = self.hard_state.read().unwrap();
        Ok((hs.0, hs.1.clone().map(|id| id)))
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

    async fn get_last_log_index(&self, _from: RaftId) -> StorageResult<u64> {
        let log = self.log.read().unwrap();
        Ok(log.last().map_or(0, |e| e.index))
    }

    async fn get_log_term(&self, _from: RaftId, idx: u64) -> StorageResult<u64> {
        let log = self.log.read().unwrap();
        for entry in log.iter() {
            if entry.index == idx {
                return Ok(entry.term);
            }
        }
        Ok(0)
    }

    async fn save_snapshot(&self, _from: RaftId, snap: Snapshot) -> StorageResult<()> {
        let mut snapshot = self.snapshot.write().unwrap();
        *snapshot = Some(snap);
        Ok(())
    }

    async fn load_snapshot(&self, _from: RaftId) -> StorageResult<Snapshot> {
        let snapshot = self.snapshot.read().unwrap();
        Ok(snapshot.as_ref().unwrap().clone())
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
