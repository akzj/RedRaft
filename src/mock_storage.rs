use super::{ClusterConfig, Error, LogEntry, Snapshot};
use std::sync::RwLock;

pub trait Storage {
    /// 保存硬状态（任期和投票信息）
    fn save_hard_state(&self, term: u64, voted_for: Option<String>);

    /// 加载硬状态
    fn load_hard_state(&self) -> (u64, Option<String>);

    /// 追加日志条目
    fn append(&self, entries: &[LogEntry]) -> Result<(), Error>;

    /// 获取指定范围的日志条目 [low, high)
    fn entries(&self, low: u64, high: u64) -> Result<Vec<LogEntry>, Error>;

    /// 截断日志后缀（保留 <= idx 的条目）
    fn truncate_suffix(&self, idx: u64) -> Result<(), Error>;

    /// 截断日志前缀（保留 >= idx 的条目）
    fn truncate_prefix(&self, idx: u64) -> Result<(), Error>;

    /// 获取最后一条日志的索引
    fn last_index(&self) -> Result<u64, Error>;

    /// 获取指定索引对应的任期
    fn term(&self, idx: u64) -> Result<u64, Error>;

    /// 保存快照
    fn save_snapshot(&self, snap: Snapshot) -> Result<(), Error>;

    /// 加载快照
    fn load_snapshot(&self) -> Result<Snapshot, Error>;

    /// 保存集群配置
    fn save_cluster_config(&self, conf: ClusterConfig) -> Result<(), Error>;

    /// 加载集群配置
    fn load_cluster_config(&self) -> Result<ClusterConfig, Error>;
}

/// 内存存储实现（用于测试和单机场景）
pub struct MockStorage {
    hard_state: RwLock<(u64, Option<String>)>, // (当前任期, 投票对象)
    log: RwLock<Vec<LogEntry>>,                // 日志条目列表（按索引递增）
    snapshot: RwLock<Snapshot>,                // 最新快照
    config: RwLock<ClusterConfig>,             // 集群配置
}

impl MockStorage {
    pub fn new() -> Self {
        Self {
            hard_state: RwLock::new((0, None)),
            log: RwLock::new(Vec::new()),
            snapshot: RwLock::new(Snapshot {
                index: 0,
                term: 0,
                data: vec![],
                config: ClusterConfig::empty(),
            }),
            config: RwLock::new(ClusterConfig::empty()),
        }
    }

    /// 辅助方法：获取日志索引到向量下标的映射（优化查询）
    fn log_index_to_pos(&self, index: u64) -> Option<usize> {
        let log = self.log.read().unwrap();
        log.binary_search_by_key(&index, |e| e.index).ok()
    }
}

impl Storage for MockStorage {
    // 保存硬状态（任期和投票信息）
    fn save_hard_state(&self, term: u64, voted_for: Option<String>) {
        *self.hard_state.write().unwrap() = (term, voted_for);
    }

    // 加载硬状态
    fn load_hard_state(&self) -> (u64, Option<String>) {
        self.hard_state.read().unwrap().clone()
    }

    // 追加日志条目（自动处理冲突截断）
    fn append(&self, entries: &[LogEntry]) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut log = self.log.write().unwrap();
        let first_idx = entries[0].index;
        let last_idx = entries.last().unwrap().index;

        // 检查日志连续性：如果新日志的起始索引小于等于已有日志的最大索引，需要截断冲突部分
        let current_last_idx = log.last().map(|e| e.index).unwrap_or(0);
        if first_idx <= current_last_idx {
            // 找到截断位置（第一个大于等于first_idx的条目）
            let truncate_pos = log
                .iter()
                .position(|e| e.index >= first_idx)
                .unwrap_or(log.len());
            log.truncate(truncate_pos);
        }

        // 检查日志是否连续（避免中间出现空洞）
        let expected_prev_idx = first_idx.checked_sub(1).unwrap_or(0);
        let actual_prev_idx = log.last().map(|e| e.index).unwrap_or(0);
        if expected_prev_idx != actual_prev_idx {
            return Err(Error(format!(
                "日志不连续：期望前序索引 {}, 实际前序索引 {}",
                expected_prev_idx, actual_prev_idx
            )));
        }

        // 追加新日志
        log.extend_from_slice(entries);
        Ok(())
    }

    // 获取指定范围的日志条目 [low, high)
    fn entries(&self, low: u64, high: u64) -> Result<Vec<LogEntry>, Error> {
        if low >= high {
            return Ok(Vec::new());
        }

        let snapshot = self.snapshot.read().unwrap();
        // 检查请求范围是否被快照覆盖
        if high <= snapshot.index {
            return Err(Error(format!(
                "请求日志在快照范围内（快照索引: {}）",
                snapshot.index
            )));
        }

        // 调整起始索引为快照之后
        let adjusted_low = low.max(snapshot.index + 1);
        if adjusted_low >= high {
            return Ok(Vec::new());
        }

        let log = self.log.read().unwrap();
        // 查找起始位置（第一个 >= adjusted_low 的条目）
        let start_pos = log
            .iter()
            .position(|e| e.index >= adjusted_low)
            .ok_or_else(|| Error(format!("日志起始索引 {} 不存在", adjusted_low)))?;

        // 查找结束位置（第一个 >= high 的条目）
        let end_pos = log
            .iter()
            .position(|e| e.index >= high)
            .unwrap_or(log.len());

        Ok(log[start_pos..end_pos].to_vec())
    }

    // 截断日志后缀（保留 <= idx 的条目）
    fn truncate_suffix(&self, idx: u64) -> Result<(), Error> {
        let mut log = self.log.write().unwrap();
        // 找到第一个 >= idx 的位置并截断
        let pos = log.iter().position(|e| e.index >= idx).unwrap_or(log.len());
        log.truncate(pos);
        Ok(())
    }

    // 截断日志前缀（保留 >= idx 的条目）
    fn truncate_prefix(&self, idx: u64) -> Result<(), Error> {
        let mut log = self.log.write().unwrap();
        // 找到第一个 >= idx 的位置，保留从该位置开始的日志
        let pos = log.iter().position(|e| e.index >= idx).unwrap_or(log.len());
        *log = log[pos..].to_vec();
        Ok(())
    }

    // 获取最后一条日志的索引
    fn last_index(&self) -> Result<u64, Error> {
        let log_last_idx = self
            .log
            .read()
            .unwrap()
            .last()
            .map(|e| e.index)
            .unwrap_or(0);
        let snapshot_idx = self.snapshot.read().unwrap().index;
        Ok(log_last_idx.max(snapshot_idx))
    }

    // 获取指定索引对应的任期
    fn term(&self, idx: u64) -> Result<u64, Error> {
        if idx == 0 {
            return Ok(0); // 0 索引是虚拟起点，任期为 0
        }

        let snapshot = self.snapshot.read().unwrap();
        // 检查是否在快照范围内
        if idx <= snapshot.index {
            return if idx == snapshot.index {
                Ok(snapshot.term) // 快照最后一条的任期
            } else {
                Err(Error(format!(
                    "索引 {} 在快照范围内，但快照仅包含到索引 {}",
                    idx, snapshot.index
                )))
            };
        }

        // 在日志中查找
        let log = self.log.read().unwrap();
        match self.log_index_to_pos(idx) {
            Some(pos) => Ok(log[pos].term),
            None => Err(Error(format!("日志索引 {} 不存在", idx))),
        }
    }

    // 保存快照
    fn save_snapshot(&self, snap: Snapshot) -> Result<(), Error> {
        let mut snapshot = self.snapshot.write().unwrap();
        // 仅保存比当前更新的快照
        if snap.index > snapshot.index {
            *snapshot = snap.clone();
            // 截断被快照覆盖的日志前缀
            self.truncate_prefix(snap.index + 1)?;
        }
        Ok(())
    }

    // 加载快照
    fn load_snapshot(&self) -> Result<Snapshot, Error> {
        Ok(self.snapshot.read().unwrap().clone())
    }

    // 保存集群配置
    fn save_cluster_config(&self, conf: ClusterConfig) -> Result<(), Error> {
        *self.config.write().unwrap() = conf;
        Ok(())
    }

    // 加载集群配置
    fn load_cluster_config(&self) -> Result<ClusterConfig, Error> {
        Ok(self.config.read().unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterConfig, RequestId};

    #[test]
    fn test_append_and_term() {
        let storage = MockStorage::new();
        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                command: vec![1],
                is_config: false,
                client_request_id: Some(RequestId::new()),
            },
            LogEntry {
                index: 2,
                term: 1,
                command: vec![2],
                is_config: false,
                client_request_id: Some(RequestId::new()),
            },
        ];

        // 测试追加日志
        assert!(storage.append(&entries).is_ok());
        assert_eq!(storage.term(1).unwrap(), 1);
        assert_eq!(storage.term(2).unwrap(), 1);
        assert!(storage.term(3).is_err());

        // 测试覆盖日志
        let new_entries = vec![LogEntry {
            index: 2,
            term: 2,
            command: vec![3],
            is_config: false,
            client_request_id: Some(RequestId::new()),
        }];
        assert!(storage.append(&new_entries).is_ok());
        assert_eq!(storage.term(2).unwrap(), 2); // 验证覆盖成功
        assert!(storage.term(1).is_ok()); // 保留未覆盖的条目
    }

    #[test]
    fn test_snapshot() {
        let storage = MockStorage::new();
        // 写入日志
        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                command: vec![1],
                is_config: false,
                client_request_id: Some(RequestId::new()),
            },
            LogEntry {
                index: 2,
                term: 1,
                command: vec![2],
                is_config: false,
                client_request_id: Some(RequestId::new()),
            },
        ];
        storage.append(&entries).unwrap();

        // 创建快照
        let snap = Snapshot {
            index: 2,
            term: 1,
            data: vec![0x01],
            config: ClusterConfig::empty(),
        };
        storage.save_snapshot(snap).unwrap();

        // 验证快照后日志截断
        assert!(storage.entries(1, 2).is_err()); // 已被快照覆盖
        assert_eq!(storage.term(2).unwrap(), 1); // 快照包含的索引
        assert!(storage.term(1).is_err()); // 快照之前的索引不可访问

        // 追加新日志
        let new_entries = vec![LogEntry {
            index: 3,
            term: 2,
            command: vec![3],
            is_config: false,
            client_request_id: Some(RequestId::new()),
        }];
        storage.append(&new_entries).unwrap();
        assert_eq!(storage.last_index().unwrap(), 3);
    }

    #[test]
    fn test_truncate() {
        let storage = MockStorage::new();
        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                command: vec![],
                is_config: false,
                client_request_id: Some(RequestId::new()),
            },
            LogEntry {
                index: 2,
                term: 1,
                command: vec![],
                is_config: false,
                client_request_id: Some(RequestId::new()),
            },
            LogEntry {
                index: 3,
                term: 1,
                command: vec![],
                is_config: false,
                client_request_id: Some(RequestId::new()),
            },
        ];
        storage.append(&entries).unwrap();

        // 截断后缀
        storage.truncate_suffix(2).unwrap();
        assert_eq!(storage.last_index().unwrap(), 1); // 保留索引1

        // 截断前缀
        storage.append(&entries).unwrap(); // 重新追加所有日志
        storage.truncate_prefix(2).unwrap();
        assert!(storage.term(1).is_err()); // 前缀已被截断
        assert_eq!(storage.term(2).unwrap(), 1);
    }
}
