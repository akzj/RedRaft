use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::{
    cluster_config::ClusterConfig,
    message::{HardState, LogEntry, Snapshot},
    traits::{Storage, StorageResult},
    RaftId,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntryMeta {
    pub log_index: u64,
    pub term: u64,
    pub offset: u64,
    pub size: u64,
}

pub struct EntryIndex {
    pub entries: HashMap<RaftId, Vec<EntryMeta>>,
}

pub struct SegmentTruncateLog {}

pub struct Segment {
    pub file_name: PathBuf,
    pub index: EntryIndex,
    pub file: File,
}

impl Segment {
    pub fn first_entry(&self, from: &RaftId) -> Option<EntryMeta> {
        self.index.entries.get(&from)?.first().cloned()
    }
    pub fn last_entry(&self, from: &RaftId) -> Option<EntryMeta> {
        self.index.entries.get(&from)?.last().cloned()
    }
}

pub struct LogEntryStoreInner {
    pub dir: PathBuf,
    pub segments: RwLock<Vec<Segment>>,
}

pub struct LogEntryStore {
    inner: Arc<LogEntryStoreInner>,
}

#[async_trait::async_trait]
impl Storage for LogEntryStore {
    async fn save_hard_state(&self, from: &RaftId, hard_state: HardState) -> StorageResult<()> {
        unimplemented!()
    }

    async fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<HardState>> {
        unimplemented!()
    }

    async fn append_log_entries(&self, from: &  RaftId, entries: &[LogEntry]) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_log_entries(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>> {
        unimplemented!()
    }

    async fn get_log_entries_term(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        unimplemented!()
    }

    async fn truncate_log_suffix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        unimplemented!()
    }

    async fn truncate_log_prefix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_last_log_index(&self, from: &RaftId) -> StorageResult<(u64, u64)> {
        unimplemented!()
    }

    async fn get_log_term(&self, from: &RaftId, idx: u64) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn save_snapshot(&self, from: &RaftId, snap: Snapshot) -> StorageResult<()> {
        unimplemented!()
    }

    async fn load_snapshot(&self, from: &RaftId) -> StorageResult<Option<Snapshot>> {
        unimplemented!()
    }

    async fn create_snapshot(&self, from: &RaftId) -> StorageResult<(u64, u64)> {
        unimplemented!()
    }

    async fn save_cluster_config(&self, from: &RaftId, conf: ClusterConfig) -> StorageResult<()> {
        unimplemented!()
    }

    async fn load_cluster_config(&self, from: &RaftId) -> StorageResult<ClusterConfig> {
        unimplemented!()
    }
}
