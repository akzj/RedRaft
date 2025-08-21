use std::{collections::HashMap, fs::File, path::PathBuf};

use serde::de;

use crate::{
    RaftId,
    cluster_config::ClusterConfig,
    message::{LogEntry, Snapshot},
    traits::{Storage, StorageResult},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexEntry {
    pub log_index: u64,
    pub term: u64,
    pub offset: u64,
    pub size: u64,
}

pub struct WalSegmentIndex {
    pub entries: HashMap<RaftId, Vec<IndexEntry>>,
}

pub struct WalSegment {
    pub file_name: PathBuf,
    pub index: WalSegmentIndex,
    pub file: File,
}

impl WalSegment {
    pub fn first_entry(&self, from: RaftId) -> Option<IndexEntry> {
        self.index.entries.get(&from)?.first().cloned()
    }
    pub fn last_entry(&self, from: RaftId) -> Option<IndexEntry> {
        self.index.entries.get(&from)?.last().cloned()
    }
}

pub struct LogStore {
    pub dir: PathBuf,
    pub segments: Vec<WalSegment>,
}

#[async_trait::async_trait]
impl Storage for LogStore {
    async fn save_hard_state(
        &self,
        from: RaftId,
        term: u64,
        voted_for: Option<RaftId>,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn load_hard_state(&self, from: RaftId) -> StorageResult<Option<(u64, Option<RaftId>)>> {
        unimplemented!()
    }

    async fn append_log_entries(&self, from: RaftId, entries: &[LogEntry]) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_log_entries(
        &self,
        from: RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>> {
        unimplemented!()
    }

    async fn get_log_entries_term(
        &self,
        from: RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        unimplemented!()
    }

    async fn truncate_log_suffix(&self, from: RaftId, idx: u64) -> StorageResult<()> {
        unimplemented!()
    }

    async fn truncate_log_prefix(&self, from: RaftId, idx: u64) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_last_log_index(&self, from: RaftId) -> StorageResult<(u64, u64)> {
        unimplemented!()
    }

    async fn get_log_term(&self, from: RaftId, idx: u64) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn save_snapshot(&self, from: RaftId, snap: Snapshot) -> StorageResult<()> {
        unimplemented!()
    }

    async fn load_snapshot(&self, from: RaftId) -> StorageResult<Option<Snapshot>> {
        unimplemented!()
    }

    async fn create_snapshot(&self, from: RaftId) -> StorageResult<(u64, u64)> {
        unimplemented!()
    }

    async fn save_cluster_config(&self, from: RaftId, conf: ClusterConfig) -> StorageResult<()> {
        unimplemented!()
    }

    async fn load_cluster_config(&self, from: RaftId) -> StorageResult<ClusterConfig> {
        unimplemented!()
    }
}
