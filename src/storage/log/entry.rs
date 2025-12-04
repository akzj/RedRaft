use std::collections::HashMap;

use anyhow::Result;
use bincode::{Decode, Encode};
use tracing::warn;

use crate::{RaftId, message::LogEntry};

#[derive(Debug, Default, Decode, Encode, Clone, PartialEq, Eq, Hash)]
pub struct EntryMeta {
    pub log_index: u64,
    pub term: u64,
    pub offset: u64,
    pub size: u64,
}

/// Represents a truncate operation record stored in the log segment
#[derive(Debug, Clone, Decode, Encode)]
pub struct TruncateRecord {
    pub raft_id: RaftId,
    pub truncate_index: u64,
}

impl TruncateRecord {
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let config = bincode::config::standard();
        Ok(bincode::encode_to_vec(self, config)?)
    }

    pub fn deserialize(data: &[u8]) -> Result<(Self, usize)> {
        let config = bincode::config::standard();
        Ok(bincode::decode_from_slice(data, config).map_err(|e| {
            warn!("Failed to deserialize truncate record: {}", e);
            e
        })?)
    }
}

/// Wrapper for log entry that includes raft_id for multi-raft storage
/// This is used when writing to the segment so we know which raft node
/// the entry belongs to during replay
#[derive(Debug, Clone, Decode, Encode)]
pub struct LogEntryRecord {
    pub raft_id: RaftId,
    pub entry: LogEntry,
}

impl LogEntryRecord {
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let config = bincode::config::standard();
        Ok(bincode::encode_to_vec(self, config)?)
    }

    pub fn deserialize(data: &[u8]) -> Result<(Self, usize)> {
        let config = bincode::config::standard();
        Ok(bincode::decode_from_slice(data, config).map_err(|e| {
            warn!("Failed to deserialize log entry record: {}", e);
            e
        })?)
    }
}

/// Per-raft node index information
#[derive(Debug, Clone, Default, Decode, Encode)]
pub struct RaftEntryIndex {
    /// First valid log index (updated by TruncatePrefix)
    pub first_log_index: u64,
    /// Last valid log index (updated by TruncateSuffix)  
    pub last_log_index: u64,
    /// Entry metadata list - entries[i] corresponds to log_index = first_log_index + i
    pub entries: Vec<EntryMeta>,
}

impl RaftEntryIndex {
    /// Check if a log index is valid (not truncated)
    pub fn is_valid_index(&self, log_index: u64) -> bool {
        log_index >= self.first_log_index && log_index <= self.last_log_index
    }

    /// Get entry meta for a given log index
    pub fn get_entry(&self, log_index: u64) -> Option<&EntryMeta> {
        if !self.is_valid_index(log_index) {
            return None;
        }
        let offset = (log_index - self.first_log_index) as usize;
        self.entries.get(offset)
    }

    /// Truncate entries before the given index (exclusive - keeps the index)
    pub fn truncate_prefix(&mut self, index: u64) {
        if index <= self.first_log_index {
            return;
        }
        if index > self.last_log_index + 1 {
            // Truncate everything
            self.entries.clear();
            self.first_log_index = index;
            self.last_log_index = index.saturating_sub(1);
            return;
        }
        let remove_count = (index - self.first_log_index) as usize;
        self.entries.drain(0..remove_count.min(self.entries.len()));
        self.first_log_index = index;
    }

    /// Truncate entries after the given index (exclusive - keeps the index)
    pub fn truncate_suffix(&mut self, index: u64) {
        if index >= self.last_log_index {
            return;
        }
        if index < self.first_log_index {
            // Truncate everything
            self.entries.clear();
            self.last_log_index = self.first_log_index.saturating_sub(1);
            return;
        }
        let keep_count = (index - self.first_log_index + 1) as usize;
        self.entries.truncate(keep_count);
        self.last_log_index = index;
    }
}

#[derive(Debug, Clone, Default, Decode, Encode)]
pub struct Index {
    pub entries: HashMap<RaftId, RaftEntryIndex>,
    #[allow(dead_code)]
    pub snapshots: HashMap<RaftId, EntryMeta>,
    #[allow(dead_code)]
    pub cluster_configs: HashMap<RaftId, EntryMeta>,
}

#[derive(Debug, Clone, Decode, Encode)]
pub enum EntryType {
    LogEntry,
    Snapshot,
    ClusterConfig,
    TruncatePrefix,
    TruncateSuffix,
}

pub const ENTRY_MAGIC_NUM: u32 = 0x_1234_5678;
pub const ENTRY_HEADER_SIZE: u32 = 16; // 4 + 4 + 4 + 4 = 16 bytes

#[derive(Debug, Clone, Decode, Encode)]
pub struct EntryHeader {
    pub size: u32,
    pub entry_type: EntryType,
    pub magic_num: u32,
    pub crc: u32,
}

impl EntryHeader {
    pub fn new(size: u32, entry_type: EntryType, crc: u32) -> Self {
        Self {
            size,
            entry_type,
            magic_num: ENTRY_MAGIC_NUM,
            crc,
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.size.to_le_bytes());

        buf.extend_from_slice(
            &{
                match self.entry_type {
                    EntryType::LogEntry => 1u32,
                    EntryType::Snapshot => 2u32,
                    EntryType::ClusterConfig => 3u32,
                    EntryType::TruncatePrefix => 4u32,
                    EntryType::TruncateSuffix => 5u32,
                }
            }
            .to_le_bytes(),
        );
        buf.extend_from_slice(&self.magic_num.to_le_bytes());
        buf.extend_from_slice(&self.crc.to_le_bytes());
        Ok(buf)
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        use anyhow::anyhow;
        
        // Check if the input data length is sufficient (u32 + u32 + u32 + u32 = 4 + 4 + 4 + 4 = 16 bytes)
        if data.len() < 16 {
            return Err(anyhow!("Invalid length"));
        }

        // Parse size (u32, 4 bytes)
        let mut size_bytes = [0u8; 4];
        size_bytes.copy_from_slice(&data[0..4]);
        let size = u32::from_le_bytes(size_bytes);

        // Parse entry_type (u32, 4 bytes)
        let mut type_bytes = [0u8; 4];
        type_bytes.copy_from_slice(&data[8..12]);
        let type_code = u32::from_le_bytes(type_bytes);
        let entry_type = match type_code {
            1 => EntryType::LogEntry,
            2 => EntryType::Snapshot,
            3 => EntryType::ClusterConfig,
            4 => EntryType::TruncatePrefix,
            5 => EntryType::TruncateSuffix,
            _ => return Err(anyhow!("Invalid entry type")),
        };

        // Parse magic_num (u32, 4 bytes)
        let mut magic_bytes = [0u8; 4];
        magic_bytes.copy_from_slice(&data[12..16]);
        let magic_num = u32::from_le_bytes(magic_bytes);

        // Parse crc (u32, 4 bytes)
        let mut crc_bytes = [0u8; 4];
        crc_bytes.copy_from_slice(&data[16..20]);
        let crc = u32::from_le_bytes(crc_bytes);

        if magic_num != ENTRY_MAGIC_NUM {
            return Err(anyhow!("Invalid magic number"));
        }

        Ok(Self {
            size,
            entry_type,
            magic_num,
            crc,
        })
    }
}

