use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use anyhow::{Result, anyhow};
use bincode::{Decode, Encode};
use tracing::warn;
use tokio::sync::Semaphore;

use crate::{RaftId, message::{HardStateMap, LogEntry}};

use super::entry::{
    EntryHeader, EntryMeta, EntryType, Index, LogEntryRecord, 
    TruncateRecord, ENTRY_HEADER_SIZE,
};

// 快照存储目录结构（以本地磁盘为例）：
// /raft/snapshots/{raft_id}
// ├─ {timestamp}_10000_5000/  # 快照目录名：时间戳_lastIncludedIndex_lastIncludedTerm（便于定位和清理）
// │  ├─ snapshot.meta       # 元数据文件（小文件，~100B，快速读取）
// │  ├─ data_000.dat        # 业务数据分片1（按固定大小分片，如100MB/片）
// │  ├─ data_001.dat        # 业务数据分片2
// │  └─ checksum.sha256     # 分片校验文件（记录每个data文件的SHA256，避免分片损坏）

#[allow(dead_code)]
pub struct SnapshotStore {
    snapshots: HashMap<RaftId, Vec<u8>>,
}

#[allow(dead_code)]
pub struct SegmentTruncateLog {}

// LogSegment file format:
// |  EntryHeader[ logEntry |  clusterConfig | truncatePrefix | truncateSuffix ] ... | hard_states ...| index ...| tail | tail size,crc (u32,u32) | version (u32)|

const LOG_SEGMENT_VERSION_V1: u32 = 1;

#[derive(Debug, Default, Decode, Encode)]
pub struct LogSegmentTail {
    hard_states_offset: u64,
    hard_states_size: u64,
    hard_states_crc: u32,

    index_offset: u64,
    index_size: u64,
    index_crc: u32,
}

#[allow(dead_code)]
pub struct LogStoreSnapshot {
    //snapshots :HashMap<>
}

pub struct LogSegment {
    #[allow(dead_code)]
    pub(crate) file_name: PathBuf,
    pub(crate) entry_index: Index,
    pub(crate) file: Arc<File>,
    pub(crate) io_semaphore: Arc<Semaphore>,
    pub(crate) hard_states: RwLock<HardStateMap>,
}

impl LogSegment {
    pub fn first_entry(&self, from: &RaftId) -> Option<EntryMeta> {
        self.entry_index.entries.get(&from)?.entries.first().cloned()
    }

    pub fn last_entry(&self, from: &RaftId) -> Option<EntryMeta> {
        self.entry_index.entries.get(&from)?.entries.last().cloned()
    }

    #[allow(dead_code)]
    pub fn entry_count(&self, from: &RaftId) -> usize {
        self.entry_index.entries.get(&from).map_or(0, |v| v.entries.len())
    }

    #[allow(dead_code)]
    pub fn first_log_index(&self, from: &RaftId) -> Option<u64> {
        self.entry_index.entries.get(from).map(|idx| idx.first_log_index)
    }

    #[allow(dead_code)]
    pub fn last_log_index(&self, from: &RaftId) -> Option<u64> {
        self.entry_index.entries.get(from).map(|idx| idx.last_log_index)
    }

    #[allow(dead_code)]
    pub fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub fn sync_data(&self) -> Result<()> {
        self.file.sync_data().map_err(|e| {
            warn!("Failed to sync log segment file data: {}", e);
            e.into()
        })
    }

    #[allow(dead_code)]
    pub fn complete_write(&mut self) -> Result<()> {
        let mut tail = LogSegmentTail::default();
        let hard_state = self.hard_states.read().unwrap();

        // write hard_state map
        let buff =
            bincode::encode_to_vec(&*hard_state, bincode::config::standard()).map_err(|e| {
                warn!("Failed to encode hard state: {}", e);
                e
            })?;

        tail.hard_states_offset = self.file.metadata()?.len();
        tail.hard_states_size = buff.len() as u64;
        tail.hard_states_crc = crc32fast::hash(&buff);

        self.file.write_all(&buff).map_err(|e| {
            warn!("Failed to write hard state to log segment file: {}", e);
            e
        })?;

        // write index
        let index_buff = bincode::encode_to_vec(&self.entry_index, bincode::config::standard())
            .map_err(|e| {
                warn!("Failed to encode index: {}", e);
                e
            })?;

        tail.index_offset = self.file.metadata()?.len();
        tail.index_size = index_buff.len() as u64;
        tail.index_crc = crc32fast::hash(&index_buff);

        self.file.write_all(&index_buff).map_err(|e| {
            warn!("Failed to write index to log segment file: {}", e);
            e
        })?;

        // write tail
        let tail_buff =
            bincode::encode_to_vec(&tail, bincode::config::standard()).map_err(|e| {
                warn!("Failed to encode tail: {}", e);
                e
            })?;

        let tail_size = tail_buff.len() as u32;
        let tail_crc = crc32fast::hash(&tail_buff);

        self.file.write_all(&tail_buff).map_err(|e| {
            warn!("Failed to write tail to log segment file: {}", e);
            e
        })?;

        // write tail meta
        self.file.write_all(&tail_size.to_le_bytes()).map_err(|e| {
            warn!("Failed to write tail size to log segment file: {}", e);
            e
        })?;

        self.file.write_all(&tail_crc.to_le_bytes()).map_err(|e| {
            warn!("Failed to write tail crc to log segment file: {}", e);
            e
        })?;

        // write version
        self.file
            .write_all(&LOG_SEGMENT_VERSION_V1.to_le_bytes())
            .map_err(|e| {
                warn!("Failed to write version to log segment file: {}", e);
                e
            })?;

        self.sync_data()
    }

    pub fn write_log_entries(&mut self, from: &RaftId, entries: Vec<LogEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Wrap entries with raft_id for multi-raft replay support
        let records: Vec<LogEntryRecord> = entries
            .iter()
            .map(|e| LogEntryRecord {
                raft_id: from.clone(),
                entry: e.clone(),
            })
            .collect();

        let buffs = records
            .iter()
            .map(|r| r.serialize())
            .collect::<Result<Vec<Vec<u8>>>>()?;

        let mut offset = self.file.metadata()?.len();
        let headers = buffs
            .iter()
            .map(|buff| {
                EntryHeader::new(
                    buff.len() as u32 + ENTRY_HEADER_SIZE,
                    EntryType::LogEntry,
                    crc32fast::hash(buff),
                )
                .serialize()
            })
            .collect::<Result<Vec<Vec<u8>>>>()?;

        // Get or create the raft entry index
        let raft_index = self.entry_index.entries.entry(from.clone()).or_default();

        // Initialize first_log_index if this is the first entry
        if raft_index.entries.is_empty() {
            raft_index.first_log_index = entries[0].index;
            raft_index.last_log_index = entries[0].index.saturating_sub(1);
        }

        // update index
        for (index, buff) in buffs.iter().enumerate() {
            let entry = &entries[index];

            // Include header size in offset calculation
            let meta = EntryMeta {
                offset: offset + ENTRY_HEADER_SIZE as u64,
                term: entry.term,
                log_index: entry.index,
                size: buff.len() as u64,
            };

            raft_index.entries.push(meta);
            raft_index.last_log_index = entry.index;

            offset += ENTRY_HEADER_SIZE as u64 + buff.len() as u64;
        }

        let buff = headers
            .iter()
            .zip(buffs.iter())
            .fold(Vec::new(), |mut acc, (header, buff)| {
                acc.extend_from_slice(header);
                acc.extend_from_slice(buff);
                acc
            });

        self.file.write_all(&buff).map_err(|e| {
            warn!("Failed to write log entry to file: {}", e);
            e
        })?;

        Ok(())
    }

    /// Write a truncate prefix operation to the log segment
    /// This marks all entries before `index` as invalid
    pub fn write_truncate_prefix(&mut self, from: &RaftId, index: u64) -> Result<()> {
        let record = TruncateRecord {
            raft_id: from.clone(),
            truncate_index: index,
        };

        let data = record.serialize()?;
        let header = EntryHeader::new(
            data.len() as u32 + ENTRY_HEADER_SIZE,
            EntryType::TruncatePrefix,
            crc32fast::hash(&data),
        );

        let header_bytes = header.serialize()?;
        self.file.write_all(&header_bytes)?;
        self.file.write_all(&data)?;

        // Update the in-memory index
        if let Some(raft_index) = self.entry_index.entries.get_mut(from) {
            raft_index.truncate_prefix(index);
        }

        Ok(())
    }

    /// Write a truncate suffix operation to the log segment
    /// This marks all entries after `index` as invalid
    pub fn write_truncate_suffix(&mut self, from: &RaftId, index: u64) -> Result<()> {
        let record = TruncateRecord {
            raft_id: from.clone(),
            truncate_index: index,
        };

        let data = record.serialize()?;
        let header = EntryHeader::new(
            data.len() as u32 + ENTRY_HEADER_SIZE,
            EntryType::TruncateSuffix,
            crc32fast::hash(&data),
        );

        let header_bytes = header.serialize()?;
        self.file.write_all(&header_bytes)?;
        self.file.write_all(&data)?;

        // Update the in-memory index
        if let Some(raft_index) = self.entry_index.entries.get_mut(from) {
            raft_index.truncate_suffix(index);
        }

        Ok(())
    }

    /// Replay the log segment to rebuild the index from scratch.
    /// This is called on startup to recover the state from the append-only log.
    /// 
    /// The replay process:
    /// 1. Read all entries sequentially from the segment file
    /// 2. For LogEntry: add to the index with raft_id from LogEntryRecord
    /// 3. For TruncatePrefix: update index to remove entries before the truncate index
    /// 4. For TruncateSuffix: update index to remove entries after the truncate index
    /// 
    /// This ensures that even though the segment is append-only, the index correctly
    /// reflects which entries are still valid after truncate operations.
    #[allow(dead_code)]
    pub fn replay_segment(&mut self) -> Result<()> {
        // Reset the index
        self.entry_index = Index::default();
        
        let file_size = self.file.metadata()?.len();
        if file_size == 0 {
            return Ok(());
        }
        
        let mut offset: u64 = 0;
        let mut header_buf = [0u8; ENTRY_HEADER_SIZE as usize];
        
        while offset < file_size {
            // Read the entry header
            if offset + ENTRY_HEADER_SIZE as u64 > file_size {
                warn!("Incomplete header at offset {}, stopping replay", offset);
                break;
            }
            
            self.file.read_exact_at(&mut header_buf, offset)?;
            
            let header = match EntryHeader::deserialize(&header_buf) {
                Ok(h) => h,
                Err(e) => {
                    warn!("Failed to deserialize header at offset {}: {}", offset, e);
                    break;
                }
            };
            
            let data_size = header.size as u64 - ENTRY_HEADER_SIZE as u64;
            let data_offset = offset + ENTRY_HEADER_SIZE as u64;
            
            // Read the entry data
            let mut data_buf = vec![0u8; data_size as usize];
            self.file.read_exact_at(&mut data_buf, data_offset)?;
            
            // Verify CRC
            let actual_crc = crc32fast::hash(&data_buf);
            if actual_crc != header.crc {
                warn!("CRC mismatch at offset {}: expected {}, got {}", offset, header.crc, actual_crc);
                break;
            }
            
            match header.entry_type {
                EntryType::LogEntry => {
                    // Deserialize the log entry record (contains raft_id + entry)
                    match LogEntryRecord::deserialize(&data_buf) {
                        Ok((record, _)) => {
                            let raft_index = self.entry_index.entries
                                .entry(record.raft_id.clone())
                                .or_default();
                            
                            // Initialize first_log_index if this is the first entry
                            if raft_index.entries.is_empty() {
                                raft_index.first_log_index = record.entry.index;
                                raft_index.last_log_index = record.entry.index.saturating_sub(1);
                            }
                            
                            // Add entry metadata
                            let meta = EntryMeta {
                                offset: data_offset,
                                term: record.entry.term,
                                log_index: record.entry.index,
                                size: data_size,
                            };
                            
                            raft_index.entries.push(meta);
                            raft_index.last_log_index = record.entry.index;
                        }
                        Err(e) => {
                            warn!("Failed to deserialize log entry record at offset {}: {}", offset, e);
                        }
                    }
                }
                EntryType::TruncatePrefix => {
                    match TruncateRecord::deserialize(&data_buf) {
                        Ok((record, _)) => {
                            if let Some(raft_index) = self.entry_index.entries.get_mut(&record.raft_id) {
                                raft_index.truncate_prefix(record.truncate_index);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to deserialize truncate prefix at offset {}: {}", offset, e);
                        }
                    }
                }
                EntryType::TruncateSuffix => {
                    match TruncateRecord::deserialize(&data_buf) {
                        Ok((record, _)) => {
                            if let Some(raft_index) = self.entry_index.entries.get_mut(&record.raft_id) {
                                raft_index.truncate_suffix(record.truncate_index);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to deserialize truncate suffix at offset {}: {}", offset, e);
                        }
                    }
                }
                EntryType::Snapshot | EntryType::ClusterConfig => {
                    // Handle other entry types if needed
                }
            }
            
            offset += header.size as u64;
        }
        
        Ok(())
    }

    pub async fn read_entry(&self, from: &RaftId, log_index: u64) -> Result<LogEntry> {
        let raft_index = match self.entry_index.entries.get(&from) {
            Some(raft_index) => {
                if raft_index.entries.is_empty() {
                    return Err(anyhow!("No log entries found for raft ID {}", from));
                }
                raft_index
            }
            None => return Err(anyhow!("No log entries found for raft ID {}", from)),
        };

        // Check if the index is within valid range
        if !raft_index.is_valid_index(log_index) {
            return Err(anyhow!(
                "Log index {} out of range for raft ID {} (valid range: {} - {})",
                log_index,
                from,
                raft_index.first_log_index,
                raft_index.last_log_index
            ));
        }

        let meta = raft_index.get_entry(log_index)
            .ok_or_else(|| anyhow!("Entry not found for index {}", log_index))?
            .clone();

        let mut buf = vec![0u8; meta.size as usize];

        let file = self.file.clone();
        let permit = self.io_semaphore.clone().acquire_owned().await.unwrap();

        Ok(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            file.read_exact_at(&mut buf, meta.offset).map_err(|e| {
                warn!("Failed to read log entry from file: {}", e);
                e
            })?;

            // Deserialize the log entry record (contains raft_id + entry)
            match LogEntryRecord::deserialize(&buf) {
                Ok((record, _size)) => Ok(record.entry),
                Err(err) => {
                    warn!("Failed to deserialize log entry record: {}", err);
                    Err(err)
                }
            }
        })
        .await??)
    }

    #[allow(dead_code)]
    pub async fn get_log_entries(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> Result<Option<Vec<LogEntry>>> {
        let raft_index = match self.entry_index.entries.get(&from) {
            Some(raft_index) => {
                if raft_index.entries.is_empty() {
                    return Ok(None);
                }
                raft_index
            }
            None => return Ok(None),
        };

        // Clamp low and high to valid range
        let low = low.max(raft_index.first_log_index);
        // high is exclusive, so we compare with last_log_index + 1
        let high = high.min(raft_index.last_log_index + 1);

        if low >= high {
            return Ok(Some(Vec::new()));
        }

        let count = (high - low) as usize;
        let begin = (low - raft_index.first_log_index) as usize;
        let metas = &raft_index.entries[begin..(begin + count)];

        let mut entries_vec = Vec::with_capacity(metas.len());

        for meta in metas {
            if entries_vec.is_empty() {
                entries_vec.push(vec![meta.clone()]);
                continue;
            }

            // Check if the current meta can be merged with the last entry (contiguous in file)
            let last = entries_vec.last_mut().unwrap();
            let last_meta = last.last().unwrap();

            // Check if entries are contiguous (accounting for header size)
            if last_meta.offset + last_meta.size == meta.offset {
                last.push(meta.clone());
            } else {
                entries_vec.push(vec![meta.clone()]);
            }
        }

        // batch read entries
        let mut tasks = Vec::with_capacity(entries_vec.len());
        for batch in entries_vec {
            let permit = self.io_semaphore.clone().acquire_owned().await.unwrap();
            let file = self.file.clone();

            tasks.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                // file offset of this batch (entry data starts after header, but we stored the data offset)
                let offset = batch.first().unwrap().offset;
                // total size of this batch (just the data, not headers)
                let size: u64 = batch.iter().map(|m| m.size).sum();
                let mut buf = vec![0u8; size as usize];

                file.read_exact_at(&mut buf, offset)?;

                let mut log_entries = Vec::new();
                let mut buf_offset = 0;

                for meta in &batch {
                    let entry_size = meta.size as usize;
                    let entry_data = &buf[buf_offset..buf_offset + entry_size];
                    
                    // Deserialize LogEntryRecord and extract the entry
                    match LogEntryRecord::deserialize(entry_data) {
                        Ok((record, _)) => {
                            log_entries.push(record.entry);
                        }
                        Err(err) => {
                            warn!("Failed to deserialize log entry record at index {}: {}", meta.log_index, err);
                            return Err(anyhow!("Failed to deserialize log entry record"));
                        }
                    }
                    buf_offset += entry_size;
                }
                Ok(log_entries)
            }));
        }

        let result = futures::future::join_all(tasks).await;

        let all_entries = result.into_iter().try_fold(
            Vec::new(),
            |mut acc, join_res| -> Result<Vec<LogEntry>> {
                let batch = join_res??;
                acc.extend(batch);
                Ok(acc)
            },
        )?;

        Ok(Some(all_entries))
    }
}

