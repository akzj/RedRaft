use std::{
    cell::Cell,
    collections::{HashMap, VecDeque},
    fs::File,
    io::Write,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use anyhow::{Result, anyhow};

use bincode::{Decode, Encode, config::LittleEndian};
use tokio::{
    io::AsyncReadExt,
    sync::{self, Semaphore, mpsc},
};
use tracing::warn;

use crate::{
    RaftId,
    cluster_config::ClusterConfig,
    error::StorageError,
    message::{HardState, HardStateMap, LogEntry, Snapshot},
    traits::{Storage, StorageResult},
};

#[derive(Debug, Default, Decode, Encode, Clone, PartialEq, Eq, Hash)]
pub struct EntryMeta {
    pub log_index: u64,
    pub term: u64,
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug, Clone, Default, Decode, Encode)]
pub struct Index {
    pub entries: HashMap<RaftId, Vec<EntryMeta>>,
    pub snapshots: HashMap<RaftId, EntryMeta>,
    pub cluster_configs: HashMap<RaftId, EntryMeta>,
}

#[derive(Debug, Clone, Decode, Encode)]
enum EntryType {
    LogEntry,
    Snapshot,
    ClusterConfig,
    TruncatePrefix,
    TruncateSuffix,
}

#[derive(Debug, Clone, Decode, Encode)]
pub struct EntryHeader {
    size: u32,
    entry_type: EntryType, //u32
    magic_num: u32,
    crc: u32,
}

const ENTRY_MAGIC_NUM: u32 = 0x_1234_5678;
const ENTRY_HEADER_SIZE: u32 = 16; // 4 + 4 + 4 + 4 = 16 bytes
const LOG_SEGMENT_VERSION_V1: u32 = 1;

pub struct SegmentTruncateLog {}

// LogSegment file format:
// |  EntryHeader[ logEntry |  clusterConfig | truncatePrefix | truncateSuffix ] ... | hard_states ...| index ...| tail | tail size,crc (u32,u32) | version (u32)|
// SnapshotSegment file format:
// |  EntryHeader[ snapshot ] ... |  index ...| tail | tail size,crc (u32,u32) | version (u32)|
// 快照存储目录结构（以本地磁盘为例）：
// /raft/snapshots/{raft_id}
// ├─ {timestamp}_10000_5000/  # 快照目录名：时间戳_lastIncludedIndex_lastIncludedTerm（便于定位和清理）
// │  ├─ snapshot.meta       # 元数据文件（小文件，~100B，快速读取）
// │  ├─ data_000.dat        # 业务数据分片1（按固定大小分片，如100MB/片）
// │  ├─ data_001.dat        # 业务数据分片2
// │  └─ checksum.sha256     # 分片校验文件（记录每个data文件的SHA256，避免分片损坏）
#[derive(Debug, Default, Decode, Encode)]
pub struct LogSegmentTail {
    hard_states_offset: u64,
    hard_states_size: u64,
    hard_states_crc: u32,

    index_offset: u64,
    index_size: u64,
    index_crc: u32,
}

pub struct LogStoreSnapshot {
    //snapshots :HashMap<>
}

pub struct LogSegment {
    file_name: PathBuf,
    entry_index: Index,
    file: Arc<File>,
    io_semaphore: Arc<Semaphore>,
    hard_states: RwLock<HardStateMap>,
}

impl EntryHeader {
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

impl LogSegment {
    pub fn first_entry(&self, from: &RaftId) -> Option<EntryMeta> {
        self.entry_index.entries.get(&from)?.first().cloned()
    }
    pub fn last_entry(&self, from: &RaftId) -> Option<EntryMeta> {
        self.entry_index.entries.get(&from)?.last().cloned()
    }
    pub fn entry_count(&self, from: &RaftId) -> usize {
        self.entry_index.entries.get(&from).map_or(0, |v| v.len())
    }

    pub fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub fn sync_data(&self) -> Result<()> {
        self.file.sync_data().map_err(|e| {
            warn!("Failed to sync log segment file data: {}", e);
            e.into()
        })
    }

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
        let buffs = entries
            .iter()
            .map(|e| e.serialize())
            .collect::<Result<Vec<Vec<u8>>>>()?;

        let mut offset = self.file.metadata()?.len();
        let headers = buffs
            .iter()
            .map(|buff| {
                EntryHeader {
                    size: buff.len() as u32 + ENTRY_HEADER_SIZE,
                    entry_type: EntryType::LogEntry,
                    magic_num: ENTRY_MAGIC_NUM,
                    crc: crc32fast::hash(buff),
                }
                .serialize()
            })
            .collect::<Result<Vec<Vec<u8>>>>()?;

        // update index
        for (index, buff) in buffs.iter().enumerate() {
            let entry = &entries[index];

            // entry header first
            let meta = EntryMeta {
                offset,
                term: entry.term,
                log_index: entry.index,
                size: buff.len() as u64,
            };

            self.entry_index
                .entries
                .entry(from.clone())
                .or_default()
                .push(meta);

            offset += buff.len() as u64;
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

    pub async fn read_entry(&self, from: &RaftId, log_index: u64) -> Result<LogEntry> {
        let entry_metas = match self.entry_index.entries.get(&from) {
            Some(entry_metas) => {
                if entry_metas.is_empty() {
                    return Err(anyhow!("No log entries found for raft ID {}", from));
                }
                entry_metas
            }
            None => return Err(anyhow!("No log entries found for raft ID {}", from)),
        };

        if log_index < entry_metas.first().unwrap().log_index
            || log_index > entry_metas.last().unwrap().log_index
        {
            return Err(anyhow!(
                "Log index {} out of range for raft ID {}",
                log_index,
                from
            ));
        }
        let entry_offset = log_index - entry_metas.first().unwrap().log_index;
        let meta = entry_metas.get(entry_offset as usize).unwrap().clone();

        let mut buf = vec![0u8; meta.size as usize];

        let file = self.file.clone();
        let permit = self.io_semaphore.clone().acquire_owned().await.unwrap(); // 等待许可

        Ok(tokio::task::spawn_blocking(move || {
            let _permit = permit; // 保持许可在作用域内
            file.read_exact_at(&mut buf, meta.offset).map_err(|e| {
                warn!("Failed to read log entry from file: {}", e);
                e
            })?;

            // decode header
            let header = EntryHeader::deserialize(&buf[..ENTRY_HEADER_SIZE as usize])?;

            // check crc32
            crc32fast::hash(&buf[ENTRY_HEADER_SIZE as usize..])
                .eq(&header.crc)
                .then_some(())
                .ok_or_else(|| {
                    warn!("CRC mismatch for log entry at index {}", log_index);
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "CRC mismatch")
                })?;

            Ok(
                match LogEntry::deserialize(&buf[ENTRY_HEADER_SIZE as usize..]) {
                    Ok((entry, size)) => entry,
                    Err(err) => {
                        warn!("Failed to deserialize log entry: {}", err);
                        return Err(err);
                    }
                },
            )
        })
        .await??)
    }

    async fn get_log_entries(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> Result<Option<Vec<LogEntry>>> {
        let entry_metas = match self.entry_index.entries.get(&from) {
            Some(entry_metas) => {
                if entry_metas.is_empty() {
                    return Ok(None);
                }
                entry_metas
            }
            None => return Ok(None),
        };

        let low = if low < entry_metas.first().unwrap().log_index {
            entry_metas.first().unwrap().log_index
        } else {
            low
        };

        // not include high
        let high = if high > entry_metas.last().unwrap().log_index {
            entry_metas.last().unwrap().log_index
        } else {
            high
        };

        let count = (high - low) as usize;
        let begin = (low - entry_metas.first().unwrap().log_index) as usize;
        let metas = &entry_metas[begin..(begin + count)];

        let mut entries_vec = Vec::with_capacity(metas.len());

        for meta in metas {
            if entries_vec.is_empty() {
                entries_vec.push(vec![meta.clone()]);
                continue;
            }

            // Check if the current meta can be merged with the last entry
            let last = entries_vec.last_mut().unwrap();

            if last.last().unwrap().offset + last.last().unwrap().size == meta.offset {
                last.push(meta.clone());
            } else {
                entries_vec.push(vec![meta.clone()]);
            }
        }

        // batch read entries
        let mut tasks = Vec::with_capacity(entries_vec.len());
        for batch in entries_vec {
            let permit = self.io_semaphore.clone().acquire_owned().await.unwrap(); // 等待许可
            let file = self.file.clone();

            tasks.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                // file offset of this batch
                let offset = batch.first().unwrap().offset;
                // total size of this batch
                let size = batch
                    .iter()
                    .map(|m| m.size + ENTRY_HEADER_SIZE as u64)
                    .sum::<u64>();
                let mut buf = vec![0u8; size as usize];

                file.read_exact_at(&mut buf, offset)?;

                let mut log_entries = Vec::new();
                let mut offset = 0;

                while offset < buf.len() {
                    // decode header
                    let header = EntryHeader::deserialize(&buf[offset..])?;
                    offset += ENTRY_HEADER_SIZE as usize;

                    // valid data
                    if !matches!(header.entry_type, EntryType::LogEntry) {
                        return Err(anyhow!(
                            "Unexpected entry type in log entries: {:?}",
                            header.entry_type
                        ));
                    }
                    if header.magic_num != ENTRY_MAGIC_NUM {
                        return Err(anyhow!("Invalid magic number in log entry"));
                    }

                    // decode entry
                    match LogEntry::deserialize(&buf[offset..]) {
                        Ok((entry, size)) => {
                            log_entries.push(entry);
                            offset += size;
                        }
                        Err(err) => {
                            warn!("Failed to deserialize log entry: {}", err);
                            return Err(anyhow!("Failed to deserialize log entry"));
                        }
                    };
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

pub struct LogEntryStoreInner {
    dir: PathBuf,
    cache_entries_size: usize,
    io_semaphore: Arc<Semaphore>,
    // read_only segments
    segments: RwLock<Vec<LogSegment>>,
    // current writeable segment
    current_segment: RwLock<LogSegment>,
    cache_table: RwLock<HashMap<RaftId, VecDeque<LogEntry>>>,
}

#[derive(Clone)]
pub struct LogEntryStoreOptions {
    memtable_memory_size: usize,
    batch_size: usize,
    cache_entries_size: usize,
    // max number of I/O threads for log segment file read
    max_io_threads: usize,
}

#[derive(Clone)]
pub struct LogEntryStore {
    options: LogEntryStoreOptions,
    inner: Arc<LogEntryStoreInner>,
    sender: Arc<mpsc::UnboundedSender<LogEntry>>,
}

impl LogEntryStoreInner {
    fn append_to_cache(&self, log_entries: Vec<(&RaftId, &Vec<LogEntry>)>) {
        // write to cache
        for (from, entry) in log_entries {
            let mut cache_table = self.cache_table.write().unwrap();
            let cache = cache_table.entry(from.clone()).or_default();
            for log_entry in entry {
                cache.push_back(log_entry.clone());
                if cache.len() > self.cache_entries_size {
                    cache.pop_front();
                }
            }
        }
        // write to log segment file
    }

    async fn append_to_segment(
        &self,
        log_entries: Vec<(&RaftId, &Vec<LogEntry>)>,
    ) -> StorageResult<()> {
        // write to log segment file
        let mut segment = self.current_segment.write().unwrap();
        for (from, entries) in log_entries {
            segment
                .write_log_entries(from, entries.clone())
                .map_err(|e| {
                    warn!("Failed to append log entries to segment: {}", e);
                    StorageError::Io(Arc::new(e))
                })?;
        }

        // flush data
        segment.sync_data().map_err(|e| {
            warn!("Failed to sync log segment data: {}", e);
            StorageError::Io(Arc::new(e))
        })?;

        Ok(())
    }

    async fn append_log_entries(
        &self,
        log_entries: Vec<(&RaftId, &Vec<LogEntry>)>,
    ) -> StorageResult<()> {
        // write to cache
        self.append_to_cache(log_entries.clone());

        // write to log segment file
        self.append_to_segment(log_entries.clone()).await?;

        Ok(())
    }
}

enum LogEntryOp {
    Append(Vec<LogEntry>),
    truncate_log_prefix(u64),
    truncate_log_suffix(u64),
}

pub struct LogEntryOpRequest {
    from: RaftId,
    log_entry_op: LogEntryOp,
    response_tx: sync::oneshot::Sender<StorageResult<()>>,
}

impl LogEntryStore {
    pub fn start(&self, mut receiver: mpsc::UnboundedReceiver<LogEntryOpRequest>) {
        let mut self_clone = self.clone();
        let batch_size = self.options.batch_size;

        tokio::spawn(async move {
            loop {
                let mut buf = Vec::with_capacity(batch_size);
                let size = receiver.recv_many(&mut buf, batch_size).await;
                if size == 0 {
                    warn!("Log entry receiver closed");
                    break;
                }

                let append_log_entries: Vec<_> = buf
                    .iter()
                    .filter_map(|req| match req.log_entry_op {
                        LogEntryOp::Append(ref entries) => Some((&req.from, entries)),
                        _ => None,
                    })
                    .collect();

                let result = self_clone
                    .inner
                    .append_log_entries(append_log_entries)
                    .await;

                for req in buf {
                    let _ = req.response_tx.send(result.clone());
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl Storage for LogEntryStore {
    async fn save_hard_state(&self, from: &RaftId, hard_state: HardState) -> StorageResult<()> {
        unimplemented!()
    }

    async fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<HardState>> {
        unimplemented!()
    }

    async fn append_log_entries(&self, from: &RaftId, entries: &[LogEntry]) -> StorageResult<()> {
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
