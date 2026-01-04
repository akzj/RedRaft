//! Segment implementation (new version without COW)
//!
//! Segment = Memory store serialized into chunks
//! - One directory per round: segments/{round}/
//! - File naming: {slot_start:05d}-{slot_end:05d}.seg (first chunk)
//!                {slot_start:05d}-{slot_end:05d}-{chunk_id:05d}.seg (subsequent chunks)
//! - Metadata: {slot_start:05d}-{slot_end:05d}.seg.json

use crate::memory::{Data, MemStore};
use crate::snapshot::{chunk::ChunkWriter, SnapshotConfig};
use crate::store::{LockedSlotStore, SlotMetadata};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::info;

/// Segment Metadata (JSON file)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    /// Slot range start (inclusive)
    pub slot_start: u32,
    /// Slot range end (exclusive)
    pub slot_end: u32,
    /// Apply index for this segment (max apply_index of all slots in range)
    pub apply_index: u64,
    /// Round number (轮次)
    pub round: u64,
    /// Chunk files information
    pub chunks: Vec<ChunkFileInfo>,
    /// Total uncompressed size
    pub total_uncompressed_size: u64,
    /// Total compressed size
    pub total_compressed_size: u64,
    /// Created timestamp
    pub created_at: u64,
    /// Slot -> apply_index mapping (记录每个 slot 的 apply_index)
    pub slot_apply_indices: HashMap<u32, u64>,
}

/// Chunk file information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkFileInfo {
    /// Chunk ID (1, 2, 3, ...)
    pub chunk_id: u32,
    /// File name (e.g., "00000-00010-00002.seg")
    pub file_name: String,
    /// Uncompressed size
    pub uncompressed_size: u64,
    /// Compressed size
    pub compressed_size: u64,
    /// Entry count
    pub entry_count: u32,
    /// CRC32 checksum
    pub crc32: u32,
}

/// Segment Generator
pub struct SegmentGenerator {
    config: SnapshotConfig,
    segments_dir: PathBuf,
    current_round: u64,
    is_generating: bool,
    last_generate_time: Option<std::time::Instant>,
}

impl SegmentGenerator {
    pub fn new(config: SnapshotConfig, segments_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&segments_dir).expect("Failed to create segments directory");

        // Load current round from file
        let current_round = Self::load_current_round(&segments_dir);

        Self {
            config,
            segments_dir,
            current_round,
            is_generating: false,
            last_generate_time: None,
        }
    }

    /// Load current round from file
    fn load_current_round(segments_dir: &Path) -> u64 {
        let round_file = segments_dir.join("current_round.txt");
        if let Ok(content) = std::fs::read_to_string(&round_file) {
            content.trim().parse().unwrap_or(1)
        } else {
            1
        }
    }

    /// Check if segment generation should be triggered
    pub fn should_generate(&self, wal_size: u64) -> bool {
        // Check if already generating
        if self.is_generating {
            return false;
        }

        // Check WAL size threshold
        if wal_size >= self.config.wal_size_threshold {
            return true;
        }

        // Check time interval
        if let Some(last_time) = self.last_generate_time {
            if last_time.elapsed().as_secs() >= self.config.segment_interval_secs {
                return true;
            }
        } else {
            // First time, generate if WAL is not empty
            return wal_size > 0;
        }

        false
    }

    /// Generate segments for log compression
    /// 按 slot 顺序遍历，写入后检查大小，超过 64MB 则下一个 slot 创建新 segment
    pub fn generate_segments(
        &mut self,
        slots: &HashMap<u32, LockedSlotStore>,
    ) -> Result<Vec<SegmentMetadata>, String> {
        // Mark as generating
        self.is_generating = true;

        // 1. 获取所有 slot 编号并排序
        let mut slot_list: Vec<u32> = slots.keys().copied().collect();
        slot_list.sort();

        if slot_list.is_empty() {
            self.is_generating = false;
            return Ok(Vec::new());
        }

        let round_dir = self.segments_dir.join(format!("{}", self.current_round));
        std::fs::create_dir_all(&round_dir)
            .map_err(|e| format!("Failed to create round directory: {}", e))?;

        let mut segments = Vec::new();
        let mut current_segment_start = slot_list[0];
        let mut current_chunk_writer = ChunkWriter::new(self.config.clone(), round_dir.clone());
        let mut current_chunks = Vec::new();
        let mut current_slot_apply_indices = HashMap::new();
        let mut current_max_apply_index = 0u64;

        // 2. 按 slot 顺序遍历
        for &slot in &slot_list {
            // 获取 slot 数据
            let (memory_clone, metadata) = {
                if let Some(slot_store) = slots.get(&slot) {
                    let guard = slot_store.read();
                    (guard.memory().clone(), guard.metadata().clone())
                } else {
                    // Slot 不存在，使用空数据
                    (MemStore::new(), SlotMetadata::new(slot))
                }
            };

            // 记录该 slot 的 apply_index
            current_slot_apply_indices.insert(slot, metadata.last_applied_index);
            current_max_apply_index = current_max_apply_index.max(metadata.last_applied_index);

            // 添加该 slot 的数据到当前 segment
            for (key, data) in memory_clone.iter() {
                current_chunk_writer.add_entry(key.clone(), data)?;

                // 如果当前 chunk 已满，flush
                if current_chunk_writer.should_flush() {
                    let chunk_info = self.flush_chunk(
                        &mut current_chunk_writer,
                        current_segment_start,
                        slot + 1, // 临时 slot_end，实际会在 finish_segment 时确定
                        &round_dir,
                    )?;
                    current_chunks.push(chunk_info);
                }
            }

            // 3. 写入后检查当前 segment 大小
            let current_segment_size = self.calculate_segment_size(&current_chunks)?;

            // 4. 如果超过 64MB，且当前 segment 已有数据，则关闭当前 segment
            if current_segment_size > self.config.chunk_size as u64
                && (!current_chunks.is_empty() || !current_slot_apply_indices.is_empty())
            {
                // 关闭当前 segment（不包含当前 slot，因为已经超过限制了）
                let segment_end = slot; // 当前 slot 不包含在内
                let segment = self.finish_segment(
                    current_segment_start,
                    segment_end,
                    &mut current_chunk_writer,
                    &mut current_chunks,
                    current_slot_apply_indices.clone(),
                    current_max_apply_index,
                    &round_dir,
                )?;
                segments.push(segment);

                // 重置状态，开始新 segment（包含当前 slot）
                current_segment_start = slot;
                current_chunk_writer = ChunkWriter::new(self.config.clone(), round_dir.clone());
                current_chunks.clear();
                current_slot_apply_indices.clear();
                current_max_apply_index = 0;

                // 重新添加当前 slot 的数据到新 segment
                current_slot_apply_indices.insert(slot, metadata.last_applied_index);
                current_max_apply_index = current_max_apply_index.max(metadata.last_applied_index);

                for (key, data) in memory_clone.iter() {
                    current_chunk_writer.add_entry(key.clone(), data)?;
                    if current_chunk_writer.should_flush() {
                        let chunk_info = self.flush_chunk(
                            &mut current_chunk_writer,
                            current_segment_start,
                            slot + 1,
                            &round_dir,
                        )?;
                        current_chunks.push(chunk_info);
                    }
                }
            }
        }

        // 5. 关闭最后一个 segment（即使为空也要生成）
        if !current_chunk_writer.is_empty() || !current_slot_apply_indices.is_empty() {
            let last_slot = slot_list[slot_list.len() - 1];
            let segment = self.finish_segment(
                current_segment_start,
                last_slot + 1, // slot_end (exclusive)
                &mut current_chunk_writer,
                &mut current_chunks,
                current_slot_apply_indices,
                current_max_apply_index,
                &round_dir,
            )?;
            segments.push(segment);
        }

        // 6. 更新 slot metadata
        for segment in &segments {
            for slot in segment.slot_start..segment.slot_end {
                if let Some(slot_store) = slots.get(&slot) {
                    let mut guard = slot_store.write();
                    if let Some(&apply_index) = segment.slot_apply_indices.get(&slot) {
                        guard.metadata_mut().last_segment_index = Some(apply_index);
                    }
                }
            }
        }

        // 7. 开始新轮次
        self.start_new_round();

        // Mark as complete
        self.is_generating = false;
        self.last_generate_time = Some(std::time::Instant::now());

        Ok(segments)
    }

    /// Calculate current segment size (compressed)
    fn calculate_segment_size(&self, chunks: &[ChunkFileInfo]) -> Result<u64, String> {
        Ok(chunks.iter().map(|c| c.compressed_size).sum())
    }

    /// Finish current segment and create metadata
    /// 即使为空也要生成 segment（至少要有 metadata JSON）
    fn finish_segment(
        &self,
        slot_start: u32,
        slot_end: u32,
        chunk_writer: &mut ChunkWriter,
        chunks: &mut Vec<ChunkFileInfo>,
        slot_apply_indices: HashMap<u32, u64>,
        max_apply_index: u64,
        round_dir: &Path,
    ) -> Result<SegmentMetadata, String> {
        // Flush 最后一个 chunk（如果有）
        if !chunk_writer.is_empty() {
            let chunk_info = self.flush_chunk(chunk_writer, slot_start, slot_end, round_dir)?;
            chunks.push(chunk_info);
        }

        // 计算总大小
        let total_uncompressed: u64 = chunks.iter().map(|c| c.uncompressed_size).sum();
        let total_compressed: u64 = chunks.iter().map(|c| c.compressed_size).sum();

        // 即使没有 chunks（空 segment），也要生成 metadata
        let metadata = SegmentMetadata {
            slot_start,
            slot_end,
            apply_index: max_apply_index,
            round: self.current_round,
            chunks: chunks.clone(),
            total_uncompressed_size: total_uncompressed,
            total_compressed_size: total_compressed,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            slot_apply_indices,
        };

        // 写入 JSON 文件（即使为空也要写入）
        let json_path = round_dir.join(format!("{:05}-{:05}.seg.json", slot_start, slot_end));
        let json_content = serde_json::to_string_pretty(&metadata)
            .map_err(|e| format!("Failed to serialize metadata: {}", e))?;
        std::fs::write(&json_path, json_content)
            .map_err(|e| format!("Failed to write metadata: {}", e))?;

        Ok(metadata)
    }

    /// Flush chunk and return chunk info
    fn flush_chunk(
        &self,
        chunk_writer: &mut ChunkWriter,
        slot_start: u32,
        slot_end: u32,
        round_dir: &Path,
    ) -> Result<ChunkFileInfo, String> {
        let chunk_id = chunk_writer.chunk_id();
        let (file_name, uncompressed_size, compressed_size, entry_count, crc32) =
            chunk_writer.flush(slot_start, slot_end, round_dir)?;

        Ok(ChunkFileInfo {
            chunk_id,
            file_name,
            uncompressed_size,
            compressed_size,
            entry_count,
            crc32,
        })
    }

    /// Start new round
    pub fn start_new_round(&mut self) {
        self.current_round += 1;
        // 更新 current_round.txt
        let round_file = self.segments_dir.join("current_round.txt");
        std::fs::write(&round_file, self.current_round.to_string()).ok();
    }

    /// Mark segment generation as complete
    pub fn mark_complete(&mut self) {
        self.is_generating = false;
        self.last_generate_time = Some(std::time::Instant::now());
    }
}

/// Segment Reader (placeholder for future implementation)
pub struct SegmentReader {
    // TODO: Implement segment reading
}

impl SegmentReader {
    pub fn new() -> Self {
        Self {}
    }
}
