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
use rr_core::routing::RoutingTable;
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
    /// Round number
    pub round: u64,
    /// Chunk files information
    pub chunks: Vec<ChunkFileInfo>,
    /// Total uncompressed size
    pub total_uncompressed_size: u64,
    /// Total compressed size
    pub total_compressed_size: u64,
    /// Created timestamp
    pub created_at: u64,
    /// Slot -> apply_index mapping (records apply_index for each slot)
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
    /// Iterate slots in order, check size after writing, create new segment for next slot if exceeds 64MB
    pub fn generate_segments(
        &mut self,
        slots: &HashMap<u32, LockedSlotStore>,
    ) -> Result<Vec<SegmentMetadata>, String> {
        // Mark as generating
        self.is_generating = true;

        // 1. Get all slot numbers and sort them
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

        // 2. Iterate slots in order
        for &slot in &slot_list {
            // Get slot data
            let (memory_clone, metadata) = {
                if let Some(slot_store) = slots.get(&slot) {
                    let guard = slot_store.read();
                    (guard.memory().clone(), guard.metadata().clone())
                } else {
                    // Slot doesn't exist, use empty data
                    (MemStore::new(), SlotMetadata::new(slot))
                }
            };

            // Record this slot's apply_index
            current_slot_apply_indices.insert(slot, metadata.applied_index);
            current_max_apply_index = current_max_apply_index.max(metadata.applied_index);

            // Add this slot's data to current segment
            for (key, data) in memory_clone.iter() {
                current_chunk_writer.add_entry(key.clone(), data)?;

                // If current chunk is full, flush it
                if current_chunk_writer.should_flush() {
                    let chunk_info = self.flush_chunk(
                        &mut current_chunk_writer,
                        current_segment_start,
                        slot + 1, // Temporary slot_end, will be finalized in finish_segment
                        &round_dir,
                    )?;
                    current_chunks.push(chunk_info);
                }
            }

            // 3. Check current segment size after writing
            let current_segment_size = self.calculate_segment_size(&current_chunks)?;

            // 4. If exceeds 64MB and current segment has data, close current segment
            if current_segment_size > self.config.chunk_size as u64
                && (!current_chunks.is_empty() || !current_slot_apply_indices.is_empty())
            {
                // Close current segment (excluding current slot, as it exceeds the limit)
                let segment_end = slot; // Current slot is not included
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

                // Reset state, start new segment (including current slot)
                current_segment_start = slot;
                current_chunk_writer = ChunkWriter::new(self.config.clone(), round_dir.clone());
                current_chunks.clear();
                current_slot_apply_indices.clear();
                current_max_apply_index = 0;

                // Re-add current slot's data to new segment
                current_slot_apply_indices.insert(slot, metadata.applied_index);
                current_max_apply_index = current_max_apply_index.max(metadata.applied_index);

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

        // 5. Close the last segment (generate even if empty)
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

        // 6. Update slot metadata
        for segment in &segments {
            for slot in segment.slot_start..segment.slot_end {
                if let Some(slot_store) = slots.get(&slot) {
                    let mut guard = slot_store.write();
                    if let Some(&apply_index) = segment.slot_apply_indices.get(&slot) {
                        guard.metadata_mut().log_seq = apply_index;
                    }
                }
            }
        }

        // 7. Start new round
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
    /// Generate segment even if empty (at least metadata JSON is required)
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
        // Flush the last chunk (if any)
        if !chunk_writer.is_empty() {
            let chunk_info = self.flush_chunk(chunk_writer, slot_start, slot_end, round_dir)?;
            chunks.push(chunk_info);
        }

        // Calculate total size
        let total_uncompressed: u64 = chunks.iter().map(|c| c.uncompressed_size).sum();
        let total_compressed: u64 = chunks.iter().map(|c| c.compressed_size).sum();

        // Generate metadata even if there are no chunks (empty segment)
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

        // Write JSON file (write even if empty)
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
        // Update current_round.txt
        let round_file = self.segments_dir.join("current_round.txt");
        std::fs::write(&round_file, self.current_round.to_string()).ok();
    }

    /// Mark segment generation as complete
    pub fn mark_complete(&mut self) {
        self.is_generating = false;
        self.last_generate_time = Some(std::time::Instant::now());
    }
}

/// Segment Reader for loading segments and restoring MemStore
pub struct SegmentReader {
    config: SnapshotConfig,
    segments_dir: PathBuf,
}

impl SegmentReader {
    pub fn new(config: SnapshotConfig, segments_dir: PathBuf) -> Self {
        Self {
            config,
            segments_dir,
        }
    }

    /// Load all segments for a given slot and restore MemStore
    ///
    /// Returns (MemStore, max_apply_index) for the slot
    pub fn load_slot_segment(&self, slot: u32) -> Result<(MemStore, u64), String> {
        // Find the latest segment that contains this slot
        let latest_segment = self.find_latest_segment_for_slot(slot)?;
        
        if let Some(segment_meta) = latest_segment {
            // Load segment data
            let mut mem_store = MemStore::new();
            let mut max_apply_index = 0u64;

            let round_dir = self.segments_dir.join(format!("{}", segment_meta.round));
            
            // Load all chunks for this segment
            for chunk_info in &segment_meta.chunks {
                let chunk_file = round_dir.join(&chunk_info.file_name);
                let chunk_reader = crate::snapshot::chunk::ChunkReader::new(self.config.clone());
                let entries = chunk_reader.read_chunk(&chunk_file)?;

                // Deserialize and insert entries into MemStore
                for entry in entries {
                    let data = Data::deserialize(&entry.data_type, &entry.data)
                        .map_err(|e| format!("Failed to deserialize data: {}", e))?;
                    mem_store.insert(entry.key, data);
                }
            }

            // Get apply_index for this slot
            if let Some(&apply_index) = segment_meta.slot_apply_indices.get(&slot) {
                max_apply_index = apply_index;
            }

            Ok((mem_store, max_apply_index))
        } else {
            // No segment found, return empty store
            Ok((MemStore::new(), 0))
        }
    }

    /// Find the latest segment that contains a given slot
    fn find_latest_segment_for_slot(&self, slot: u32) -> Result<Option<SegmentMetadata>, String> {
        // Find the latest round
        let current_round = SegmentGenerator::load_current_round(&self.segments_dir);
        
        // Search from latest round backwards
        for round in (1..=current_round).rev() {
            let round_dir = self.segments_dir.join(format!("{}", round));
            if !round_dir.exists() {
                continue;
            }

            // Find all segment metadata files in this round
            let entries = std::fs::read_dir(&round_dir)
                .map_err(|e| format!("Failed to read round directory: {}", e))?;

            let mut segments = Vec::new();
            for entry in entries {
                let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if ext == "json" {
                        let content = std::fs::read_to_string(&path)
                            .map_err(|e| format!("Failed to read segment metadata: {}", e))?;
                        let meta: SegmentMetadata = serde_json::from_str(&content)
                            .map_err(|e| format!("Failed to parse segment metadata: {}", e))?;
                        segments.push(meta);
                    }
                }
            }

            // Find segment that contains this slot
            for segment in segments {
                if slot >= segment.slot_start && slot < segment.slot_end {
                    return Ok(Some(segment));
                }
            }
        }

        Ok(None)
    }

    /// Load all segments and restore MemStore for all slots
    ///
    /// Returns HashMap<slot, (MemStore, apply_index)>
    pub fn load_all_segments(&self) -> Result<HashMap<u32, (MemStore, u64)>, String> {
        let mut result = HashMap::new();
        
        // Find all slots from all segments
        let current_round = SegmentGenerator::load_current_round(&self.segments_dir);
        
        for round in 1..=current_round {
            let round_dir = self.segments_dir.join(format!("{}", round));
            if !round_dir.exists() {
                continue;
            }

            let entries = std::fs::read_dir(&round_dir)
                .map_err(|e| format!("Failed to read round directory: {}", e))?;

            for entry in entries {
                let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if ext == "json" {
                        let content = std::fs::read_to_string(&path)
                            .map_err(|e| format!("Failed to read segment metadata: {}", e))?;
                        let segment_meta: SegmentMetadata = serde_json::from_str(&content)
                            .map_err(|e| format!("Failed to parse segment metadata: {}", e))?;

                        // Load segment data for all slots in range
                        for slot in segment_meta.slot_start..segment_meta.slot_end {
                            // Only load if we haven't loaded a newer version
                            if result.contains_key(&slot) {
                                continue;
                            }

                            let (mem_store, apply_index) = self.load_segment_data(&segment_meta, &round_dir, slot)?;
                            result.insert(slot, (mem_store, apply_index));
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Load segment data for a specific slot
    ///
    /// Only loads entries that belong to the specified slot (based on key slot calculation)
    fn load_segment_data(
        &self,
        segment_meta: &SegmentMetadata,
        round_dir: &Path,
        slot: u32,
    ) -> Result<(MemStore, u64), String> {
        let mut mem_store = MemStore::new();
        let chunk_reader = crate::snapshot::chunk::ChunkReader::new(self.config.clone());

        // Load all chunks for this segment
        for chunk_info in &segment_meta.chunks {
            let chunk_file = round_dir.join(&chunk_info.file_name);
            let entries = chunk_reader.read_chunk(&chunk_file)?;

            // Deserialize and insert entries into MemStore (only for this slot)
            for entry in entries {
                // Check if this entry belongs to the target slot
                let entry_slot = RoutingTable::slot_for_key(&entry.key);
                if entry_slot == slot {
                    let data = Data::deserialize(&entry.data_type, &entry.data)
                        .map_err(|e| format!("Failed to deserialize data: {}", e))?;
                    mem_store.insert(entry.key, data);
                }
            }
        }

        // Get apply_index for this slot
        let apply_index = segment_meta.slot_apply_indices.get(&slot).copied().unwrap_or(0);

        Ok((mem_store, apply_index))
    }
}
