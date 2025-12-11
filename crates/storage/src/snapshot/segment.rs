//! Segment implementation
//!
//! Segment = Memory COW base serialized into chunks
//! - One directory per shard
//! - File naming: shard_{shard_id}-{apply_index}.{chunk_id:05d}.seg
//! - Metadata: metadata.json with shard_index, chunks[], checksum[]

use crate::memory::{DataCow, MemStoreCow};
use crate::shard::ShardId;
use crate::snapshot::{chunk::ChunkWriter, SnapshotConfig};
use crc32fast::Hasher as Crc32Hasher;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use tracing::info;

/// Segment Metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    /// Shard ID
    pub shard_id: ShardId,
    
    /// Apply index for this segment
    pub apply_index: u64,
    
    /// Number of chunks
    pub chunk_count: u32,
    
    /// Total size (uncompressed) in bytes
    pub total_size: u64,
    
    /// Created timestamp
    pub created_at: u64,
    
    /// Checksums for each chunk (CRC32)
    pub checksums: Vec<u32>,
    
    /// Chunk file information
    pub chunks: Vec<ChunkInfo>,
}

/// Chunk information in segment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    /// Chunk ID
    pub chunk_id: u32,
    
    /// File name
    pub file_name: String,
    
    /// Size in bytes (compressed)
    pub size: u64,
    
    /// CRC32 checksum
    pub crc32: u32,
}

/// Segment Generator
pub struct SegmentGenerator {
    config: SnapshotConfig,
    segments_dir: PathBuf,
    is_generating: bool,
    last_generate_time: Option<std::time::Instant>,
}

impl SegmentGenerator {
    pub fn new(config: SnapshotConfig, segments_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&segments_dir)
            .expect("Failed to create segments directory");
        
        Self {
            config,
            segments_dir,
            is_generating: false,
            last_generate_time: None,
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

    /// Generate segment for a shard (read lock, doesn't block writes)
    pub fn generate_segment(
        &mut self,
        shard_id: ShardId,
        memory_store: &MemStoreCow,
        apply_index: u64,
    ) -> Result<SegmentMetadata, String> {
        // Mark as generating
        self.is_generating = true;
        
        // Get read lock to clone COW base
        let cow_base = {
            let base = memory_store.base.read();
            base.clone() // Clone the HashMap (keys and DataCow references)
        };
        
        // Create shard directory
        let shard_dir = self.segments_dir.join(format!("shard_{}", shard_id));
        std::fs::create_dir_all(&shard_dir)
            .map_err(|e| format!("Failed to create shard directory: {}", e))?;
        
        // Generate chunks
        let chunk_output_dir = shard_dir.join("temp_chunks");
        std::fs::create_dir_all(&chunk_output_dir)
            .map_err(|e| format!("Failed to create chunk output directory: {}", e))?;
        
        let mut chunk_writer = ChunkWriter::new(self.config.clone(), chunk_output_dir.clone());
        
        // Traverse HashMap and write entries
        for (key, data_cow) in cow_base.iter() {
            chunk_writer.add_entry(key.clone(), data_cow)?;
        }
        
        // Finish writing chunks
        let chunk_files = chunk_writer.finish()?;
        
        // Move chunks to segment files and calculate checksums
        let mut chunks = Vec::new();
        let mut checksums = Vec::new();
        let mut total_size = 0u64;
        
        for (chunk_id, chunk_file) in chunk_files.iter().enumerate() {
            let chunk_id = (chunk_id + 1) as u32;
            let segment_file_name = format!("shard_{}-{}.{:05}.seg", shard_id, apply_index, chunk_id);
            let segment_file_path = shard_dir.join(&segment_file_name);
            
            // Calculate checksum
            let checksum = self.calculate_file_checksum(chunk_file)?;
            checksums.push(checksum);
            
            // Get file size
            let file_size = std::fs::metadata(chunk_file)
                .map_err(|e| format!("Failed to get chunk file metadata: {}", e))?
                .len();
            
            // Move chunk file to segment file
            std::fs::rename(chunk_file, &segment_file_path)
                .map_err(|e| format!("Failed to move chunk to segment: {}", e))?;
            
            chunks.push(ChunkInfo {
                chunk_id,
                file_name: segment_file_name,
                size: file_size,
                crc32: checksum,
            });
            
            total_size += file_size;
        }
        
        // Clean up temp directory
        std::fs::remove_dir_all(&chunk_output_dir)
            .map_err(|e| format!("Failed to remove temp directory: {}", e))?;
        
        // Create metadata
        let metadata = SegmentMetadata {
            shard_id,
            apply_index,
            chunk_count: chunks.len() as u32,
            total_size,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            checksums,
            chunks,
        };
        
        // Save metadata
        let metadata_path = shard_dir.join("metadata.json");
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| format!("Failed to serialize metadata: {}", e))?;
        std::fs::write(&metadata_path, metadata_json)
            .map_err(|e| format!("Failed to write metadata: {}", e))?;
        
        info!(
            "Generated segment for shard {} at apply_index {}: {} chunks, {} bytes",
            shard_id, apply_index, metadata.chunk_count, total_size
        );
        
        Ok(metadata)
    }

    /// Calculate file checksum (CRC32)
    fn calculate_file_checksum(&self, file_path: &Path) -> Result<u32, String> {
        let mut file = BufReader::new(
            File::open(file_path)
                .map_err(|e| format!("Failed to open file for checksum: {}", e))?,
        );
        let mut hasher = Crc32Hasher::new();
        let mut buffer = [0u8; 8192];
        
        loop {
            let bytes_read = file.read(&mut buffer)
                .map_err(|e| format!("Failed to read file for checksum: {}", e))?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }
        
        Ok(hasher.finalize())
    }

    /// Mark segment generation as complete
    pub fn mark_complete(&mut self) {
        self.is_generating = false;
        self.last_generate_time = Some(std::time::Instant::now());
    }

    /// Get the latest segment for a shard
    pub fn get_latest_segment(&self, shard_id: ShardId) -> Result<Option<SegmentMetadata>, String> {
        let shard_dir = self.segments_dir.join(format!("shard_{}", shard_id));
        
        if !shard_dir.exists() {
            return Ok(None);
        }
        
        let metadata_path = shard_dir.join("metadata.json");
        if !metadata_path.exists() {
            return Ok(None);
        }
        
        let content = std::fs::read_to_string(&metadata_path)
            .map_err(|e| format!("Failed to read segment metadata: {}", e))?;
        let metadata: SegmentMetadata = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse segment metadata: {}", e))?;
        
        Ok(Some(metadata))
    }
}

/// Segment Reader
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

    /// Read segment metadata for a shard
    pub fn read_metadata(&self, shard_id: ShardId) -> Result<Option<SegmentMetadata>, String> {
        let shard_dir = self.segments_dir.join(format!("shard_{}", shard_id));
        
        if !shard_dir.exists() {
            return Ok(None);
        }
        
        let metadata_path = shard_dir.join("metadata.json");
        if !metadata_path.exists() {
            return Ok(None);
        }
        
        let content = std::fs::read_to_string(&metadata_path)
            .map_err(|e| format!("Failed to read segment metadata: {}", e))?;
        
        // Verify checksum
        let metadata: SegmentMetadata = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse segment metadata: {}", e))?;
        
        // Verify chunk files exist and checksums match
        for chunk_info in &metadata.chunks {
            let chunk_path = shard_dir.join(&chunk_info.file_name);
            if !chunk_path.exists() {
                return Err(format!("Chunk file not found: {}", chunk_info.file_name));
            }
            
            // Verify checksum
            let actual_checksum = self.calculate_file_checksum(&chunk_path)?;
            if actual_checksum != chunk_info.crc32 {
                return Err(format!(
                    "Checksum mismatch for chunk {}: expected {}, got {}",
                    chunk_info.file_name, chunk_info.crc32, actual_checksum
                ));
            }
        }
        
        Ok(Some(metadata))
    }

    /// Calculate file checksum (CRC32)
    fn calculate_file_checksum(&self, file_path: &Path) -> Result<u32, String> {
        let mut file = BufReader::new(
            File::open(file_path)
                .map_err(|e| format!("Failed to open file for checksum: {}", e))?,
        );
        let mut hasher = Crc32Hasher::new();
        let mut buffer = [0u8; 8192];
        
        loop {
            let bytes_read = file.read(&mut buffer)
                .map_err(|e| format!("Failed to read file for checksum: {}", e))?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }
        
        Ok(hasher.finalize())
    }
}

