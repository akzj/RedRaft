//! Chunk file format implementation
//!
//! Chunk format:
//! - Header: chunk_id, entry_count, uncompressed_size, compressed_size
//! - Data: [entry_size (u32), crc32 (u32), compressed_data (Vec<u8>)]
//!   - entry_size: number of entries (not bytes)
//!   - compressed_data: Vec<Entry> serialized with bincode, then compressed with zstd

use crate::memory::DataCow;
use crate::snapshot::SnapshotConfig;
use bincode::{config::standard, serde::{decode_from_slice, encode_to_vec}};
use crc32fast::Hasher as Crc32Hasher;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use tracing::debug;

/// Chunk header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkHeader {
    /// Chunk ID (starting from 1)
    pub chunk_id: u32,
    
    /// Number of entries in this chunk
    pub entry_count: u32,
    
    /// Uncompressed size (in bytes)
    pub uncompressed_size: u64,
    
    /// Compressed size (in bytes)
    pub compressed_size: u64,
}

/// Entry in a chunk (before serialization)
/// 
/// Note: DataCow serialization will be implemented separately
/// For now, we store the serialized data as Vec<u8>
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkEntry {
    pub key: Vec<u8>,
    pub data_type: String, // "list", "set", "zset", "bitmap"
    pub data: Vec<u8>,     // Serialized DataCow (to be implemented)
}

/// Chunk writer
pub struct ChunkWriter {
    config: SnapshotConfig,
    current_chunk: Vec<ChunkEntry>,
    current_size: u64,
    chunk_id: u32,
    output_dir: PathBuf,
}

impl ChunkWriter {
    pub fn new(config: SnapshotConfig, output_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&output_dir).expect("Failed to create chunk output directory");
        
        Self {
            config,
            current_chunk: Vec::new(),
            current_size: 0,
            chunk_id: 1,
            output_dir,
        }
    }

    /// Add an entry to the current chunk
    /// 
    /// Serializes DataCow base data (extracts from COW structure)
    pub fn add_entry(&mut self, key: Vec<u8>, data: &DataCow) -> Result<(), String> {
        // Serialize base data using DataCow's method
        let serialized_data = data.serialize_base()?;
        let data_type = data.type_name().to_string();
        let entry = ChunkEntry {
            key: key.clone(),
            data_type: data_type.clone(),
            data: serialized_data.clone(),
        };
        
        // Estimate size (serialize to get actual size)
        let serialized = encode_to_vec(&entry, standard())
            .map_err(|e| format!("Failed to serialize entry: {}", e))?;
        let entry_size = serialized.len() as u64;
        
        // If adding this entry would exceed chunk size, flush current chunk
        if self.current_size + entry_size > self.config.chunk_size && !self.current_chunk.is_empty() {
            self.flush_chunk()?;
        }
        
        let entry = ChunkEntry {
            key,
            data_type,
            data: serialized_data.clone(),
        };
        
        let serialized = encode_to_vec(&entry, standard())
            .map_err(|e| format!("Failed to serialize entry: {}", e))?;
        let entry_size = serialized.len() as u64;
        
        // If adding this entry would exceed chunk size, flush current chunk
        if self.current_size + entry_size > self.config.chunk_size && !self.current_chunk.is_empty() {
            self.flush_chunk()?;
        }
        
        self.current_chunk.push(entry);
        self.current_size += entry_size;
        
        Ok(())
    }

    /// Flush the current chunk to disk
    fn flush_chunk(&mut self) -> Result<PathBuf, String> {
        if self.current_chunk.is_empty() {
            return Err("No entries to flush".to_string());
        }
        
        let chunk_file = self.output_dir.join(format!("chunk_{:05}.bin", self.chunk_id));
        let mut file = BufWriter::new(
            File::create(&chunk_file)
                .map_err(|e| format!("Failed to create chunk file: {}", e))?,
        );
        
        // Serialize entries
        let serialized = encode_to_vec(&self.current_chunk, standard())
            .map_err(|e| format!("Failed to serialize chunk entries: {}", e))?;
        let uncompressed_size = serialized.len() as u64;
        
        // Compress with zstd
        let compressed = zstd::encode_all(
            serialized.as_slice(),
            self.config.zstd_level,
        )
        .map_err(|e| format!("Failed to compress chunk: {}", e))?;
        let compressed_size = compressed.len() as u64;
        
        // Calculate CRC32
        let mut hasher = Crc32Hasher::new();
        hasher.update(&compressed);
        let crc32 = hasher.finalize();
        
        // Write header
        let header = ChunkHeader {
            chunk_id: self.chunk_id,
            entry_count: self.current_chunk.len() as u32,
            uncompressed_size,
            compressed_size,
        };
        
        let header_bytes = encode_to_vec(&header, standard())
            .map_err(|e| format!("Failed to serialize chunk header: {}", e))?;
        file.write_all(&(header_bytes.len() as u32).to_le_bytes())
            .map_err(|e| format!("Failed to write header size: {}", e))?;
        file.write_all(&header_bytes)
            .map_err(|e| format!("Failed to write header: {}", e))?;
        
        // Write data: [entry_size, crc32, compressed_data]
        file.write_all(&(self.current_chunk.len() as u32).to_le_bytes())
            .map_err(|e| format!("Failed to write entry count: {}", e))?;
        file.write_all(&crc32.to_le_bytes())
            .map_err(|e| format!("Failed to write CRC32: {}", e))?;
        file.write_all(&compressed)
            .map_err(|e| format!("Failed to write compressed data: {}", e))?;
        
        file.flush()
            .map_err(|e| format!("Failed to flush chunk file: {}", e))?;
        
        debug!(
            "Flushed chunk {}: {} entries, {} bytes (uncompressed) -> {} bytes (compressed)",
            self.chunk_id,
            self.current_chunk.len(),
            uncompressed_size,
            compressed_size
        );
        
        self.chunk_id += 1;
        self.current_chunk.clear();
        self.current_size = 0;
        
        Ok(chunk_file)
    }

    /// Finish writing (flush remaining chunk)
    pub fn finish(mut self) -> Result<Vec<PathBuf>, String> {
        let mut chunk_files = Vec::new();
        
        if !self.current_chunk.is_empty() {
            chunk_files.push(self.flush_chunk()?);
        }
        
        Ok(chunk_files)
    }
}

/// Chunk reader
pub struct ChunkReader {
    config: SnapshotConfig,
}

impl ChunkReader {
    pub fn new(config: SnapshotConfig) -> Self {
        Self { config }
    }

    /// Read a chunk file and return entries
    pub fn read_chunk(&self, chunk_file: &PathBuf) -> Result<Vec<ChunkEntry>, String> {
        let mut file = BufReader::new(
            File::open(chunk_file)
                .map_err(|e| format!("Failed to open chunk file: {}", e))?,
        );
        
        // Read header size
        let mut header_size_bytes = [0u8; 4];
        file.read_exact(&mut header_size_bytes)
            .map_err(|e| format!("Failed to read header size: {}", e))?;
        let header_size = u32::from_le_bytes(header_size_bytes) as usize;
        
        // Read header
        let mut header_bytes = vec![0u8; header_size];
        file.read_exact(&mut header_bytes)
            .map_err(|e| format!("Failed to read header: {}", e))?;
        let (header, _): (ChunkHeader, _) = decode_from_slice(&header_bytes, standard())
            .map_err(|e| format!("Failed to deserialize header: {}", e))?;
        
        // Read data: [entry_count, crc32, compressed_data]
        let mut entry_count_bytes = [0u8; 4];
        file.read_exact(&mut entry_count_bytes)
            .map_err(|e| format!("Failed to read entry count: {}", e))?;
        let entry_count = u32::from_le_bytes(entry_count_bytes);
        
        let mut crc32_bytes = [0u8; 4];
        file.read_exact(&mut crc32_bytes)
            .map_err(|e| format!("Failed to read CRC32: {}", e))?;
        let expected_crc32 = u32::from_le_bytes(crc32_bytes);
        
        let mut compressed_data = vec![0u8; header.compressed_size as usize];
        file.read_exact(&mut compressed_data)
            .map_err(|e| format!("Failed to read compressed data: {}", e))?;
        
        // Verify CRC32
        let mut hasher = Crc32Hasher::new();
        hasher.update(&compressed_data);
        let actual_crc32 = hasher.finalize();
        
        if actual_crc32 != expected_crc32 {
            return Err(format!(
                "CRC32 mismatch: expected {}, got {}",
                expected_crc32, actual_crc32
            ));
        }
        
        // Decompress
        let decompressed = zstd::decode_all(compressed_data.as_slice())
            .map_err(|e| format!("Failed to decompress chunk: {}", e))?;
        
        if decompressed.len() as u64 != header.uncompressed_size {
            return Err(format!(
                "Decompressed size mismatch: expected {}, got {}",
                header.uncompressed_size,
                decompressed.len()
            ));
        }
        
        // Deserialize entries
        let (entries, _): (Vec<ChunkEntry>, _) = decode_from_slice(&decompressed, standard())
            .map_err(|e| format!("Failed to deserialize entries: {}", e))?;
        
        if entries.len() != entry_count as usize {
            return Err(format!(
                "Entry count mismatch: expected {}, got {}",
                entry_count,
                entries.len()
            ));
        }
        
        Ok(entries)
    }
}

