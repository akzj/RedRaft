//! Snapshot storage implementation
//!
//! This module provides snapshot storage with the following features:
//! - Chunk-based storage (64MB chunks, compressed with zstd)
//! - Segment generation (periodic full snapshots of Memory COW base)
//! - WAL (Write-Ahead Log) for incremental changes
//! - On-demand snapshot generation for Raft transmission
//!
//! ## Architecture
//!
//! - **Segment**: Periodic full snapshots of Memory COW base, organized by shard
//! - **WAL**: Append-only log of all write operations, with apply_index tracking
//! - **Chunk**: 64MB (uncompressed) chunks of serialized data, compressed with zstd
//!
//! ## Lock Strategy
//!
//! - Read/Write operations: Use read lock (shared access)
//! - Snapshot generation: Use read lock to create COW snapshot, then release
//! - Segment generation: Use read lock (doesn't block writes due to COW)
//! - WAL cleanup: Physical deletion of old WAL files after segment generation

pub mod chunk;
pub mod segment;
pub mod wal;

pub use chunk::{ChunkHeader, ChunkWriter, ChunkReader};
pub use segment::{SegmentGenerator, SegmentMetadata, SegmentReader};
pub use wal::{WalWriter, WalReader, WalMetadata, WalEntry};

use std::path::PathBuf;

/// Snapshot storage configuration
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Base directory for snapshot storage
    pub base_dir: PathBuf,
    
    /// Number of shards
    pub shard_count: u32,
    
    /// Chunk size threshold (uncompressed, in bytes)
    /// Default: 64MB
    pub chunk_size: u64,
    
    /// WAL size threshold for triggering segment generation (in bytes)
    /// Default: 100MB
    pub wal_size_threshold: u64,
    
    /// Time interval for segment generation (in seconds)
    /// Default: 3600 (1 hour)
    pub segment_interval_secs: u64,
    
    /// Zstd compression level (1-22)
    /// Default: 3 (balanced)
    pub zstd_level: i32,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("./data/snapshots"),
            shard_count: 16, // Default 16 shards
            chunk_size: 64 * 1024 * 1024, // 64MB
            wal_size_threshold: 100 * 1024 * 1024, // 100MB
            segment_interval_secs: 3600, // 1 hour
            zstd_level: 3,
        }
    }
}

impl SnapshotConfig {
    pub fn with_base_dir<P: AsRef<std::path::Path>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }
}

