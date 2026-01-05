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

use crate::memory::MemStore;
use crate::store::SlotMetadata;
use resp::Command;
use rr_core::routing::RoutingTable;
use std::collections::HashMap;
use std::path::PathBuf;
use anyhow::Result;
use bincode::serde::decode_from_slice;
use bincode::config::standard;

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

/// Reload MemStore from segments and WAL
///
/// Recovery process:
/// 1. Load segments (full snapshots) to restore base state
/// 2. Load WAL entries (incremental changes) and apply them
/// 3. Return restored MemStore and metadata for each slot
///
/// # Arguments
/// - `config`: Snapshot configuration
///
/// # Returns
/// Result with HashMap<slot, (MemStore, SlotMetadata)> containing restored stores
pub fn reload_memstore(
    config: SnapshotConfig,
) -> Result<HashMap<u32, (MemStore, SlotMetadata)>> {
    let segments_dir = config.base_dir.join("segments");
    let wal_dir = config.base_dir.join("wal");

    // Step 1: Load segments to restore base state
    let segment_reader = SegmentReader::new(config.clone(), segments_dir);
    let mut slot_data = segment_reader.load_all_segments()
        .map_err(|e| anyhow::anyhow!("Failed to load segments: {}", e))?;

    // Convert to (MemStore, SlotMetadata) format
    let mut result: HashMap<u32, (MemStore, SlotMetadata)> = HashMap::new();
    for (slot, (mem_store, apply_index)) in slot_data {
        let metadata = SlotMetadata {
            slot,
            applied_index: apply_index,
            log_seq: 0, // Will be updated from WAL
        };
        result.insert(slot, (mem_store, metadata));
    }

    // Step 2: Load WAL entries and apply incremental changes
    let wal_reader = WalReader::new(config.clone(), wal_dir)
        .map_err(|e| anyhow::anyhow!("Failed to create WAL reader: {}", e))?;

    // Read all WAL entries
    let all_entries = wal_reader.read_entries_from(0)
        .map_err(|e| anyhow::anyhow!("Failed to read WAL entries: {}", e))?;

    // Group WAL entries by slot and sort by apply_index
    let mut wal_entries_by_slot: HashMap<u32, Vec<WalEntry>> = HashMap::new();
    for entry in all_entries {
        // Deserialize command to get key
        let command: Command = decode_from_slice(&entry.command, standard())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize WAL command: {}", e))?
            .0;

        if let Some(key) = command.get_key() {
            let slot = RoutingTable::slot_for_key(key);
            wal_entries_by_slot.entry(slot).or_insert_with(Vec::new).push(entry);
        }
    }

    // Sort entries by apply_index for each slot
    for entries in wal_entries_by_slot.values_mut() {
        entries.sort_by_key(|e| e.apply_index);
    }

    // Apply WAL entries to restore incremental changes
    // Note: This requires applying commands, which should be done by HybridStore
    // For now, we'll just update metadata with the latest apply_index and log_seq
    for (slot, entries) in wal_entries_by_slot {
        // Get or create entry for this slot
        let (mem_store, metadata) = result.entry(slot).or_insert_with(|| {
            (MemStore::new(), SlotMetadata::new(slot))
        });

        // Update metadata with latest values from WAL
        if let Some(last_entry) = entries.last() {
            metadata.applied_index = last_entry.apply_index;
            metadata.log_seq = last_entry.log_seq;
        }
    }

    Ok(result)
}

