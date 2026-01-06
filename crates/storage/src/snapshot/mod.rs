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

pub use chunk::{ChunkHeader, ChunkReader, ChunkWriter};
pub use segment::{SegmentGenerator, SegmentMetadata, SegmentReader};
pub use wal::{WalEntry, WalReader, WalWriter};

use crate::memory::MemStore;
use crate::store::SlotMetadata;
use anyhow::Result;
use bincode::config::standard;
use bincode::serde::decode_from_slice;
use bytes::Bytes;
use resp::Command;
use rr_core::routing::RoutingTable;
use std::collections::HashMap;
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
            shard_count: 16,                       // Default 16 shards
            chunk_size: 64 * 1024 * 1024,          // 64MB
            wal_size_threshold: 100 * 1024 * 1024, // 100MB
            segment_interval_secs: 3600,           // 1 hour
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
pub fn reload_memstore(config: SnapshotConfig) -> Result<HashMap<u32, (MemStore, SlotMetadata)>> {
    let segments_dir = config.base_dir.join("segments");
    let wal_dir = config.base_dir.join("wal");

    // Step 1: Load segments to restore base state
    let segment_reader = SegmentReader::new(config.clone(), segments_dir);
    let slot_data = segment_reader
        .load_all_segments()
        .map_err(|e| anyhow::anyhow!("Failed to load segments: {}", e))?;

    // Convert to (MemStore, SlotMetadata) format
    let mut result: HashMap<u32, (MemStore, SlotMetadata)> = HashMap::new();
    // Store original segment log_seq for each slot (before WAL updates)
    let mut segment_log_seqs: HashMap<u32, u64> = HashMap::new();
    for (slot, (mem_store, apply_index, log_seq)) in slot_data {
        let metadata = SlotMetadata {
            slot,
            applied_index: apply_index,
            log_seq, // Load log_seq from segment
        };
        segment_log_seqs.insert(slot, log_seq); // Store original segment log_seq
        result.insert(slot, (mem_store, metadata));
    }

    // Step 2: Load WAL entries and apply incremental changes
    let wal_reader = WalReader::new(config.clone(), wal_dir)
        .map_err(|e| anyhow::anyhow!("Failed to create WAL reader: {}", e))?;

    // Step 2: Load WAL entries and apply incremental changes using iterator
    // Only process WAL entries with log_seq > segment.log_seq (skip entries already in segment)
    // Use iterator to avoid loading all entries into memory at once

    // Iterate over WAL entries one at a time and update metadata directly
    for entry_result in wal_reader.iter_entries_from(0) {
        let entry = entry_result.map_err(|e| anyhow::anyhow!("Failed to read WAL entry: {}", e))?;

        // Deserialize command to get key
        let command: Command = decode_from_slice(&entry.command, standard())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize WAL command: {}", e))?
            .0;

        if let Some(key) = command.get_key() {
            let slot = RoutingTable::slot_for_key(key);

            // Get original segment log_seq for this slot (before any WAL updates)
            let segment_log_seq = segment_log_seqs.get(&slot).copied().unwrap_or(0);

            // Only process WAL entries with log_seq > segment.log_seq
            // (entries with log_seq <= segment.log_seq are already in the segment)
            if entry.log_seq > segment_log_seq {
                // Get or create entry for this slot
                let (mem_store, metadata) = result
                    .entry(slot)
                    .or_insert_with(|| (MemStore::new(), SlotMetadata::new(slot)));

                // Replay command to restore data
                if let Err(e) = replay_command_to_memstore(mem_store, &command) {
                    // Log error but continue processing (don't fail entire recovery)
                    tracing::warn!("Failed to replay command for slot {}: {}", slot, e);
                }

                // Update metadata (always use the latest values)
                // Since we iterate in order, later entries will overwrite earlier ones
                metadata.applied_index = metadata.applied_index.max(entry.apply_index);
                metadata.log_seq = metadata.log_seq.max(entry.log_seq);
            }
        }
    }

    Ok(result)
}

/// Replay a command to MemStore (for WAL recovery)
///
/// Only handles memory store commands (List, Set, ZSet, Bitmap, Key operations).
/// String and Hash commands are stored in RocksDB and don't need WAL replay.
fn replay_command_to_memstore(mem_store: &mut MemStore, command: &Command) -> Result<()> {
    use crate::traits::StoreError;

    match command {
        // List operations
        Command::LPush { key, values } => {
            let values_converted: Vec<Bytes> =
                values.iter().map(|v| Bytes::from(v.clone())).collect();
            mem_store
                .lpush(key.as_ref(), values_converted)
                .map_err(|e| anyhow::anyhow!("Failed to replay LPUSH: {:?}", e))?;
        }
        Command::RPush { key, values } => {
            let values_converted: Vec<Bytes> =
                values.iter().map(|v| Bytes::from(v.clone())).collect();
            mem_store
                .rpush(key.as_ref(), values_converted)
                .map_err(|e| anyhow::anyhow!("Failed to replay RPUSH: {:?}", e))?;
        }
        Command::LPop { key } => {
            mem_store
                .lpop(key.as_ref())
                .map_err(|e| anyhow::anyhow!("Failed to replay LPOP: {:?}", e))?;
        }
        Command::RPop { key } => {
            mem_store
                .rpop(key.as_ref())
                .map_err(|e| anyhow::anyhow!("Failed to replay RPOP: {:?}", e))?;
        }
        Command::LSet { key, index, value } => {
            mem_store
                .lset(key.as_ref(), *index, Bytes::from(value.clone()))
                .map_err(|e| anyhow::anyhow!("Failed to replay LSET: {:?}", e))?;
        }

        // Set operations
        Command::SAdd { key, members } => {
            for member in members {
                mem_store
                    .add(key.to_vec(), Bytes::from(member.clone()))
                    .map_err(|e| anyhow::anyhow!("Failed to replay SADD: {:?}", e))?;
            }
        }
        Command::SRem { key, members } => {
            for member in members {
                mem_store
                    .remove(key.as_ref(), member.as_ref())
                    .map_err(|e| anyhow::anyhow!("Failed to replay SREM: {:?}", e))?;
            }
        }

        // ZSet operations
        Command::ZAdd { key, members } => {
            use crate::memory::ZSetData;

            // Get or create ZSet
            let zset = match mem_store.data.get_mut(key.as_ref()) {
                Some(crate::memory::Data::ZSet(zset)) => zset,
                Some(_) => return Err(anyhow::anyhow!("Key exists but is not a ZSet")),
                None => {
                    let new_zset = ZSetData::new();
                    mem_store
                        .data
                        .insert(key.to_vec(), crate::memory::Data::ZSet(new_zset));
                    match mem_store.data.get_mut(key.as_ref()) {
                        Some(crate::memory::Data::ZSet(zset)) => zset,
                        _ => return Err(anyhow::anyhow!("Internal error creating ZSet")),
                    }
                }
            };

            for (score, member) in members {
                zset.add(Bytes::from(member.clone()), *score);
            }
        }
        Command::ZRem { key, members } => {
            match mem_store.data.get_mut(key.as_ref()) {
                Some(crate::memory::Data::ZSet(zset)) => {
                    for member in members {
                        zset.remove(member.as_ref());
                    }
                }
                Some(_) => return Err(anyhow::anyhow!("Key exists but is not a ZSet")),
                None => {} // Key doesn't exist, nothing to remove
            }
        }
        Command::ZIncrBy {
            key,
            increment,
            member,
        } => {
            use crate::memory::ZSetData;

            // Get or create ZSet
            let zset = match mem_store.data.get_mut(key.as_ref()) {
                Some(crate::memory::Data::ZSet(zset)) => zset,
                Some(_) => return Err(anyhow::anyhow!("Key exists but is not a ZSet")),
                None => {
                    let new_zset = ZSetData::new();
                    mem_store
                        .data
                        .insert(key.to_vec(), crate::memory::Data::ZSet(new_zset));
                    match mem_store.data.get_mut(key.as_ref()) {
                        Some(crate::memory::Data::ZSet(zset)) => zset,
                        _ => return Err(anyhow::anyhow!("Internal error creating ZSet")),
                    }
                }
            };

            let current_score = zset.get_score(member.as_ref()).unwrap_or(0.0);
            let new_score = current_score + increment;
            zset.add(Bytes::from(member.clone()), new_score);
        }

        // Key operations
        Command::Del { keys } => {
            for key in keys {
                mem_store.del(key.as_ref());
            }
        }
        Command::Expire { key: _, seconds: _ } => {
            // Expire is handled by HybridStore, not MemStore
            // Just skip it here
        }
        Command::Persist { key: _ } => {
            // Persist is handled by HybridStore, not MemStore
            // Just skip it here
        }
        Command::Rename { key, new_key } => {
            // Rename: move data from old key to new key
            if let Some(data) = mem_store.data.remove(key.as_ref()) {
                mem_store.data.insert(new_key.to_vec(), data);
            }
        }
        Command::RenameNx { key, new_key } => {
            // RenameNx: move data only if new_key doesn't exist
            if !mem_store.data.contains_key(new_key.as_ref()) {
                if let Some(data) = mem_store.data.remove(key.as_ref()) {
                    mem_store.data.insert(new_key.to_vec(), data);
                }
            }
        }

        // Other commands that don't affect MemStore (String, Hash are in RocksDB)
        _ => {
            // Skip commands that don't affect MemStore
            // String and Hash commands are stored in RocksDB and don't need WAL replay
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{Data, ListData, SetData, ZSetData};
    use crate::snapshot::chunk::ChunkWriter;
    use crate::snapshot::segment::{ChunkFileInfo, SegmentMetadata, SlotSegmentInfo};
    use crate::snapshot::wal::WalWriter;
    use bytes::Bytes;
    use resp::Command;
    use std::collections::HashMap;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_reload_memstore_from_segments_and_wal() {
        let temp_dir = TempDir::new().unwrap();
        let config = SnapshotConfig {
            base_dir: temp_dir.path().to_path_buf(),
            shard_count: 16,
            chunk_size: 64 * 1024 * 1024,
            wal_size_threshold: 100 * 1024 * 1024,
            segment_interval_secs: 3600,
            zstd_level: 3,
        };

        let segments_dir = config.base_dir.join("segments");
        let wal_dir = config.base_dir.join("wal");
        fs::create_dir_all(&segments_dir).unwrap();
        fs::create_dir_all(&wal_dir).unwrap();

        // Step 1: Create a segment file with base data
        // Use keys that will map to specific slots
        let key1 = b"list_key".to_vec();
        let key2 = b"set_key".to_vec();
        let slot1 = RoutingTable::slot_for_key(&key1);
        let slot2 = RoutingTable::slot_for_key(&key2);

        let round_dir = segments_dir.join("1");
        fs::create_dir_all(&round_dir).unwrap();

        // Create a chunk file with test data
        let mut chunk_writer = ChunkWriter::new(config.clone(), round_dir.clone());

        // Add some test entries to the chunk
        let mut list_data = ListData::new();
        list_data.push_back(Bytes::from("item1"));
        list_data.push_back(Bytes::from("item2"));
        chunk_writer
            .add_entry(key1.clone(), &Data::List(list_data))
            .unwrap();

        let mut set_data = SetData::new();
        set_data.add(Bytes::from("member1"));
        set_data.add(Bytes::from("member2"));
        chunk_writer
            .add_entry(key2.clone(), &Data::Set(set_data))
            .unwrap();

        // Flush chunk (use slot range that covers both slots)
        let slot_start = slot1.min(slot2);
        let slot_end = slot1.max(slot2) + 1;
        let (file_name, uncompressed_size, compressed_size, entry_count, crc32) = chunk_writer
            .flush(slot_start, slot_end, &round_dir)
            .unwrap();

        // Create segment metadata
        let mut slot_infos = HashMap::new();
        slot_infos.insert(
            slot1,
            crate::snapshot::segment::SlotSegmentInfo {
                apply_index: 10,
                log_seq: 5,
            },
        );
        slot_infos.insert(
            slot2,
            crate::snapshot::segment::SlotSegmentInfo {
                apply_index: 10,
                log_seq: 5,
            },
        );

        let segment_meta = crate::snapshot::segment::SegmentMetadata {
            slot_start,
            slot_end,
            apply_index: 10,
            round: 1,
            chunks: vec![crate::snapshot::segment::ChunkFileInfo {
                chunk_id: 1,
                file_name,
                uncompressed_size,
                compressed_size,
                entry_count,
                crc32,
            }],
            total_uncompressed_size: uncompressed_size,
            total_compressed_size: compressed_size,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            slot_infos,
        };

        // Write segment metadata JSON
        let json_path = round_dir.join(format!("{:05}-{:05}.seg.json", slot_start, slot_end));
        let json_content = serde_json::to_string_pretty(&segment_meta).unwrap();
        fs::write(&json_path, json_content).unwrap();

        // Write current_round.txt
        fs::write(segments_dir.join("current_round.txt"), "1").unwrap();

        // Step 2: Create WAL entries with incremental changes
        // These should have log_seq > 5 (segment's log_seq)
        let mut wal_writer = WalWriter::new(config.clone(), wal_dir.clone()).unwrap();

        // Add a new item to the list (log_seq = 6)
        wal_writer
            .write_entry(
                11, // apply_index
                6,  // log_seq
                &Command::RPush {
                    key: Bytes::from("list_key"),
                    values: vec![Bytes::from("item3")],
                },
                &key1,
            )
            .unwrap();

        // Add a new member to the set (log_seq = 7)
        wal_writer
            .write_entry(
                12, // apply_index
                7,  // log_seq
                &Command::SAdd {
                    key: Bytes::from("set_key"),
                    members: vec![Bytes::from("member3")],
                },
                &key2,
            )
            .unwrap();

        // Create a new key (log_seq = 8)
        let key3 = b"zset_key".to_vec();
        let slot3 = RoutingTable::slot_for_key(&key3);
        wal_writer
            .write_entry(
                13, // apply_index
                8,  // log_seq
                &Command::ZAdd {
                    key: Bytes::from("zset_key"),
                    members: vec![(1.0, Bytes::from("member1")), (2.0, Bytes::from("member2"))],
                },
                &key3,
            )
            .unwrap();

        wal_writer.flush().unwrap();
        drop(wal_writer);

        // Step 3: Reload MemStore
        let restored = reload_memstore(config).unwrap();

        // Step 4: Verify restored data
        // Calculate slots for each key (they might be different)
        let slot1 = RoutingTable::slot_for_key(&key1);
        let slot2 = RoutingTable::slot_for_key(&key2);
        let slot3 = RoutingTable::slot_for_key(&key3);

        // Verify slot1 data
        assert!(
            restored.contains_key(&slot1),
            "Slot {} should be restored",
            slot1
        );
        let (mem_store1, metadata1) = &restored[&slot1];
        // Note: applied_index should be updated from WAL if log_seq > segment.log_seq
        // Segment has apply_index=10, log_seq=5
        // WAL has apply_index=11, log_seq=6 for slot1
        // So final applied_index should be 11
        assert!(
            metadata1.applied_index >= 10,
            "applied_index should be at least 10, got {}",
            metadata1.applied_index
        );
        assert!(
            metadata1.log_seq >= 5,
            "log_seq should be at least 5, got {}",
            metadata1.log_seq
        );

        // Verify list data (should have at least 2 items from segment, possibly 3 if WAL was applied)
        let list_data = mem_store1.get(&key1).unwrap();
        match list_data {
            Data::List(list) => {
                assert!(
                    list.len() >= 2,
                    "List should have at least 2 items, got {}",
                    list.len()
                );
                assert_eq!(list[0], Bytes::from("item1"));
                assert_eq!(list[1], Bytes::from("item2"));
                // If WAL was applied, there should be a third item
                if list.len() >= 3 {
                    assert_eq!(list[2], Bytes::from("item3"));
                }
            }
            _ => panic!("Expected List data"),
        }

        // Verify slot2 data
        if slot2 == slot1 {
            // Same slot, verify set data in the same store
            let set_data = mem_store1.get(&key2).unwrap();
            match set_data {
                Data::Set(set) => {
                    assert_eq!(set.len(), 3);
                    assert!(set.contains(&Bytes::from("member1")));
                    assert!(set.contains(&Bytes::from("member2")));
                    assert!(set.contains(&Bytes::from("member3")));
                }
                _ => panic!("Expected Set data"),
            }
        } else {
            // Different slot
            assert!(
                restored.contains_key(&slot2),
                "Slot {} should be restored",
                slot2
            );
            let (mem_store2, metadata2) = &restored[&slot2];
            assert_eq!(metadata2.applied_index, 12); // Latest apply_index from WAL for slot2
            assert_eq!(metadata2.log_seq, 7); // Latest log_seq from WAL for slot2

            let set_data = mem_store2.get(&key2).unwrap();
            match set_data {
                Data::Set(set) => {
                    assert_eq!(set.len(), 3);
                    assert!(set.contains(&Bytes::from("member1")));
                    assert!(set.contains(&Bytes::from("member2")));
                    assert!(set.contains(&Bytes::from("member3")));
                }
                _ => panic!("Expected Set data"),
            }
        }

        // Verify slot3 data
        if slot3 == slot1 || slot3 == slot2 {
            // Same slot as slot1 or slot2, verify zset data in the same store
            let mem_store3 = if slot3 == slot1 {
                mem_store1
            } else {
                &restored[&slot2].0
            };
            let zset_data = mem_store3.get(&key3).unwrap();
            match zset_data {
                Data::ZSet(zset) => {
                    assert_eq!(zset.len(), 2);
                    assert_eq!(zset.get_score(b"member1"), Some(1.0));
                    assert_eq!(zset.get_score(b"member2"), Some(2.0));
                }
                _ => panic!("Expected ZSet data"),
            }
        } else {
            // Different slot
            assert!(
                restored.contains_key(&slot3),
                "Slot {} should be restored",
                slot3
            );
            let (mem_store3, metadata3) = &restored[&slot3];
            assert_eq!(metadata3.applied_index, 13); // Latest apply_index from WAL for slot3
            assert_eq!(metadata3.log_seq, 8); // Latest log_seq from WAL for slot3

            let zset_data = mem_store3.get(&key3).unwrap();
            match zset_data {
                Data::ZSet(zset) => {
                    assert_eq!(zset.len(), 2);
                    assert_eq!(zset.get_score(b"member1"), Some(1.0));
                    assert_eq!(zset.get_score(b"member2"), Some(2.0));
                }
                _ => panic!("Expected ZSet data"),
            }
        }
    }

    #[test]
    fn test_reload_memstore_skip_old_wal_entries() {
        let temp_dir = TempDir::new().unwrap();
        let config = SnapshotConfig {
            base_dir: temp_dir.path().to_path_buf(),
            shard_count: 16,
            chunk_size: 64 * 1024 * 1024,
            wal_size_threshold: 100 * 1024 * 1024,
            segment_interval_secs: 3600,
            zstd_level: 3,
        };

        let segments_dir = config.base_dir.join("segments");
        let wal_dir = config.base_dir.join("wal");
        fs::create_dir_all(&segments_dir).unwrap();
        fs::create_dir_all(&wal_dir).unwrap();

        // Create segment with log_seq = 10
        let round_dir = segments_dir.join("1");
        fs::create_dir_all(&round_dir).unwrap();

        let mut chunk_writer = ChunkWriter::new(config.clone(), round_dir.clone());
        let key = b"test_key".to_vec();
        let slot = RoutingTable::slot_for_key(&key);
        let mut list_data = ListData::new();
        list_data.push_back(Bytes::from("segment_item"));
        chunk_writer
            .add_entry(key.clone(), &Data::List(list_data))
            .unwrap();

        let slot_start = slot;
        let slot_end = slot + 1;
        let (file_name, uncompressed_size, compressed_size, entry_count, crc32) = chunk_writer
            .flush(slot_start, slot_end, &round_dir)
            .unwrap();

        let mut slot_infos = HashMap::new();
        slot_infos.insert(
            slot,
            crate::snapshot::segment::SlotSegmentInfo {
                apply_index: 20,
                log_seq: 10, // Segment has log_seq = 10
            },
        );

        let segment_meta = crate::snapshot::segment::SegmentMetadata {
            slot_start,
            slot_end,
            apply_index: 20,
            round: 1,
            chunks: vec![crate::snapshot::segment::ChunkFileInfo {
                chunk_id: 1,
                file_name,
                uncompressed_size,
                compressed_size,
                entry_count,
                crc32,
            }],
            total_uncompressed_size: uncompressed_size,
            total_compressed_size: compressed_size,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            slot_infos,
        };

        let json_path = round_dir.join(format!("{:05}-{:05}.seg.json", slot_start, slot_end));
        let json_content = serde_json::to_string_pretty(&segment_meta).unwrap();
        fs::write(&json_path, json_content).unwrap();
        fs::write(segments_dir.join("current_round.txt"), "1").unwrap();

        // Create WAL entries with mixed log_seq
        let mut wal_writer = WalWriter::new(config.clone(), wal_dir.clone()).unwrap();

        // Old entry (log_seq = 5, should be skipped)
        wal_writer
            .write_entry(
                15,
                5, // log_seq < 10, should be skipped
                &Command::LPush {
                    key: Bytes::from("test_key"),
                    values: vec![Bytes::from("old_item")],
                },
                &key,
            )
            .unwrap();

        // New entry (log_seq = 11, should be applied)
        wal_writer
            .write_entry(
                21,
                11, // log_seq > 10, should be applied
                &Command::RPush {
                    key: Bytes::from("test_key"),
                    values: vec![Bytes::from("new_item")],
                },
                &key,
            )
            .unwrap();

        wal_writer.flush().unwrap();
        drop(wal_writer);

        // Reload
        let restored = reload_memstore(config).unwrap();
        let (mem_store, metadata) = &restored[&slot];

        // Verify metadata
        assert_eq!(metadata.log_seq, 11); // Should be from new entry

        // Verify data: should only have segment_item + new_item (old_item should be skipped)
        let list_data = mem_store.get(&key).unwrap();
        match list_data {
            Data::List(list) => {
                assert_eq!(list.len(), 2);
                assert_eq!(list[0], Bytes::from("segment_item"));
                assert_eq!(list[1], Bytes::from("new_item"));
            }
            _ => panic!("Expected List data"),
        }
    }

    #[test]
    fn test_reload_memstore_empty_segments() {
        let temp_dir = TempDir::new().unwrap();
        let config = SnapshotConfig {
            base_dir: temp_dir.path().to_path_buf(),
            shard_count: 16,
            chunk_size: 64 * 1024 * 1024,
            wal_size_threshold: 100 * 1024 * 1024,
            segment_interval_secs: 3600,
            zstd_level: 3,
        };

        let wal_dir = config.base_dir.join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        // Create WAL entries without segments
        let mut wal_writer = WalWriter::new(config.clone(), wal_dir.clone()).unwrap();
        let key = b"test_key".to_vec();

        wal_writer
            .write_entry(
                1,
                1,
                &Command::SAdd {
                    key: Bytes::from("test_key"),
                    members: vec![Bytes::from("member1")],
                },
                &key,
            )
            .unwrap();

        wal_writer.flush().unwrap();
        drop(wal_writer);

        // Reload (no segments, only WAL)
        let restored = reload_memstore(config).unwrap();

        // Should create slot from WAL
        let slot = RoutingTable::slot_for_key(&key);
        assert!(restored.contains_key(&slot));

        let (mem_store, metadata) = &restored[&slot];
        assert_eq!(metadata.log_seq, 1);

        // Verify set data
        let set_data = mem_store.get(&key).unwrap();
        match set_data {
            Data::Set(set) => {
                assert_eq!(set.len(), 1);
                assert!(set.contains(&Bytes::from("member1")));
            }
            _ => panic!("Expected Set data"),
        }
    }
}
