//! Snapshot Store implementation for HybridStore

use crate::shard::ShardId;
use crate::store::HybridStore;
use crate::traits::{SnapshotStore, SnapshotStoreEntry, StoreError};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
impl SnapshotStore for HybridStore {
    /// Flush data to disk for snapshot creation
    ///
    /// Only flushes WAL and RocksDB to ensure all writes are persisted.
    /// Data is already stored on disk (WAL + Segment), so no need to generate or return data.
    async fn create_snapshot(
        &self,
        shard_id: &ShardId,
        channel: tokio::sync::mpsc::Sender<SnapshotStoreEntry>,
    ) -> Result<()> {
        // Get shard (must be done before moving into closure)
        let shard = {
            let shards = self.shards.read();
            shards
                .get(shard_id)
                .cloned()
                .ok_or(StoreError::ShardNotFound(shard_id.clone()))?
        };

        // Clone DB and prepare CF name for the closure
        let db = self.rocksdb.db.clone();
        let cf_name = format!("shard_{}", shard_id);

        // notify the main thread that the snapshot object is created
        // This allows load_snapshot to wait synchronously for snapshot object creation
        let (tx, rx) = tokio::sync::oneshot::channel();

        let _ = tokio::task::spawn_blocking(move || {
            // Helper function to send error through channel
            let send_error = |err: StoreError| {
                let _ = channel.blocking_send(SnapshotStoreEntry::Error(err));
            };

            // Create snapshot inside the closure (after db is moved)
            // Note: db.snapshot() returns a snapshot directly, not a Result
            let snapshot = db.snapshot();

            // Get Column Family handle from db (CF handle is valid as long as db exists)
            let cf_handler = match db.cf_handle(&cf_name) {
                Some(handler) => handler,
                None => {
                    let err = StoreError::Internal(format!("Column Family {} not found", cf_name));
                    send_error(err);
                    // Signal error to unblock waiting thread (must send before return)
                    let _ = tx.send(());
                    return;
                }
            };

            // Signal that snapshot object is created (RocksDB snapshot + Memory COW will be created next)
            // This allows load_snapshot to wait synchronously for snapshot object creation
            // Create Memory COW snapshot BEFORE signaling snapshot object is ready
            // This ensures state consistency - snapshot object is fully created before signaling
            let mut shard_guard = shard.write();
            let memory_snapshot = shard_guard.memory_mut().store.make_snapshot();
            drop(shard_guard); // Release write lock immediately

            // Signal that snapshot object is fully created (RocksDB snapshot + Memory COW)
            // This allows load_snapshot to wait synchronously for snapshot object creation
            let _ = tx.send(());

            // Iterate RocksDB entries (String and Hash)
            let iter = snapshot.iterator_cf(cf_handler, rocksdb::IteratorMode::Start);
            for item in iter {
                let (key, value) = match item {
                    Ok(kv) => kv,
                    Err(e) => {
                        let err = StoreError::Internal(format!("RocksDB iteration error: {}", e));
                        send_error(err.clone());
                        return;
                    }
                };

                // Skip apply_index key
                if key.starts_with(b"@:") {
                    continue;
                }

                // Parse key to determine type
                if key.starts_with(b"s:") {
                    // String: s:{key} -> value
                    let original_key = &key[2..];
                    let _ = channel.send(SnapshotStoreEntry::String(
                        bytes::Bytes::copy_from_slice(original_key),
                        bytes::Bytes::copy_from_slice(&value),
                    ));
                } else if key.starts_with(b"h:") {
                    // Hash field: h:{key}:{field} -> value
                    // Find the second colon
                    let key_part = &key[2..];
                    if let Some(colon_pos) = key_part.iter().position(|&b| b == b':') {
                        let hash_key = &key_part[..colon_pos];
                        let field = &key_part[colon_pos + 1..];
                        if let Err(e) = channel.blocking_send(SnapshotStoreEntry::Hash(
                            bytes::Bytes::copy_from_slice(hash_key),
                            bytes::Bytes::copy_from_slice(field),
                            bytes::Bytes::copy_from_slice(&value),
                        )) {
                            let err =
                                StoreError::Internal(format!("Failed to send Hash entry: {}", e));
                            send_error(err.clone());
                            return;
                        }
                    }
                }
                // Skip hash metadata (H:{key}) as we only need fields
            }

            // Iterate Memory store entries (List, Set, ZSet, Bitmap)
            // Note: memory_snapshot was already created above before signaling
            let memory_data = memory_snapshot.read();
            for (key, data_cow) in memory_data.iter() {
                match data_cow {
                    crate::memory::DataCow::List(list) => {
                        // Send all list elements
                        for element in list.iter() {
                            if let Err(e) = channel.blocking_send(SnapshotStoreEntry::List(
                                bytes::Bytes::copy_from_slice(key),
                                element.clone(),
                            )) {
                                let err = StoreError::Internal(format!(
                                    "Failed to send List entry: {}",
                                    e
                                ));
                                send_error(err.clone());
                                return;
                            }
                        }
                    }
                    crate::memory::DataCow::Set(set) => {
                        // Send all set members
                        for member in set.members() {
                            if let Err(e) = channel.blocking_send(SnapshotStoreEntry::Set(
                                bytes::Bytes::copy_from_slice(key),
                                member.clone(),
                            )) {
                                let err = StoreError::Internal(format!(
                                    "Failed to send Set entry: {}",
                                    e
                                ));
                                send_error(err.clone());
                                return;
                            }
                        }
                    }
                    crate::memory::DataCow::ZSet(zset) => {
                        // Send all zset elements with scores
                        // Use range_by_score to get all (member, score) pairs
                        let members_with_scores =
                            zset.range_by_score(f64::NEG_INFINITY, f64::INFINITY);
                        for (member, score) in members_with_scores {
                            if let Err(e) = channel.blocking_send(SnapshotStoreEntry::ZSet(
                                bytes::Bytes::copy_from_slice(key),
                                score,
                                member,
                            )) {
                                let err = StoreError::Internal(format!(
                                    "Failed to send ZSet entry: {}",
                                    e
                                ));
                                send_error(err.clone());
                                return;
                            }
                        }
                    }
                    crate::memory::DataCow::Bitmap(bitmap) => {
                        // Send bitmap data (BitmapData is Vec<u8>)
                        if let Err(e) = channel.blocking_send(SnapshotStoreEntry::Bitmap(
                            bytes::Bytes::copy_from_slice(key),
                            bytes::Bytes::copy_from_slice(bitmap),
                        )) {
                            let err =
                                StoreError::Internal(format!("Failed to send Bitmap entry: {}", e));
                            send_error(err.clone());
                            return;
                        }
                    }
                }
            }
            drop(memory_data);

            // Send completion signal
            let _ = channel.blocking_send(SnapshotStoreEntry::Completed);

            ()
        })
        .await;

        rx.blocking_recv()?;

        Ok(())
    }

    fn restore_from_snapshot(&self, _snapshot: &[u8]) -> Result<(), String> {
        // TODO: Implement snapshot restoration
        Ok(())
    }

    fn create_split_snapshot(
        &self,
        _slot_start: u32,
        _slot_end: u32,
        _total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        // TODO: Implement split snapshot
        Ok(Vec::new())
    }

    fn merge_from_snapshot(&self, _snapshot: &[u8]) -> Result<usize, String> {
        // TODO: Implement merge snapshot
        Ok(0)
    }

    fn delete_keys_in_slot_range(
        &self,
        _slot_start: u32,
        _slot_end: u32,
        _total_slots: u32,
    ) -> usize {
        // TODO: Implement delete keys in slot range
        0
    }
}

