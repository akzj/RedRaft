//! Snapshot Store implementation for HybridStore

use crate::rocksdb::key_prefix;
use crate::rocksdb::{parse_hash_field_key, parse_hash_meta_key, parse_string_key};
use crate::store::{HybridStore, LockedSlotStore};
use crate::traits::{SnapshotStore, SnapshotStoreEntry, StoreError};
use async_trait::async_trait;
use rr_core::routing::RoutingTable;
use rr_core::shard::ShardId;
use tracing::error;

/// Wrapper that ensures merge_cow is called on drop for multiple slot stores
///
/// This wrapper stores references to slot_stores and will acquire write locks
/// in its Drop implementation to call merge_cow() for each. This ensures COW changes
/// are merged back to the base stores even if the function returns early or panics.
struct MergeCowGuard {
    slot_stores: Vec<LockedSlotStore>,
}

impl MergeCowGuard {
    /// Create a new guard that will call merge_cow on drop for all provided slot stores
    fn new(slot_stores: Vec<LockedSlotStore>) -> Self {
        Self { slot_stores }
    }
}

impl Drop for MergeCowGuard {
    fn drop(&mut self) {
        // Acquire write lock and call merge_cow for each slot store
        // This will be called even if the function returns early or panics
        for slot_store in &self.slot_stores {
            let mut store_guard = slot_store.write();
            store_guard.memory_mut().merge_cow();
        }
    }
}

#[async_trait]
impl SnapshotStore for HybridStore {
    /// Flush data to disk for snapshot creation
    ///
    /// Only flushes WAL and RocksDB to ensure all writes are persisted.
    /// Data is already stored on disk (WAL + Segment), so no need to generate or return data.
    ///
    /// # Arguments
    /// - `shard_id`: Shard ID (deprecated, kept for interface compatibility)
    /// - `channel`: Channel to send snapshot entries
    /// - `slot_range`: Optional slot range (slot_begin, slot_end) to filter keys.
    ///   If Some((begin, end)), only keys with slot ∈ [begin, end) are included.
    ///   If None, all keys are included.
    async fn create_snapshot(
        &self,
        shard_id: &ShardId,
        channel: std::sync::mpsc::SyncSender<SnapshotStoreEntry>,
        slot_range: Option<(u32, u32)>,
    ) -> anyhow::Result<u64> {
        // Determine which slots to process
        // If slot_range is provided, process all slots in that range
        // Otherwise, process all available slots
        let slots_to_process: Vec<u32> = {
            let slots = self.slots.read();
            if let Some((slot_start, slot_end)) = slot_range {
                // Process all slots in the range [slot_start, slot_end)
                (slot_start..slot_end)
                    .filter(|slot| slots.contains_key(slot))
                    .collect()
            } else {
                // Process all available slots
                slots.keys().copied().collect()
            }
        };

        if slots_to_process.is_empty() {
            return Err(anyhow::anyhow!(
                "No slot stores available for shard {} (slot_range: {:?})",
                shard_id,
                slot_range
            ));
        }

        // Collect all slot stores that need merge_cow
        let slot_stores: Vec<LockedSlotStore> = {
            let slots = self.slots.read();
            slots_to_process
                .iter()
                .filter_map(|slot| slots.get(slot).cloned())
                .collect()
        };

        if slot_stores.is_empty() {
            return Err(anyhow::anyhow!(
                "No slot stores found for slots {:?} (shard {})",
                slots_to_process,
                shard_id
            ));
        }

        // Clone DB - use default column family
        let db = self.rocksdb.db.clone();

        // notify the main thread that the snapshot object is created
        // This allows load_snapshot to wait synchronously for snapshot object creation
        // Use std::sync::mpsc instead of oneshot to allow cloning sender for panic handling
        let (tx, rx) = std::sync::mpsc::channel();

        // Move slot_range and slot_stores into closure
        let slot_range = slot_range;
        let slot_stores_clone = slot_stores.clone();

        // Clone tx for use outside closure (for panic handling)
        let tx_for_panic = tx.clone();
        let _handle = std::thread::spawn(move || {
            // Use catch_unwind to handle panics gracefully and prevent main thread from blocking forever
            // Wrap everything in a closure that ensures tx.send() is always called
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                // Helper function to send error through channel
                let send_error = |err: StoreError| {
                    let _ = channel.send(SnapshotStoreEntry::Error(err));
                };

                // Create snapshot inside the closure (after db is moved)
                // Note: db.snapshot() returns a snapshot directly, not a Result
                let snapshot = db.snapshot();

                // Get default Column Family handle
                let cf_handler = match db.cf_handle("default") {
                    Some(handler) => handler,
                    None => {
                        let err =
                            StoreError::Internal("Default column family not found".to_string());
                        send_error(err.clone());
                        // Signal error to unblock waiting thread (must send before return)
                        let _ = tx.send(Err(err));
                        return;
                    }
                };

                // Signal that snapshot object is created (RocksDB snapshot + Memory COW will be created next)
                // This allows load_snapshot to wait synchronously for snapshot object creation
                // Create Memory COW snapshots for all slot stores BEFORE signaling snapshot object is ready
                // This ensures state consistency - snapshot object is fully created before signaling
                //
                // IMPORTANT: Hold write lock only for creating snapshot, then release immediately
                // to avoid blocking other operations. merge_cow will be called automatically on drop
                // of merge_cow_guard, even if there's an early return or panic.
                let merge_cow_guard = MergeCowGuard::new(slot_stores_clone.clone());

                // Create memory snapshots for all slot stores
                let memory_snapshots: Vec<_> = slot_stores_clone
                    .iter()
                    .map(|slot_store| {
                        let mut store_guard = slot_store.write();
                        store_guard.memory_mut().make_snapshot()
                    })
                    .collect(); // Write locks are released here

                // Signal that snapshot object is fully created (RocksDB snapshot + Memory COW)
                // This allows load_snapshot to wait synchronously for snapshot object creation
                // Note: apply_index is no longer used, return 0 for compatibility
                let _ = tx.send(Ok(0));

                // Helper function to check if slot is in range
                let slot_in_range = |slot: u32| -> bool {
                    if let Some((slot_begin, slot_end)) = slot_range {
                        slot >= slot_begin && slot < slot_end
                    } else {
                        true // No filtering if slot_range is None
                    }
                };

                // Iterate RocksDB entries (only String and Hash are stored in RocksDB)
                let iter = snapshot.iterator_cf(cf_handler, rocksdb::IteratorMode::Start);
                for item in iter {
                    let (key, value) = match item {
                        Ok(kv) => kv,
                        Err(e) => {
                            let err =
                                StoreError::Internal(format!("RocksDB iteration error: {}", e));
                            send_error(err.clone());
                            return;
                        }
                    };

                    // Skip apply_index key (format: @:apply_index, no slot prefix)
                    if key.len() >= 2 && key[0] == key_prefix::APPLY_INDEX && key[1] == b':' {
                        continue;
                    }

                    // Try to parse with new format (with slot prefix)
                    // Format: {slot(4字节)}:s:{key} or {slot(4字节)}:h:{key}:{field}
                    if let Some((slot, original_key)) = parse_string_key(&key) {
                        // Filter by slot range if specified
                        if !slot_in_range(slot) {
                            continue;
                        }
                        let _ = channel.send(SnapshotStoreEntry::String(
                            bytes::Bytes::copy_from_slice(original_key),
                            bytes::Bytes::copy_from_slice(&value),
                        ));
                    } else if let Some((slot, hash_key, field)) = parse_hash_field_key(&key) {
                        // Filter by slot range if specified
                        if !slot_in_range(slot) {
                            continue;
                        }
                        if let Err(e) = channel.send(SnapshotStoreEntry::Hash(
                            bytes::Bytes::copy_from_slice(hash_key),
                            bytes::Bytes::copy_from_slice(field),
                            bytes::Bytes::copy_from_slice(&value),
                        )) {
                            let err =
                                StoreError::Internal(format!("Failed to send Hash entry: {}", e));
                            send_error(err.clone());
                            return;
                        }
                    } else if parse_hash_meta_key(&key).is_some() {
                        // Skip hash metadata (H:{key}) as we only need fields
                        continue;
                    } else {
                        // Fallback: Try old format (backward compatibility)
                        // Old format: s:{key} or h:{key}:{field}
                        if key.len() >= 2 && key[0] == key_prefix::STRING && key[1] == b':' {
                            let original_key = &key[2..];
                            let slot = RoutingTable::slot_for_key(original_key);
                            // Filter by slot range if specified
                            if !slot_in_range(slot) {
                                continue;
                            }
                            let _ = channel.send(SnapshotStoreEntry::String(
                                bytes::Bytes::copy_from_slice(original_key),
                                bytes::Bytes::copy_from_slice(&value),
                            ));
                        } else if key.len() >= 2 && key[0] == key_prefix::HASH && key[1] == b':' {
                            // Hash field: h:{key}:{field} -> value
                            let key_part = &key[2..];
                            if let Some(colon_pos) = key_part.iter().position(|&b| b == b':') {
                                let hash_key = &key_part[..colon_pos];
                                let slot = RoutingTable::slot_for_key(hash_key);
                                // Filter by slot range if specified
                                if !slot_in_range(slot) {
                                    continue;
                                }
                                let field = &key_part[colon_pos + 1..];
                                if let Err(e) = channel.send(SnapshotStoreEntry::Hash(
                                    bytes::Bytes::copy_from_slice(hash_key),
                                    bytes::Bytes::copy_from_slice(field),
                                    bytes::Bytes::copy_from_slice(&value),
                                )) {
                                    let err = StoreError::Internal(format!(
                                        "Failed to send Hash entry: {}",
                                        e
                                    ));
                                    send_error(err.clone());
                                    return;
                                }
                            }
                        }
                    }
                }

                // Helper function to check if key is in slot range (for memory store)
                let key_in_range = |key: &[u8]| -> bool {
                    if let Some((slot_begin, slot_end)) = slot_range {
                        let slot = RoutingTable::slot_for_key(key);
                        slot >= slot_begin && slot < slot_end
                    } else {
                        true // No filtering if slot_range is None
                    }
                };

                // Iterate Memory store entries for all slot stores
                // Note: memory_snapshots were already created above before signaling
                // Each slot store has its own memory snapshot
                for memory_snapshot in &memory_snapshots {
                    let memory_data = memory_snapshot.read();
                    for (key, data_cow) in memory_data.iter() {
                        // Filter by slot range if specified
                        if !key_in_range(key) {
                            continue;
                        }
                        match data_cow {
                            crate::memory::DataCow::List(list) => {
                                // Send all list elements
                                for element in list.iter() {
                                    if let Err(e) = channel.send(SnapshotStoreEntry::List(
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
                                    if let Err(e) = channel.send(SnapshotStoreEntry::Set(
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
                                    if let Err(e) = channel.send(SnapshotStoreEntry::ZSet(
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
                                if let Err(e) = channel.send(SnapshotStoreEntry::Bitmap(
                                    bytes::Bytes::copy_from_slice(key),
                                    bytes::Bytes::copy_from_slice(bitmap),
                                )) {
                                    let err = StoreError::Internal(format!(
                                        "Failed to send Bitmap entry: {}",
                                        e
                                    ));
                                    send_error(err.clone());
                                    return;
                                }
                            }
                        }
                    }
                    drop(memory_data);
                }

                // merge_cow_guard will be dropped here automatically, which will call merge_cow()
                // This ensures merge_cow is called even if there was an early return above
                drop(merge_cow_guard);

                // Send completion signal
                let _ = channel.send(SnapshotStoreEntry::Completed);
            }));

            // Handle panic: send error and signal to unblock main thread
            if let Err(panic_info) = result {
                let error_msg = if let Some(s) = panic_info.downcast_ref::<String>() {
                    format!("Thread panicked: {}", s)
                } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                    format!("Thread panicked: {}", s)
                } else {
                    "Thread panicked with unknown error".to_string()
                };
                let _ = channel.send(SnapshotStoreEntry::Error(StoreError::Internal(
                    error_msg.clone(),
                )));
                error!("Snapshot creation thread panicked: {}", error_msg);
            }

            // Always signal to unblock main thread, even on panic
            // This prevents main thread from blocking forever if thread panics
            // Use cloned sender that wasn't moved into the closure
            let _ = tx_for_panic.send(Err(StoreError::Internal(
                "Snapshot creation thread panicked".to_string(),
            )));
        });

        // Wait for signal (blocking receive)
        // This will unblock when thread sends signal, even on panic

        let apply_index = rx
            .recv()
            .map_err(|e| anyhow::anyhow!("Failed to receive snapshot creation signal: {}", e))?
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to get apply index from snapshot creation signal: {}",
                    e
                )
            })?;
        Ok(apply_index)
    }

    fn restore_from_snapshot(&self, _snapshot: &[u8]) -> anyhow::Result<()> {
        // TODO: Implement snapshot restoration
        Ok(())
    }

    fn create_split_snapshot(
        &self,
        _slot_start: u32,
        _slot_end: u32,
        _total_slots: u32,
    ) -> anyhow::Result<Vec<u8>> {
        // TODO: Implement split snapshot
        Ok(Vec::new())
    }

    fn merge_from_snapshot(&self, _snapshot: &[u8]) -> anyhow::Result<usize> {
        // TODO: Implement merge snapshot
        Err(anyhow::anyhow!("Not implemented"))
    }

    fn delete_keys_in_slot_range(
        &self,
        _slot_start: u32,
        _slot_end: u32,
        _total_slots: u32,
    ) -> anyhow::Result<usize> {
        // TODO: Implement delete keys in slot range
        Err(anyhow::anyhow!("Not implemented"))
    }
}
