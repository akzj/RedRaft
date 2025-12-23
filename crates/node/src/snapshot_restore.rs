//! Snapshot restore module
//!
//! Handles snapshot restoration from compressed chunks, including decompression,
//! deserialization, and applying entries to the store.

use std::sync::Arc;
use tracing::{error, info, warn};

use storage::store::HybridStore;
use storage::traits::{
    ApplyResult as StoreApplyResult, HashStore, KeyStore, ListStore, SetStore, SnapshotStoreEntry,
    StringStore,
};

/// Restore snapshot from compressed chunks in blocking context
/// This handles CPU-intensive operations: decompression and deserialization
pub async fn restore_snapshot_from_chunks(
    store: Arc<HybridStore>,
    blocking_rx: std::sync::mpsc::Receiver<Result<(Vec<u8>, bool), String>>,
) -> anyhow::Result<Result<u64, String>, tokio::task::JoinError> {
    tokio::task::spawn_blocking(move || {
        let mut entry_count = 0u64;
        let mut chunk_count = 0u32;

        // Process compressed chunks from blocking channel
        loop {
            let (chunk_data, is_last) = match blocking_rx.recv() {
                Ok(Ok((data, last))) => (data, last),
                Ok(Err(e)) => {
                    return Err(format!("Error receiving chunk: {}", e));
                }
                Err(_) => {
                    // Channel closed
                    break;
                }
            };

            chunk_count += 1;

            // Decompress chunk (CPU-intensive operation)
            // chunk_data is Vec<u8>, use as_slice() for decode_all
            let decompressed = match zstd::decode_all(&chunk_data[..]) {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to decompress chunk {}: {}", chunk_count, e);
                    return Err(format!("Failed to decompress chunk: {}", e));
                }
            };

            // Parse and apply entries from decompressed data
            match process_decompressed_chunk(&store, &decompressed, &mut entry_count) {
                Ok(should_return) => {
                    if should_return {
                        return Ok(entry_count);
                    }
                }
                Err(e) => return Err(e.to_string()),
            }

            if is_last {
                info!("Received last chunk, snapshot restore finished");
                break;
            }
        }

        Ok(entry_count)
    })
    .await
}

/// Process decompressed chunk data and apply entries to store
/// Returns true if we should return early (e.g., Completed or Error)
fn process_decompressed_chunk(
    store: &Arc<HybridStore>,
    decompressed: &[u8],
    entry_count: &mut u64,
) -> anyhow::Result<bool> {
    let mut cursor = 0;
    while cursor < decompressed.len() {
        match bincode::serde::decode_from_slice::<SnapshotStoreEntry, _>(
            &decompressed[cursor..],
            bincode::config::standard(),
        ) {
            Ok((entry, bytes_read)) => {
                // Apply entry directly to store
                match apply_snapshot_entry(store, entry) {
                    Ok(should_return) => {
                        if should_return {
                            return Ok(true);
                        }
                        *entry_count += 1;
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to apply snapshot entry: {}", e));
                    }
                }
                cursor += bytes_read;
            }
            Err(e) => {
                error!(
                    "Failed to deserialize snapshot entry at offset {}: {}",
                    cursor, e
                );
                return Err(anyhow::anyhow!("Failed to deserialize entry: {}", e));
            }
        }
    }

    Ok(false)
}

/// Apply a single snapshot entry to store
/// Returns true if we should return early (e.g., Completed or Error)
fn apply_snapshot_entry(
    store: &Arc<HybridStore>,
    entry: SnapshotStoreEntry,
) -> anyhow::Result<bool> {
    match entry {
        SnapshotStoreEntry::Completed => {
            info!("Received completion signal, snapshot restore finished");
            return Ok(true);
        }
        SnapshotStoreEntry::Error(err) => {
            return Err(anyhow::anyhow!("Snapshot restore error: {}", err));
        }
        SnapshotStoreEntry::String(key, value) => {
            store
                .set(&key, value)
                .map_err(|e| anyhow::anyhow!("Failed to restore String entry: {}", e))?;
        }
        SnapshotStoreEntry::Hash(key, field, value) => {
            store
                .hset(&key, &field, value)
                .map_err(|e| anyhow::anyhow!("Failed to restore Hash entry: {}", e))?;
        }
        SnapshotStoreEntry::List(key, element) => {
            store
                .rpush(&key, vec![element])
                .map_err(|e| anyhow::anyhow!("Failed to restore List entry: {}", e))?;
        }
        SnapshotStoreEntry::Set(key, member) => {
            store
                .sadd(&key, vec![member])
                .map_err(|e| anyhow::anyhow!("Failed to restore Set entry: {}", e))?;
        }
        SnapshotStoreEntry::ZSet(_key, _score, _member) => {
            // ZSetStore is not implemented for HybridStore yet
            // For now, skip ZSet restoration (TODO: implement ZSetStore trait)
            warn!("ZSet restoration not yet implemented, skipping entry");
        }
        SnapshotStoreEntry::Bitmap(key, bitmap) => {
            // Bitmap is stored as bytes, need to set bits
            // For now, we'll store it as a string value
            // TODO: Implement proper bitmap restoration
            store
                .set(&key, bitmap)
                .map_err(|e| anyhow::anyhow!("Failed to restore Bitmap entry: {}", e))?;
        }
    }

    Ok(false)
}

/// Handle snapshot restore result and update state
pub fn handle_snapshot_restore_result(
    restore_result: Result<Result<u64, String>, tokio::task::JoinError>,
    apply_index: Arc<std::sync::atomic::AtomicU64>,
    term_atomic: Arc<std::sync::atomic::AtomicU64>,
    pending_requests: crate::pending_requests::PendingRequests,
    from: raft::RaftId,
    index: u64,
    term: u64,
    oneshot: tokio::sync::oneshot::Sender<raft::SnapshotResult<()>>,
) {
    match restore_result {
        Ok(Ok(entry_count)) => {
            // Update apply_index and term to snapshot values
            apply_index.store(index, std::sync::atomic::Ordering::SeqCst);
            term_atomic.store(term, std::sync::atomic::Ordering::SeqCst);

            // Clear old apply result cache
            pending_requests.clear_apply_results();

            info!(
                "Snapshot installed successfully for {} at index {}, {} entries restored",
                from, index, entry_count
            );

            let _ = oneshot.send(Ok(()));
        }
        Ok(Err(e)) => {
            warn!(
                "Failed to install snapshot for {} at index {}: {}",
                from, index, e
            );
            let _ = oneshot.send(Err(raft::SnapshotError::DataCorrupted(Arc::new(
                anyhow::anyhow!("Failed to restore snapshot: {}", e),
            ))));
        }
        Err(join_err) => {
            warn!(
                "Failed to install snapshot for {} at index {}: Task join error: {:?}",
                from, index, join_err
            );
            let _ = oneshot.send(Err(raft::SnapshotError::DataCorrupted(Arc::new(
                anyhow::anyhow!("Task join error: {:?}", join_err),
            ))));
        }
    }
}
