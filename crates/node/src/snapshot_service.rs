//! Snapshot Service implementation
//!
//! Implements the gRPC service for pulling snapshot data from Leader to Follower.
//! Supports streaming snapshot chunks while generation is in progress.

use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

use crate::snapshot_transfer::{
    read_chunk_from_file, wait_for_chunk, ChunkIndex, ChunkMetadata, SnapshotTransferManager,
    SnapshotTransferState,
};
use proto::snapshot_service::{
    snapshot_service_server::SnapshotService, GetTransferProgressRequest,
    GetTransferProgressResponse, PullSnapshotDataRequest, PullSnapshotDataResponse,
};

/// Snapshot Service implementation
pub struct SnapshotServiceImpl {
    /// Snapshot transfer manager
    transfer_manager: Arc<SnapshotTransferManager>,
}

impl SnapshotServiceImpl {
    pub fn new(transfer_manager: Arc<SnapshotTransferManager>) -> Self {
        Self { transfer_manager }
    }
}

#[tonic::async_trait]
impl SnapshotService for SnapshotServiceImpl {
    type PullSnapshotDataStream =
        tokio_stream::wrappers::ReceiverStream<Result<PullSnapshotDataResponse, Status>>;

    async fn pull_snapshot_data(
        &self,
        request: Request<PullSnapshotDataRequest>,
    ) -> Result<Response<Self::PullSnapshotDataStream>, Status> {
        let req = request.into_inner();
        let transfer_id = req.transfer_id.clone();
        let raft_group_id = req.raft_group_id.clone();
        let offset = req.offset;
        let max_chunk_size = req.max_chunk_size;

        info!(
            "PullSnapshotData request: transfer_id={}, raft_group_id={}, offset={}, max_chunk_size={}",
            transfer_id, raft_group_id, offset, max_chunk_size
        );

        // Get transfer state
        let state = self
            .transfer_manager
            .get_transfer_state(&transfer_id)
            .ok_or_else(|| Status::not_found(format!("Transfer {} not found", transfer_id)))?;

        // Check if transfer is failed
        if state.is_failed() {
            return Err(Status::failed_precondition(format!(
                "Transfer {} has failed: {}",
                transfer_id,
                state.error.as_deref().unwrap_or("Unknown error")
            )));
        }

        let snapshot_path = state.snapshot_path.clone();
        let chunk_index = state.chunk_index.clone();

        // Create channel for streaming responses
        // Buffer size: 16 chunks (to balance memory usage and throughput)
        // Each chunk is typically 1-10MB compressed, so 16 chunks = ~16-160MB buffer
        let (tx, rx) = tokio::sync::mpsc::channel(16);

        // Spawn task to stream chunks
        let transfer_manager_clone = self.transfer_manager.clone();
        let snapshot_path_clone = snapshot_path.clone();
        let transfer_id_clone = transfer_id.clone();
        tokio::spawn(async move {
            let result = stream_snapshot_chunks(
                transfer_manager_clone,
                snapshot_path_clone,
                chunk_index,
                transfer_id_clone.clone(),
                offset,
                max_chunk_size,
                tx,
            )
            .await;

            if let Err(e) = result {
                error!(
                    "Error streaming snapshot chunks for transfer {}: {}",
                    transfer_id_clone, e
                );
                // Note: tx is moved into stream_snapshot_chunks, so we can't send error here
                // Errors are handled inside stream_snapshot_chunks
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn get_transfer_progress(
        &self,
        request: Request<GetTransferProgressRequest>,
    ) -> Result<Response<GetTransferProgressResponse>, Status> {
        let req = request.into_inner();
        let transfer_id = req.transfer_id;

        let state = self
            .transfer_manager
            .get_transfer_state(&transfer_id)
            .ok_or_else(|| Status::not_found(format!("Transfer {} not found", transfer_id)))?;

        use proto::node::TransferStatus;

        let index = state.chunk_index.read();
        let is_complete = state.is_completed();
        let total_bytes = if state.total_size > 0 {
            state.total_size
        } else {
            index.total_compressed_size
        };
        let error_message = state
            .error
            .clone()
            .unwrap_or_else(|| index.error.clone().unwrap_or_default());

        let (status, received_bytes, received_chunks, total_chunks) = (
            if is_complete {
                TransferStatus::Completed as i32
            } else if state.is_failed() {
                TransferStatus::Failed as i32
            } else {
                TransferStatus::InProgress as i32
            },
            index.total_compressed_size,
            index.generated_chunks,
            index.chunks.len() as u32,
        );
        drop(index);

        Ok(Response::new(GetTransferProgressResponse {
            status,
            received_bytes,
            total_bytes,
            received_chunks,
            total_chunks,
            error_message,
        }))
    }
}

/// Stream snapshot chunks to the client
async fn stream_snapshot_chunks(
    _transfer_manager: Arc<SnapshotTransferManager>,
    snapshot_path: std::path::PathBuf,
    chunk_index: Arc<RwLock<ChunkIndex>>,
    transfer_id: String,
    start_offset: u64,
    _max_chunk_size: u32,
    tx: tokio::sync::mpsc::Sender<Result<PullSnapshotDataResponse, Status>>,
) -> Result<(), String> {
    // Find the starting chunk based on offset
    // We need to wait for chunks to be generated if still generating
    let (start_chunk_index, total_size, is_complete) = {
        let index_guard = chunk_index.read();
        let mut start_chunk_idx = 0u32;

        // Find which chunk contains the start_offset
        // If chunks are not yet generated, we'll start from chunk 0
        for (idx, chunk) in index_guard.chunks.iter().enumerate() {
            // Calculate chunk end offset: file_offset + header_len (4) + header_size + compressed_size
            let header_size =
                match bincode::serde::encode_to_vec(chunk, bincode::config::standard()) {
                    Ok(bytes) => bytes.len() as u64,
                    Err(_) => {
                        // If serialization fails, use a rough estimate
                        // ChunkMetadata is small, typically < 100 bytes
                        100
                    }
                };
            let chunk_end_offset =
                chunk.file_offset + 4 + header_size + chunk.compressed_size as u64;

            if chunk.file_offset <= start_offset && start_offset < chunk_end_offset {
                start_chunk_idx = idx as u32;
                break;
            }
            if chunk.file_offset > start_offset {
                // We've passed the start_offset, use previous chunk
                if idx > 0 {
                    start_chunk_idx = (idx - 1) as u32;
                }
                break;
            }
        }

        (
            start_chunk_idx,
            index_guard.total_compressed_size,
            index_guard.is_complete,
        )
    };

    info!(
        "Starting to stream chunks for transfer {} from chunk {} (offset {})",
        transfer_id, start_chunk_index, start_offset
    );

    // Stream chunks starting from start_chunk_index
    let mut current_chunk_index = start_chunk_index;
    let is_complete_local = is_complete;

    loop {
        // Wait for chunk to be available (if still generating)
        if !is_complete_local {
            let wait_result = wait_for_chunk(
                &chunk_index,
                current_chunk_index,
                Duration::from_millis(100), // Check every 100ms
                Duration::from_secs(60),    // 60s timeout
            )
            .await;

            if let Err(e) = wait_result {
                let _ = tx
                    .send(Err(Status::internal(format!(
                        "Failed to wait for chunk {}: {}",
                        current_chunk_index, e
                    ))))
                    .await;
                return Err(e);
            }
        }

        // Get chunk metadata
        let chunk_metadata = {
            let (meta, complete) = {
                let index = chunk_index.read();
                match index.get_chunk(current_chunk_index) {
                    Some(meta) => (Some(meta.clone()), index.is_complete),
                    None => (None, index.is_complete),
                }
            };

            match meta {
                Some(meta) => meta,
                None => {
                    // Check if generation is complete
                    if complete {
                        // All chunks sent
                        break;
                    } else {
                        let _ = tx
                            .send(Err(Status::internal(format!(
                                "Chunk {} not found and generation not complete",
                                current_chunk_index
                            ))))
                            .await;
                        return Err(format!("Chunk {} not found", current_chunk_index));
                    }
                }
            }
        };

        // Read chunk from file
        let chunk_data =
            match read_chunk_from_file(&snapshot_path, current_chunk_index, &chunk_metadata) {
                Ok(data) => data,
                Err(e) => {
                    let _ = tx
                        .send(Err(Status::internal(format!(
                            "Failed to read chunk {}: {}",
                            current_chunk_index, e
                        ))))
                        .await;
                    return Err(e);
                }
            };

        // Determine if this is the last chunk
        let is_last = chunk_metadata.is_last;

        // Use chunk metadata file offset
        let chunk_offset = chunk_metadata.file_offset;

        // Create response
        // chunk_size should be the compressed size (which is what we're sending)
        let chunk_size = chunk_metadata.compressed_size;
        let response = PullSnapshotDataResponse {
            transfer_id: transfer_id.clone(),
            chunk_data: chunk_data.into(),
            offset: chunk_offset,
            chunk_size,
            is_last_chunk: is_last,
            total_size: if current_chunk_index == start_chunk_index {
                total_size
            } else {
                0
            },
            checksum: chunk_metadata.crc32.to_le_bytes().to_vec(),
            error_message: String::new(),
        };

        // Send response
        if tx.send(Ok(response)).await.is_err() {
            // Receiver dropped, client disconnected
            info!(
                "Client disconnected during snapshot transfer {}",
                transfer_id
            );
            return Ok(());
        }

        if is_last {
            info!("Completed streaming snapshot for transfer {}", transfer_id);
            break;
        }

        current_chunk_index += 1;
    }

    Ok(())
}
