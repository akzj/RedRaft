//! ReadIndex and LeaderLease implementation for linearizable reads
//! 
//! ## ReadIndex Mechanism
//! ReadIndex allows the Leader to provide linearizable reads without writing to the log:
//! 1. Record current commit_index as read_index
//! 2. Send heartbeat to confirm still Leader (1 RTT)
//! 3. Wait until last_applied >= read_index
//! 4. Return read result
//! 
//! ## LeaderLease Optimization
//! LeaderLease further optimizes ReadIndex to achieve 0 RTT reads:
//! 1. Leader maintains a lease through heartbeat responses
//! 2. During lease validity, Leader can be confident it's still the only Leader
//! 3. When lease is valid, ReadIndex can skip heartbeat confirmation and return directly
//! 
//! **Note**: LeaderLease relies on clock synchronization and has risks in environments with large clock skew

use std::collections::HashSet;
use std::time::Instant;

use tracing::{debug, info, warn};

use super::{RaftState, ReadIndexState};
use crate::error::ClientError;
use crate::event::Role;
use crate::types::{RaftId, RequestId};

/// ReadIndex request timeout
const READ_INDEX_TIMEOUT_MS: u64 = 5000;

/// LeaderLease safety factor (lease duration = election_timeout_min * LEASE_FACTOR)
/// Use 0.9 to ensure lease expires before election timeout, avoiding split brain
const LEASE_FACTOR: f64 = 0.9;

impl RaftState {
    /// Handle ReadIndex request
    pub(crate) async fn handle_read_index(&mut self, request_id: RequestId) {
        // Not Leader, return error
        if self.role != Role::Leader {
            warn!(
                "Node {} received ReadIndex but not leader (role: {:?})",
                self.id, self.role
            );
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .read_index_response(
                            &self.id,
                            request_id,
                            Err(ClientError::NotLeader(self.leader_id.clone())),
                        )
                        .await,
                    "read_index_response",
                    None,
                )
                .await;
            return;
        }

        let read_index = self.commit_index;

        // Single-node cluster: return directly
        if self.is_single_voter() {
            debug!(
                "Node {} single-node cluster, completing ReadIndex immediately at index {}",
                self.id, read_index
            );
            self.complete_read_index(request_id, read_index).await;
            return;
        }

        // LeaderLease optimization: if lease is valid, complete directly (0 RTT)
        if self.options.leader_lease_enabled && self.is_lease_valid() {
            debug!(
                "Node {} LeaderLease valid, completing ReadIndex {} immediately (0 RTT)",
                self.id, request_id
            );
            self.complete_read_index(request_id, read_index).await;
            return;
        }

        // Standard ReadIndex flow: heartbeat confirmation required (1 RTT)
        let state = ReadIndexState {
            read_index,
            acks: HashSet::from([self.id.clone()]), // Leader itself
            request_time: Instant::now(),
        };
        self.pending_read_indices.insert(request_id, state);

        info!(
            "Node {} starting ReadIndex request {} at commit_index {} (awaiting heartbeat confirmation)",
            self.id, request_id, read_index
        );

        // Use next heartbeat to confirm Leader identity
        // Heartbeat response will call handle_read_index_ack
    }

    /// Check if it's a single voter node
    fn is_single_voter(&self) -> bool {
        let voters = self.config.get_effective_voters();
        voters.len() == 1 && voters.contains(&self.id)
    }

    // ==================== LeaderLease Related Methods ====================

    /// Check if lease is valid
    pub(crate) fn is_lease_valid(&self) -> bool {
        if let Some(lease_end) = self.lease_end {
            Instant::now() < lease_end
        } else {
            false
        }
    }

    /// Extend lease (called when receiving majority heartbeat responses)
    pub(crate) fn extend_lease(&mut self) {
        if !self.options.leader_lease_enabled {
            return;
        }

        // Lease duration = election_timeout_min * LEASE_FACTOR
        let lease_duration = self.election_timeout_min.mul_f64(LEASE_FACTOR);
        let new_lease_end = Instant::now() + lease_duration;

        // Only extend, do not shorten
        match self.lease_end {
            Some(current) if current >= new_lease_end => {
                // Current lease is longer, no update
            }
            _ => {
                self.lease_end = Some(new_lease_end);
                debug!(
                    "Node {} extended lease to {:?} from now",
                    self.id, lease_duration
                );
            }
        }
    }

    /// Try to extend lease based on current match_index
    /// Called when majority match_index is confirmed
    pub(crate) fn try_extend_lease_from_majority(&mut self) {
        if !self.options.leader_lease_enabled || self.role != Role::Leader {
            return;
        }

        // Collect all nodes with match_index records (including self)
        let mut responsive_nodes: HashSet<RaftId> = self.match_index.keys().cloned().collect();
        responsive_nodes.insert(self.id.clone());

        // Check if majority is achieved
        if self.config.majority(&responsive_nodes) {
            self.extend_lease();
        }
    }

    /// Handle ReadIndex confirmation in heartbeat response
    /// Called when handle_append_entries_response succeeds
    pub(crate) async fn handle_read_index_ack(&mut self, peer: &crate::types::RaftId) {
        // Collect ReadIndex requests that need to be completed
        let mut completed = Vec::new();

        for (request_id, state) in self.pending_read_indices.iter_mut() {
            state.acks.insert(peer.clone());

            // Check if majority confirmation is achieved
            if self.config.majority(&state.acks) {
                completed.push((*request_id, state.read_index));
            }
        }

        // Complete confirmed requests
        for (request_id, read_index) in completed {
            self.pending_read_indices.remove(&request_id);
            info!(
                "Node {} ReadIndex {} confirmed by majority, read_index={}",
                self.id, request_id, read_index
            );
            self.complete_read_index(request_id, read_index).await;
        }
    }

    /// Complete ReadIndex: wait for application and return
    async fn complete_read_index(&mut self, request_id: RequestId, read_index: u64) {
        if self.last_applied >= read_index {
            // Already applied, return directly
            debug!(
                "Node {} ReadIndex {} completed immediately (last_applied={} >= read_index={})",
                self.id, request_id, self.last_applied, read_index
            );
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .read_index_response(&self.id, request_id, Ok(read_index))
                        .await,
                    "read_index_response",
                    None,
                )
                .await;
        } else {
            // Wait for application to read_index
            debug!(
                "Node {} ReadIndex {} waiting for apply (last_applied={} < read_index={})",
                self.id, request_id, self.last_applied, read_index
            );
            self.pending_reads.insert(request_id, read_index);
        }
    }

    /// Check pending read requests (called in apply_committed_logs)
    pub(crate) async fn check_pending_reads(&mut self) {
        if self.pending_reads.is_empty() {
            return;
        }

        let completed: Vec<_> = self
            .pending_reads
            .iter()
            .filter(|&(_, idx)| self.last_applied >= *idx)
            .map(|(id, idx)| (*id, *idx))
            .collect();

        for (request_id, read_index) in completed {
            self.pending_reads.remove(&request_id);
            debug!(
                "Node {} ReadIndex {} now complete (last_applied={} >= read_index={})",
                self.id, request_id, self.last_applied, read_index
            );
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .read_index_response(&self.id, request_id, Ok(read_index))
                        .await,
                    "read_index_response",
                    None,
                )
                .await;
        }
    }

    /// Clean up timeout ReadIndex requests
    pub(crate) async fn cleanup_timeout_read_indices(&mut self) {
        let now = Instant::now();
        let timeout = std::time::Duration::from_millis(READ_INDEX_TIMEOUT_MS);

        let expired: Vec<_> = self
            .pending_read_indices
            .iter()
            .filter(|(_, state)| now.duration_since(state.request_time) > timeout)
            .map(|(id, _)| *id)
            .collect();

        for request_id in expired {
            self.pending_read_indices.remove(&request_id);
            warn!(
                "Node {} ReadIndex {} timed out after {:?}",
                self.id, request_id, timeout
            );
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .read_index_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Timeout),
                        )
                        .await,
                    "read_index_response",
                    None,
                )
                .await;
        }

        // Also clean up read requests waiting for application
        let expired_reads: Vec<_> = self
            .pending_reads
            .keys()
            .cloned()
            .collect();

        // If no longer Leader, clean up all pending read requests
        if self.role != Role::Leader {
            for request_id in expired_reads {
                self.pending_reads.remove(&request_id);
                let _ = self
                    .error_handler
                    .handle_void(
                        self.callbacks
                            .read_index_response(
                                &self.id,
                                request_id,
                                Err(ClientError::NotLeader(self.leader_id.clone())),
                            )
                            .await,
                        "read_index_response",
                        None,
                    )
                    .await;
            }
        }
    }

    /// Clean up ReadIndex related state when cleaning up Leader state
    pub(crate) fn clear_read_index_state(&mut self) {
        self.pending_read_indices.clear();
        self.pending_reads.clear();
    }
}

