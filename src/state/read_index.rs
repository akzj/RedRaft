//! ReadIndex 和 LeaderLease 实现 - 用于线性一致性读
//!
//! ## ReadIndex 机制
//! ReadIndex 允许 Leader 在不写入日志的情况下提供线性一致性读:
//! 1. 记录当前 commit_index 作为 read_index
//! 2. 发送心跳确认仍是 Leader（1 RTT）
//! 3. 等待 last_applied >= read_index
//! 4. 返回读取结果
//!
//! ## LeaderLease 优化
//! LeaderLease 在 ReadIndex 基础上进一步优化，实现 0 RTT 读:
//! 1. Leader 通过心跳响应维护一个租约（lease）
//! 2. 租约有效期内，Leader 可确信自己仍是唯一 Leader
//! 3. 租约有效时，ReadIndex 可跳过心跳确认，直接返回
//!
//! **注意**: LeaderLease 依赖时钟同步，在时钟偏移较大的环境下有风险

use std::collections::HashSet;
use std::time::Instant;

use tracing::{debug, info, warn};

use super::{RaftState, ReadIndexState};
use crate::error::ClientError;
use crate::event::Role;
use crate::types::{RaftId, RequestId};

/// ReadIndex 请求超时时间
const READ_INDEX_TIMEOUT_MS: u64 = 5000;

/// LeaderLease 安全系数（租约时长 = election_timeout_min * LEASE_FACTOR）
/// 使用 0.9 确保在选举超时前租约过期，避免脑裂
const LEASE_FACTOR: f64 = 0.9;

impl RaftState {
    /// 处理 ReadIndex 请求
    pub(crate) async fn handle_read_index(&mut self, request_id: RequestId) {
        // 非 Leader 返回错误
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

        // 单节点集群：直接返回
        if self.is_single_voter() {
            debug!(
                "Node {} single-node cluster, completing ReadIndex immediately at index {}",
                self.id, read_index
            );
            self.complete_read_index(request_id, read_index).await;
            return;
        }

        // LeaderLease 优化：如果租约有效，直接完成（0 RTT）
        if self.options.leader_lease_enabled && self.is_lease_valid() {
            debug!(
                "Node {} LeaderLease valid, completing ReadIndex {} immediately (0 RTT)",
                self.id, request_id
            );
            self.complete_read_index(request_id, read_index).await;
            return;
        }

        // 标准 ReadIndex 流程：需要心跳确认（1 RTT）
        let state = ReadIndexState {
            read_index,
            acks: HashSet::from([self.id.clone()]), // Leader 自己
            request_time: Instant::now(),
        };
        self.pending_read_indices.insert(request_id, state);

        info!(
            "Node {} starting ReadIndex request {} at commit_index {} (awaiting heartbeat confirmation)",
            self.id, request_id, read_index
        );

        // 利用下一次心跳确认 Leader 身份
        // 心跳响应会调用 handle_read_index_ack
    }

    /// 检查是否是单节点 voter
    fn is_single_voter(&self) -> bool {
        let voters = self.config.get_effective_voters();
        voters.len() == 1 && voters.contains(&self.id)
    }

    // ==================== LeaderLease 相关方法 ====================

    /// 检查租约是否有效
    pub(crate) fn is_lease_valid(&self) -> bool {
        if let Some(lease_end) = self.lease_end {
            Instant::now() < lease_end
        } else {
            false
        }
    }

    /// 延长租约（在收到多数派心跳响应时调用）
    pub(crate) fn extend_lease(&mut self) {
        if !self.options.leader_lease_enabled {
            return;
        }

        // 租约时长 = election_timeout_min * LEASE_FACTOR
        let lease_duration = self.election_timeout_min.mul_f64(LEASE_FACTOR);
        let new_lease_end = Instant::now() + lease_duration;

        // 只延长，不缩短
        match self.lease_end {
            Some(current) if current >= new_lease_end => {
                // 当前租约更长，不更新
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

    /// 尝试基于当前 match_index 延长租约
    /// 当多数派的 match_index 确认时调用
    pub(crate) fn try_extend_lease_from_majority(&mut self) {
        if !self.options.leader_lease_enabled || self.role != Role::Leader {
            return;
        }

        // 收集所有有 match_index 记录的节点（包括自己）
        let mut responsive_nodes: HashSet<RaftId> = self.match_index.keys().cloned().collect();
        responsive_nodes.insert(self.id.clone());

        // 检查是否达到多数派
        if self.config.majority(&responsive_nodes) {
            self.extend_lease();
        }
    }

    /// 处理心跳响应中的 ReadIndex 确认
    /// 在 handle_append_entries_response 成功时调用
    pub(crate) async fn handle_read_index_ack(&mut self, peer: &crate::types::RaftId) {
        // 收集需要完成的 ReadIndex 请求
        let mut completed = Vec::new();

        for (request_id, state) in self.pending_read_indices.iter_mut() {
            state.acks.insert(peer.clone());

            // 检查是否获得多数派确认
            if self.config.majority(&state.acks) {
                completed.push((*request_id, state.read_index));
            }
        }

        // 完成已确认的请求
        for (request_id, read_index) in completed {
            self.pending_read_indices.remove(&request_id);
            info!(
                "Node {} ReadIndex {} confirmed by majority, read_index={}",
                self.id, request_id, read_index
            );
            self.complete_read_index(request_id, read_index).await;
        }
    }

    /// 完成 ReadIndex：等待应用后返回
    async fn complete_read_index(&mut self, request_id: RequestId, read_index: u64) {
        if self.last_applied >= read_index {
            // 已经应用，直接返回
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
            // 等待应用到 read_index
            debug!(
                "Node {} ReadIndex {} waiting for apply (last_applied={} < read_index={})",
                self.id, request_id, self.last_applied, read_index
            );
            self.pending_reads.insert(request_id, read_index);
        }
    }

    /// 检查待处理的读请求（在 apply_committed_logs 中调用）
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

    /// 清理超时的 ReadIndex 请求
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

        // 同样清理等待应用的读请求
        let expired_reads: Vec<_> = self
            .pending_reads
            .keys()
            .cloned()
            .collect();

        // 如果不再是 Leader，清理所有待处理的读请求
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

    /// 清理 Leader 状态时清理 ReadIndex 相关状态
    pub(crate) fn clear_read_index_state(&mut self) {
        self.pending_read_indices.clear();
        self.pending_reads.clear();
    }
}

