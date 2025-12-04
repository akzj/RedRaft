//! ReadIndex 实现 - 用于线性一致性读
//!
//! ReadIndex 机制允许 Leader 在不写入日志的情况下提供线性一致性读:
//! 1. 记录当前 commit_index 作为 read_index
//! 2. 发送心跳确认仍是 Leader
//! 3. 等待 last_applied >= read_index
//! 4. 返回读取结果

use std::collections::HashSet;
use std::time::Instant;

use tracing::{debug, info, warn};

use super::{RaftState, ReadIndexState};
use crate::error::ClientError;
use crate::event::Role;
use crate::types::RequestId;

/// ReadIndex 请求超时时间
const READ_INDEX_TIMEOUT_MS: u64 = 5000;

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

        // 记录 pending 状态
        let state = ReadIndexState {
            read_index,
            acks: HashSet::from([self.id.clone()]), // Leader 自己
            request_time: Instant::now(),
        };
        self.pending_read_indices.insert(request_id, state);

        info!(
            "Node {} starting ReadIndex request {} at commit_index {}",
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

