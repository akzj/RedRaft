use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use tracing::{debug, trace, warn};

use crate::{RaftId, RaftStateOptions, RequestId};

/// 超时队列清理阈值：当队列中的过期条目超过此数量时触发批量清理
const TIMEOUT_QUEUE_CLEANUP_THRESHOLD: usize = 100;

/// Pipeline 状态管理器，负责处理 InFlight 请求的反馈控制和超时管理
pub struct PipelineState {
    // 反馈控制状态
    /// peer -> (request_id -> (next_ack, send_time))
    inflight_requests: HashMap<RaftId, HashMap<RequestId, (u64, Instant)>>,
    /// 每个 peer 的当前批次大小
    current_batch_size: HashMap<RaftId, u64>,
    /// 响应时间历史窗口
    response_times: HashMap<RaftId, VecDeque<Duration>>,
    /// 成功率历史窗口
    success_rates: HashMap<RaftId, VecDeque<bool>>,

    // 超时管理 - 按发送顺序排列
    /// peer -> 按时间顺序的请求队列（用于高效超时检查）
    inflight_timeout_queue: HashMap<RaftId, VecDeque<(RequestId, Instant)>>,
    /// 累计的陈旧条目计数（用于触发批量清理）
    stale_entry_count: usize,

    // 配置选项
    options: RaftStateOptions,
}

impl PipelineState {
    /// 创建新的 Pipeline 状态管理器
    pub fn new(options: RaftStateOptions) -> Self {
        Self {
            inflight_requests: HashMap::new(),
            current_batch_size: HashMap::new(),
            response_times: HashMap::new(),
            success_rates: HashMap::new(),
            inflight_timeout_queue: HashMap::new(),
            stale_entry_count: 0,
            options,
        }
    }

    /// 检查是否可以向peer发送请求（InFlight限制）
    pub fn can_send_to_peer(&self, peer: &RaftId) -> bool {
        let inflight_count = self
            .inflight_requests
            .get(peer)
            .map(|requests| requests.len() as u64)
            .unwrap_or(0);

        inflight_count < self.options.max_inflight_requests
    }

    /// 获取基于反馈控制的批次大小
    pub fn get_adaptive_batch_size(&mut self, peer: &RaftId) -> u64 {
        let current_batch = self
            .current_batch_size
            .get(peer)
            .copied()
            .unwrap_or(self.options.initial_batch_size);

        let avg_rtt = self.calculate_average_response_time(peer);
        let success_rate = self.calculate_success_rate(peer);

        let adjusted = self.adjust_batch_size_based_on_feedback(current_batch, avg_rtt, success_rate);
        self.current_batch_size.insert(peer.clone(), adjusted);

        trace!(
            "Batch size for {}: {} (rtt: {:?}, success: {:.0}%)",
            peer, adjusted, avg_rtt, success_rate * 100.0
        );

        adjusted
    }

    /// 记录 InFlight 请求
    pub fn track_inflight_request(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
        next_ack: u64,
        _send_time: Instant,
    ) {
        self.track_inflight_request_with_timeout(peer, request_id, next_ack);
    }

    /// 移除 InFlight 请求（失败响应时调用）
    pub fn remove_inflight_request(&mut self, peer: &RaftId, request_id: RequestId) {
        if self.remove_inflight_request_with_request_id(peer, request_id) {
            // 标记有陈旧条目需要清理（timeout_queue 中仍有此条目）
            self.stale_entry_count += 1;
            self.maybe_cleanup_stale_entries();
        }
    }

    /// 记录成功响应的反馈
    pub fn record_success_response(&mut self, peer: &RaftId, request_id: RequestId) {
        let removed_request = self
            .inflight_requests
            .get_mut(peer)
            .and_then(|peer_requests| peer_requests.remove(&request_id));

        if let Some((_, send_time)) = removed_request {
            let response_time = send_time.elapsed();

            trace!(
                "Response from {} (request: {}, rtt: {:?})",
                peer, request_id, response_time
            );

            self.record_response_feedback(peer, response_time, true);
            
            // 标记有陈旧条目需要清理
            self.stale_entry_count += 1;
            self.maybe_cleanup_stale_entries();
        }
    }

    /// 记录 AppendEntries 响应反馈
    pub fn record_append_entries_response_feedback(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
        success: bool,
    ) {
        if success {
            self.record_success_response(peer, request_id);
        } else {
            self.remove_inflight_request(peer, request_id);
        }
    }

    /// 优化的周期性超时检查 - 在心跳时调用
    pub fn periodic_timeout_check(&mut self) {
        self.check_and_cleanup_expired_requests();
    }

    /// 记录响应反馈数据
    fn record_response_feedback(&mut self, peer: &RaftId, response_time: Duration, success: bool) {
        // 记录响应时间
        let response_times = self
            .response_times
            .entry(peer.clone())
            .or_insert_with(VecDeque::new);
        response_times.push_back(response_time);
        if response_times.len() > self.options.feedback_window_size {
            response_times.pop_front();
        }

        // 记录成功率
        let success_rates = self
            .success_rates
            .entry(peer.clone())
            .or_insert_with(VecDeque::new);
        success_rates.push_back(success);
        if success_rates.len() > self.options.feedback_window_size {
            success_rates.pop_front();
        }
    }

    /// 计算平均响应时间
    fn calculate_average_response_time(&self, peer: &RaftId) -> Option<Duration> {
        self.response_times.get(peer).and_then(|times| {
            if times.is_empty() {
                None
            } else {
                let total = times.iter().sum::<Duration>();
                Some(total / times.len() as u32)
            }
        })
    }

    /// 计算成功率
    fn calculate_success_rate(&self, peer: &RaftId) -> f64 {
        self.success_rates
            .get(peer)
            .map(|rates| {
                if rates.is_empty() {
                    1.0 // 默认成功率
                } else {
                    let success_count = rates.iter().filter(|&&success| success).count();
                    success_count as f64 / rates.len() as f64
                }
            })
            .unwrap_or(1.0)
    }

    /// 基于反馈调整批次大小
    fn adjust_batch_size_based_on_feedback(
        &self,
        current_batch: u64,
        avg_response_time: Option<Duration>,
        success_rate: f64,
    ) -> u64 {
        let mut new_batch = current_batch;

        // 基于成功率调整
        if success_rate < 0.8 {
            // 成功率低，减小批次大小
            new_batch = (new_batch * 3 / 4).max(self.options.min_batch_size);
        } else if success_rate > 0.95 {
            // 成功率高，可以尝试增大批次
            new_batch = (new_batch * 5 / 4).min(self.options.max_batch_size);
        }

        // 基于响应时间调整
        if let Some(response_time) = avg_response_time {
            let target = self.options.target_response_time;

            if response_time > target * 2 {
                // 响应时间过长，减小批次
                new_batch = (new_batch * 2 / 3).max(self.options.min_batch_size);
            } else if response_time < target / 2 {
                // 响应时间很短，可以增大批次
                new_batch = (new_batch * 6 / 5).min(self.options.max_batch_size);
            }
        }

        // 确保在合法范围内
        new_batch.clamp(self.options.min_batch_size, self.options.max_batch_size)
    }

    /// 当陈旧条目积累到阈值时，批量清理超时队列
    fn maybe_cleanup_stale_entries(&mut self) {
        if self.stale_entry_count < TIMEOUT_QUEUE_CLEANUP_THRESHOLD {
            return;
        }

        let mut cleaned = 0;
        for (peer, queue) in self.inflight_timeout_queue.iter_mut() {
            let inflight = self.inflight_requests.get(peer);
            let before_len = queue.len();
            
            // 保留仍在 inflight_requests 中的条目
            queue.retain(|(req_id, _)| {
                inflight.map_or(false, |reqs| reqs.contains_key(req_id))
            });
            
            cleaned += before_len - queue.len();
        }

        if cleaned > 0 {
            debug!(
                "Cleaned {} stale entries from timeout queues (threshold: {})",
                cleaned, TIMEOUT_QUEUE_CLEANUP_THRESHOLD
            );
        }

        self.stale_entry_count = 0;
    }

    /// 简单高效的超时检查 - 利用时间顺序特性，基于智能超时计算
    fn check_and_cleanup_expired_requests(&mut self) {
        let now = Instant::now();
        let mut expired_items = Vec::new();
        let mut peer_timeouts = Vec::new();

        // 预先计算所有peer的超时时间，避免借用冲突
        for peer in self.inflight_timeout_queue.keys() {
            let timeout_duration = self.calculate_smart_timeout(peer);
            peer_timeouts.push((peer.clone(), timeout_duration));
        }

        // 遍历每个peer的超时队列
        for (peer, timeout_duration) in peer_timeouts {
            if let Some(timeout_queue) = self.inflight_timeout_queue.get_mut(&peer) {
                // 从队列前面开始检查，遇到未超时的就停止
                while let Some((_request_id, send_time)) = timeout_queue.front() {
                    if now.duration_since(*send_time) > timeout_duration {
                        let (expired_request_id, expired_send_time) =
                            timeout_queue.pop_front().unwrap();
                        expired_items.push((
                            peer.clone(),
                            expired_request_id,
                            expired_send_time,
                            timeout_duration,
                        ));
                    } else {
                        // 遇到未超时的请求，后续的都不会超时，直接停止
                        break;
                    }
                }
            }
        }

        // 批量处理过期的请求
        for (peer, expired_request_id, expired_send_time, timeout_used) in expired_items {
            // 从inflight_requests中移除
            if let Some(peer_requests) = self.inflight_requests.get_mut(&peer) {
                if peer_requests.remove(&expired_request_id).is_some() {
                    let elapsed = expired_send_time.elapsed();
                    warn!(
                        "Cleaned up expired inflight request {} to peer {} (elapsed: {:?}, timeout: {:?})",
                        expired_request_id, peer, elapsed, timeout_used
                    );

                    // 记录为失败的反馈
                    self.record_response_feedback(&peer, elapsed, false);
                }
            }
        }
    }

    /// 计算基于平均响应时间的智能超时时间
    fn calculate_smart_timeout(&self, peer: &RaftId) -> Duration {
        let avg_rtt = self.calculate_average_response_time(peer);

        if let Some(rtt) = avg_rtt {
            // 超时 = avg_rtt * factor，限制在合理范围内
            rtt.mul_f64(self.options.timeout_response_factor)
                .clamp(self.options.min_request_timeout, self.options.max_request_timeout)
        } else {
            // 没有历史数据时使用基础超时
            self.options.base_request_timeout
        }
    }

    /// 添加 InFlight 请求到超时队列
    fn track_inflight_request_with_timeout(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
        next_ack_index: u64,
    ) {
        let now = Instant::now();

        // 添加到 inflight_requests
        self.inflight_requests
            .entry(peer.clone())
            .or_default()
            .insert(request_id, (next_ack_index, now));

        // 添加到超时队列的尾部（保持时间顺序）
        self.inflight_timeout_queue
            .entry(peer.clone())
            .or_default()
            .push_back((request_id, now));

        trace!(
            "Tracked inflight {} to {} (next_ack: {}, queue_size: {})",
            request_id,
            peer,
            next_ack_index,
            self.inflight_timeout_queue.get(peer).map(|q| q.len()).unwrap_or(0)
        );
    }

    /// 移除 InFlight 请求（O(1) 操作）
    /// 
    /// 注意：不从 timeout_queue 移除，因为 VecDeque 中移除需要 O(n)。
    /// 超时检查时会自动跳过已不在 inflight_requests 中的条目。
    fn remove_inflight_request_with_request_id(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
    ) -> bool {
        self.inflight_requests
            .get_mut(peer)
            .and_then(|peer_requests| peer_requests.remove(&request_id))
            .is_some()
    }

    /// 获取当前 InFlight 请求总数
    pub fn get_inflight_request_count(&self) -> usize {
        self.inflight_requests
            .values()
            .map(|peer_requests| peer_requests.len())
            .sum()
    }

    /// 获取指定 peer 的 InFlight 请求数
    pub fn get_peer_inflight_count(&self, peer: &RaftId) -> usize {
        self.inflight_requests
            .get(peer)
            .map(|reqs| reqs.len())
            .unwrap_or(0)
    }

    /// 获取 Pipeline 统计信息
    pub fn get_stats(&self) -> PipelineStats {
        let total_inflight = self.get_inflight_request_count();
        let total_timeout_queue: usize = self
            .inflight_timeout_queue
            .values()
            .map(|q| q.len())
            .sum();

        let peer_stats: HashMap<RaftId, PeerPipelineStats> = self
            .inflight_requests
            .keys()
            .map(|peer| {
                let inflight = self.get_peer_inflight_count(peer);
                let avg_rtt = self.calculate_average_response_time(peer);
                let success_rate = self.calculate_success_rate(peer);
                let batch_size = self.current_batch_size.get(peer).copied().unwrap_or(0);

                (
                    peer.clone(),
                    PeerPipelineStats {
                        inflight_count: inflight,
                        avg_response_time: avg_rtt,
                        success_rate,
                        current_batch_size: batch_size,
                    },
                )
            })
            .collect();

        PipelineStats {
            total_inflight,
            total_timeout_queue_size: total_timeout_queue,
            stale_entry_count: self.stale_entry_count,
            peer_stats,
        }
    }

    /// 清理所有状态（角色切换时调用）
    pub fn clear_all(&mut self) {
        self.inflight_requests.clear();
        self.current_batch_size.clear();
        self.response_times.clear();
        self.success_rates.clear();
        self.inflight_timeout_queue.clear();
        self.stale_entry_count = 0;
    }
}

/// Pipeline 整体统计信息
#[derive(Debug, Clone)]
pub struct PipelineStats {
    /// 总 InFlight 请求数
    pub total_inflight: usize,
    /// 超时队列总大小（可能包含已处理的陈旧条目）
    pub total_timeout_queue_size: usize,
    /// 待清理的陈旧条目计数
    pub stale_entry_count: usize,
    /// 各 peer 的统计信息
    pub peer_stats: HashMap<RaftId, PeerPipelineStats>,
}

/// 单个 peer 的 Pipeline 统计信息
#[derive(Debug, Clone)]
pub struct PeerPipelineStats {
    /// InFlight 请求数
    pub inflight_count: usize,
    /// 平均响应时间
    pub avg_response_time: Option<Duration>,
    /// 成功率
    pub success_rate: f64,
    /// 当前批次大小
    pub current_batch_size: u64,
}
