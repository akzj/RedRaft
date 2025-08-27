use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use tracing::{debug, warn};

use crate::{RaftId, RaftStateOptions, RequestId};

/// Pipeline 状态管理器，负责处理 InFlight 请求的反馈控制和超时管理
pub struct PipelineState {
    // 反馈控制状态
    inflight_requests: HashMap<RaftId, HashMap<RequestId, (u64, Instant)>>, // peer -> (request_id -> (next_ack, send_time))
    current_batch_size: HashMap<RaftId, u64>, // 每个peer的当前批次大小
    response_times: HashMap<RaftId, VecDeque<Duration>>, // 响应时间历史窗口
    success_rates: HashMap<RaftId, VecDeque<bool>>, // 成功率历史窗口

    // 简单高效的超时管理 - 按发送顺序排列
    inflight_timeout_queue: HashMap<RaftId, VecDeque<(RequestId, Instant)>>, // peer -> 按时间顺序的请求队列

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
        // 如果是第一次发送，使用初始批次大小
        let current_batch = self
            .current_batch_size
            .get(peer)
            .copied()
            .unwrap_or(self.options.initial_batch_size);

        // 获取历史性能数据
        let avg_response_time = self.calculate_average_response_time(peer);
        let success_rate = self.calculate_success_rate(peer);

        // 基于性能指标调整批次大小
        let adjusted_batch = self.adjust_batch_size_based_on_feedback(
            current_batch,
            avg_response_time,
            success_rate,
        );

        // 更新当前批次大小
        self.current_batch_size.insert(peer.clone(), adjusted_batch);

        debug!(
            "Adaptive batch size for {}: {} (avg_rtt: {:?}, success_rate: {:.2})",
            peer, adjusted_batch, avg_response_time, success_rate
        );

        adjusted_batch
    }

    /// 记录InFlight请求 - 使用高效超时管理
    pub fn track_inflight_request(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
        next_ack: u64,
        _send_time: Instant,
    ) {
        // 使用新的高效方法
        self.track_inflight_request_with_timeout(peer, request_id, next_ack);

        debug!(
            "Tracked inflight request {} to peer {} (next_ack: {}, total_inflight: {})",
            request_id,
            peer,
            next_ack,
            self.inflight_requests.get(peer).unwrap().len()
        );
    }

    /// 移除InFlight请求并记录反馈 - 使用高效超时管理
    pub fn remove_inflight_request(&mut self, peer: &RaftId, request_id: RequestId) {
        // 使用新的高效方法
        if self.remove_inflight_request_with_request_id(peer, request_id) {
            let remaining_count = self
                .inflight_requests
                .get(peer)
                .map(|reqs| reqs.len())
                .unwrap_or(0);

            debug!(
                "Removed failed inflight request {} to peer {} (remaining: {})",
                request_id, peer, remaining_count
            );
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
            let remaining_count = self
                .inflight_requests
                .get(peer)
                .map(|reqs| reqs.len())
                .unwrap_or(0);

            debug!(
                "Recorded successful response from peer {} (request: {}, response_time: {:?}, remaining_inflight: {})",
                peer, request_id, response_time, remaining_count
            );

            self.record_response_feedback(peer, response_time, true); // true 表示成功
        }
    }

    /// 记录AppendEntries响应反馈
    pub fn record_append_entries_response_feedback(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
        success: bool,
    ) {
        debug!(
            "AppendEntries response from {}: {} (success: {})",
            peer, request_id, success
        );
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
            let target_response_time = Duration::from_millis(100); // 目标响应时间100ms

            if response_time > target_response_time * 2 {
                // 响应时间过长，减小批次
                new_batch = (new_batch * 2 / 3).max(self.options.min_batch_size);
            } else if response_time < target_response_time / 2 {
                // 响应时间很短，可以增大批次
                new_batch = (new_batch * 6 / 5).min(self.options.max_batch_size);
            }
        }

        // 确保在合法范围内
        new_batch.clamp(self.options.min_batch_size, self.options.max_batch_size)
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
        // 获取该peer的平均响应时间
        let avg_response_time = self.calculate_average_response_time(peer);

        let timeout = if let Some(avg_rtt) = avg_response_time {
            // 基于平均响应时间计算: 超时 = avg_rtt * factor
            let calculated = avg_rtt.mul_f64(self.options.timeout_response_factor);

            // 确保在合理范围内
            calculated
                .max(self.options.min_request_timeout)
                .min(self.options.max_request_timeout)
        } else {
            // 没有历史数据时使用基础超时
            self.options.base_request_timeout
        };

        debug!(
            "Smart timeout for peer {}: {:?} (avg_rtt: {:?}, factor: {})",
            peer, timeout, avg_response_time, self.options.timeout_response_factor
        );

        timeout
    }

    /// 添加InFlight请求到超时队列 - 简单的尾部添加
    fn track_inflight_request_with_timeout(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
        next_ack_index: u64,
    ) {
        let now = Instant::now();

        // 添加到inflight_requests
        self.inflight_requests
            .entry(peer.clone())
            .or_insert_with(HashMap::new)
            .insert(request_id, (next_ack_index, now));

        // 添加到超时队列的尾部（保持时间顺序）
        self.inflight_timeout_queue
            .entry(peer.clone())
            .or_insert_with(VecDeque::new)
            .push_back((request_id, now));

        debug!(
            "Tracked inflight request {} to peer {} (queue_size: {})",
            request_id,
            peer,
            self.inflight_timeout_queue.get(peer).unwrap().len()
        );
    }

    /// 移除InFlight请求 - 简单且高效
    fn remove_inflight_request_with_request_id(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
    ) -> bool {
        // 从inflight_requests移除
        let removed = self
            .inflight_requests
            .get_mut(peer)
            .and_then(|peer_requests| peer_requests.remove(&request_id))
            .is_some();

        if removed {
            debug!(
                "Removed inflight request {} from peer {} (normal response)",
                request_id, peer
            );
        }

        // 注意：我们不从timeout_queue中移除，因为：
        // 1. 在VecDeque中查找并移除特定元素需要O(n)时间
        // 2. 在超时检查时发现请求已经不在inflight_requests中会自动忽略
        // 3. 这样保持了移除操作的O(1)性能

        removed
    }

    /// 获取当前InFlight请求总数
    pub fn get_inflight_request_count(&self) -> usize {
        self.inflight_requests
            .values()
            .map(|peer_requests| peer_requests.len())
            .sum()
    }
}
