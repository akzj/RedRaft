//! Multi-Raft 驱动器
//!
//! 负责管理多个 Raft 组的事件调度和定时器服务。
//! 使用无锁设计和高效的状态机转换来处理并发事件。

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, info, trace, warn};

use crate::{Event, RaftId, TimerId};

/// 事件通道容量（提供背压保护）
const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Raft 节点状态枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftNodeStatus {
    /// 空闲状态（无未处理消息，未被 Worker 处理）
    Idle = 0,
    /// 待处理状态（有未处理消息，已加入 active_map）
    Pending = 1,
    /// 活跃状态（正在被 Worker 处理）
    Active = 2,
    /// 停止状态
    Stop = 3,
}

impl RaftNodeStatus {
    /// 从 u8 转换，返回 None 如果值无效
    fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(RaftNodeStatus::Idle),
            1 => Some(RaftNodeStatus::Pending),
            2 => Some(RaftNodeStatus::Active),
            3 => Some(RaftNodeStatus::Stop),
            _ => None,
        }
    }
}

use std::sync::atomic::AtomicU8;

/// 原子 RaftNodeStatus 包装器
pub struct AtomicRaftNodeStatus(AtomicU8);

impl AtomicRaftNodeStatus {
    pub fn new(status: RaftNodeStatus) -> Self {
        Self(AtomicU8::new(status as u8))
    }

    pub fn load(&self, order: Ordering) -> RaftNodeStatus {
        RaftNodeStatus::from_u8(self.0.load(order)).unwrap_or(RaftNodeStatus::Stop)
    }

    pub fn store(&self, status: RaftNodeStatus, order: Ordering) {
        self.0.store(status as u8, order);
    }

    pub fn compare_exchange(
        &self,
        current: RaftNodeStatus,
        new: RaftNodeStatus,
        success: Ordering,
        failure: Ordering,
    ) -> Result<RaftNodeStatus, RaftNodeStatus> {
        match self
            .0
            .compare_exchange(current as u8, new as u8, success, failure)
        {
            Ok(val) => Ok(RaftNodeStatus::from_u8(val).unwrap_or(RaftNodeStatus::Stop)),
            Err(val) => Err(RaftNodeStatus::from_u8(val).unwrap_or(RaftNodeStatus::Stop)),
        }
    }
}

/// 定时器事件
#[derive(Debug)]
struct TimerEvent {
    timer_id: TimerId,
    node_id: RaftId,
    event: Event,
    trigger_time: Instant,
    delay: Duration,
}

impl PartialEq for TimerEvent {
    fn eq(&self, other: &Self) -> bool {
        self.trigger_time.eq(&other.trigger_time)
    }
}

impl Eq for TimerEvent {}

impl PartialOrd for TimerEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // 反转顺序实现最小堆
        other.trigger_time.cmp(&self.trigger_time)
    }
}

/// 事件处理 trait
#[async_trait::async_trait]
pub trait HandleEventTrait: Send + Sync {
    async fn handle_event(&self, event: Event);
}

/// Raft 组核心数据
struct RaftGroupCore {
    status: AtomicRaftNodeStatus,
    sender: mpsc::Sender<Event>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<Event>>,
    handle_event: Box<dyn HandleEventTrait>,
}

/// 定时器内部状态
pub struct TimerInner {
    timer_id_counter: AtomicU64,
    timer_heap: Mutex<BinaryHeap<TimerEvent>>,
    /// 已取消的定时器 ID（惰性删除）
    cancelled_timers: Mutex<HashSet<TimerId>>,
}

/// 定时器服务
#[derive(Clone)]
pub struct Timers {
    inner: Arc<TimerInner>,
    notify: Arc<Notify>,
}

impl Timers {
    pub fn new(notify: Arc<Notify>) -> Self {
        Self {
            notify,
            inner: Arc::new(TimerInner {
                timer_id_counter: AtomicU64::new(0),
                timer_heap: Mutex::new(BinaryHeap::new()),
                cancelled_timers: Mutex::new(HashSet::new()),
            }),
        }
    }

    /// 添加定时器，返回定时器 ID
    pub fn add_timer(&self, node_id: &RaftId, event: Event, delay: Duration) -> TimerId {
        let timer_id = self.inner.timer_id_counter.fetch_add(1, Ordering::Relaxed);
        let trigger_time = Instant::now() + delay;
        let timer_event = TimerEvent {
            timer_id,
            event,
            delay,
            trigger_time,
            node_id: node_id.clone(),
        };

        self.inner.timer_heap.lock().push(timer_event);
        self.notify.notify_one();

        trace!("Added timer {} for node {} with delay {:?}", timer_id, node_id, delay);
        timer_id
    }

    /// 删除定时器（O(1) 惰性删除）
    pub fn del_timer(&self, timer_id: TimerId) {
        self.inner.cancelled_timers.lock().insert(timer_id);
        trace!("Cancelled timer {}", timer_id);
    }

    /// 删除节点的所有定时器
    pub fn del_all_timers_for_node(&self, node_id: &RaftId) {
        let mut timer_heap = self.inner.timer_heap.lock();
        let before_len = timer_heap.len();

        // 重建堆，排除该节点的定时器
        let remaining: Vec<_> = timer_heap
            .drain()
            .filter(|t| &t.node_id != node_id)
            .collect();

        for timer in remaining {
            timer_heap.push(timer);
        }

        let removed = before_len - timer_heap.len();
        if removed > 0 {
            debug!("Removed {} timers for node {}", removed, node_id);
        }
    }

    /// 处理过期定时器，返回触发的事件和下一个定时器的等待时间
    fn process_expired_timers(&self) -> (Vec<TimerEvent>, Option<Duration>) {
        let now = Instant::now();
        let mut events = Vec::new();
        let mut timer_heap = self.inner.timer_heap.lock();
        let cancelled = self.inner.cancelled_timers.lock();

        while let Some(timer_event) = timer_heap.peek() {
            // 跳过已取消的定时器
            if cancelled.contains(&timer_event.timer_id) {
                timer_heap.pop();
                continue;
            }

            if timer_event.trigger_time <= now {
                let event = timer_heap.pop().unwrap();
                events.push(event);
            } else {
                return (events, Some(timer_event.trigger_time - now));
            }
        }

        (events, None)
    }

    /// 清理已取消的定时器集合（定期调用以释放内存）
    pub fn cleanup_cancelled(&self) {
        let mut cancelled = self.inner.cancelled_timers.lock();
        if cancelled.len() > 1000 {
            // 只有当积累较多时才清理
            let timer_heap = self.inner.timer_heap.lock();
            let active_ids: HashSet<_> = timer_heap.iter().map(|t| t.timer_id).collect();
            cancelled.retain(|id| active_ids.contains(id));
        }
    }
}

impl Deref for Timers {
    type Target = TimerInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// 发送事件结果
#[derive(Debug)]
pub enum SendEventResult {
    Success,
    NotFound,
    SendFailed,
    ChannelFull,
}

/// Multi-Raft 驱动器内部状态
pub struct MultiRaftDriverInner {
    timer_service: Timers,
    groups: Mutex<HashMap<RaftId, Arc<RaftGroupCore>>>,
    active_map: Mutex<HashSet<RaftId>>,
    notify: Arc<Notify>,
    stop: AtomicBool,
}

/// Multi-Raft 驱动器
#[derive(Clone)]
pub struct MultiRaftDriver {
    inner: Arc<MultiRaftDriverInner>,
}

impl Deref for MultiRaftDriver {
    type Target = MultiRaftDriverInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl MultiRaftDriver {
    /// 创建新的 MultiRaftDriver
    pub fn new() -> Self {
        let notify = Arc::new(Notify::new());
        Self {
            inner: Arc::new(MultiRaftDriverInner {
                timer_service: Timers::new(notify.clone()),
                groups: Mutex::new(HashMap::new()),
                active_map: Mutex::new(HashSet::new()),
                notify,
                stop: AtomicBool::new(false),
            }),
        }
    }

    /// 获取定时器服务
    pub fn get_timer_service(&self) -> Timers {
        self.inner.timer_service.clone()
    }

    /// 添加新的 Raft 组
    pub fn add_raft_group(&self, group_id: RaftId, handle_event: Box<dyn HandleEventTrait>) {
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let core = RaftGroupCore {
            status: AtomicRaftNodeStatus::new(RaftNodeStatus::Idle),
            sender: tx,
            receiver: tokio::sync::Mutex::new(rx),
            handle_event,
        };
        self.groups.lock().insert(group_id.clone(), Arc::new(core));
        info!("Added Raft group: {}", group_id);
    }

    /// 删除 Raft 组
    pub fn del_raft_group(&self, group_id: &RaftId) {
        if self.groups.lock().remove(group_id).is_some() {
            info!("Removed Raft group: {}", group_id);
            self.timer_service.del_all_timers_for_node(group_id);
        }
    }

    /// 停止驱动器
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.notify.notify_waiters();
        info!("MultiRaftDriver stop signal sent");
    }

    /// 处理过期定时器
    async fn process_expired_timers(&self) -> Option<Duration> {
        let (events, duration) = self.timer_service.process_expired_timers();

        for event in events {
            let result = self.dispatch_event(event.node_id.clone(), event.event.clone());
            match result {
                SendEventResult::Success => {}
                SendEventResult::NotFound => {
                    debug!(
                        "Timer event for removed node ignored: node_id: {}",
                        event.node_id
                    );
                }
                SendEventResult::SendFailed | SendEventResult::ChannelFull => {
                    warn!(
                        "Failed to send timer event: node_id: {}, delay: {:?}",
                        event.node_id, event.delay
                    );
                }
            }
        }

        // 定期清理已取消的定时器
        self.timer_service.cleanup_cancelled();

        duration
    }

    /// 向指定 Raft 组发送事件
    pub fn dispatch_event(&self, target: RaftId, event: Event) -> SendEventResult {
        let core = {
            let groups = self.groups.lock();
            match groups.get(&target) {
                Some(core) => core.clone(),
                None => return SendEventResult::NotFound,
            }
        };

        // 1. 发送事件到通道
        match core.sender.try_send(event) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("Event channel full for node {}", target);
                return SendEventResult::ChannelFull;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return SendEventResult::SendFailed;
            }
        }

        // 2. 根据当前状态决定是否需要激活组
        let current_status = core.status.load(Ordering::Acquire);
        match current_status {
            RaftNodeStatus::Idle => {
                // 从 Idle -> Pending，加入 active_map 并唤醒 Worker
                if core
                    .status
                    .compare_exchange(
                        RaftNodeStatus::Idle,
                        RaftNodeStatus::Pending,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.active_map.lock().insert(target);
                    self.notify.notify_one();
                }
            }
            RaftNodeStatus::Pending | RaftNodeStatus::Active => {
                // 已在处理中
            }
            RaftNodeStatus::Stop => {
                warn!("Target node {} is in Stop status", target);
                return SendEventResult::NotFound;
            }
        }

        SendEventResult::Success
    }

    /// 主循环
    pub async fn main_loop(&self) {
        info!("Starting main loop for MultiRaftDriver");

        loop {
            // 处理过期定时器
            let wait_duration = self.process_expired_timers().await;

            // 等待：有定时器则等待到期，否则等待唤醒
            if let Some(duration) = wait_duration {
                tokio::select! {
                    _ = tokio::time::sleep(duration) => {
                        continue;
                    }
                    _ = self.notify.notified() => {}
                }
            } else {
                self.notify.notified().await;
            }

            // 检查停止标志
            if self.stop.load(Ordering::Acquire) {
                info!("Stop signal received, exiting main loop");
                break;
            }

            // 处理活跃节点
            self.process_active_nodes().await;
        }
    }

    /// 处理 active_map 中的所有待处理组
    async fn process_active_nodes(&self) {
        let pendings: HashSet<RaftId> = {
            let mut active_map = self.active_map.lock();
            std::mem::take(&mut *active_map)
        };

        for node_id in pendings {
            let driver = self.clone();
            tokio::spawn(async move {
                driver.process_single_group(node_id).await;
            });
        }
    }

    /// 处理单个 Raft 组的所有事件
    async fn process_single_group(&self, node_id: RaftId) {
        let core = {
            let groups = self.groups.lock();
            match groups.get(&node_id) {
                Some(core) => core.clone(),
                None => {
                    debug!("Group {} not found, skipping", node_id);
                    return;
                }
            }
        };

        // 1. 尝试获取处理权（Pending -> Active）
        if core
            .status
            .compare_exchange(
                RaftNodeStatus::Pending,
                RaftNodeStatus::Active,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            trace!(
                "Group {} not pending, current: {:?}",
                node_id,
                core.status.load(Ordering::Acquire)
            );
            return;
        }

        // 2. 处理事件循环
        let mut rx = core.receiver.lock().await;

        loop {
            match rx.try_recv() {
                Ok(event) => {
                    // 检查是否已停止
                    if core.status.load(Ordering::Acquire) == RaftNodeStatus::Stop {
                        warn!("Node {} stopped, ignoring event", node_id);
                        return;
                    }
                    core.handle_event.handle_event(event).await;
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    // 尝试转为 Idle
                    if core
                        .status
                        .compare_exchange(
                            RaftNodeStatus::Active,
                            RaftNodeStatus::Idle,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        // 转换成功后，再次检查是否有新消息
                        // 这解决了 is_empty 检查和状态转换之间的竞态
                        match rx.try_recv() {
                            Ok(event) => {
                                // 有新消息，回到 Active 状态
                                if core
                                    .status
                                    .compare_exchange(
                                        RaftNodeStatus::Idle,
                                        RaftNodeStatus::Active,
                                        Ordering::AcqRel,
                                        Ordering::Acquire,
                                    )
                                    .is_ok()
                                {
                                    core.handle_event.handle_event(event).await;
                                    continue;
                                } else {
                                    // 其他 worker 已接管，将消息放回
                                    // 由于无法放回，重新 dispatch
                                    drop(rx);
                                    let _ = core.sender.try_send(event);
                                    break;
                                }
                            }
                            Err(_) => {
                                // 确实没有消息，退出
                                break;
                            }
                        }
                    } else {
                        // 状态变更失败，退出
                        break;
                    }
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    core.status.store(RaftNodeStatus::Idle, Ordering::Release);
                    debug!("Channel disconnected for node {}", node_id);
                    return;
                }
            }
        }
    }
}

impl Default for MultiRaftDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::time::sleep;

    #[derive(Clone)]
    struct MockHandleEvent {
        pub events_received: Arc<std::sync::Mutex<Vec<Event>>>,
    }

    impl MockHandleEvent {
        fn new() -> Self {
            Self {
                events_received: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        pub fn get_events(&self) -> Vec<Event> {
            self.events_received.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl HandleEventTrait for MockHandleEvent {
        async fn handle_event(&self, event: Event) {
            self.events_received.lock().unwrap().push(event);
        }
    }

    #[tokio::test]
    async fn test_add_raft_group() {
        let manager = MultiRaftDriver::new();
        let clone = manager.clone();

        tokio::spawn(async move {
            clone.main_loop().await;
        });

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());
        let mock_handler = MockHandleEvent::new();

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        assert!(matches!(
            manager.dispatch_event(group_id.clone(), Event::HeartbeatTimeout),
            SendEventResult::Success
        ));

        sleep(Duration::from_millis(30)).await;

        assert_eq!(mock_handler.get_events().len(), 1);
    }

    #[tokio::test]
    async fn test_send_event_success() {
        let manager = MultiRaftDriver::new();
        let driver_clone = manager.clone();

        tokio::spawn(async move {
            driver_clone.main_loop().await;
        });

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());
        let mock_handler = MockHandleEvent::new();

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        let result = manager.dispatch_event(group_id.clone(), Event::HeartbeatTimeout);
        assert!(matches!(result, SendEventResult::Success));

        sleep(Duration::from_millis(50)).await;

        let received_events = mock_handler.get_events();
        assert_eq!(received_events.len(), 1);
    }

    #[tokio::test]
    async fn test_send_multiple_events() {
        let manager = MultiRaftDriver::new();
        let driver_clone = manager.clone();

        tokio::spawn(async move {
            driver_clone.main_loop().await;
        });

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());
        let mock_handler = MockHandleEvent::new();

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        assert!(matches!(
            manager.dispatch_event(group_id.clone(), Event::HeartbeatTimeout),
            SendEventResult::Success
        ));
        assert!(matches!(
            manager.dispatch_event(group_id.clone(), Event::ElectionTimeout),
            SendEventResult::Success
        ));

        sleep(Duration::from_millis(50)).await;

        let received_events = mock_handler.get_events();
        assert_eq!(received_events.len(), 2);
    }

    #[tokio::test]
    async fn test_timer_service() {
        let manager = MultiRaftDriver::new();
        let driver_clone = manager.clone();

        tokio::spawn(async move {
            driver_clone.main_loop().await;
        });

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());
        let mock_handler = MockHandleEvent::new();

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        let timers = manager.get_timer_service();
        timers.add_timer(&group_id, Event::HeartbeatTimeout, Duration::from_millis(100));

        sleep(Duration::from_millis(150)).await;

        let received_events = mock_handler.get_events();
        assert_eq!(received_events.len(), 1);
    }

    #[tokio::test]
    async fn test_timer_cancellation() {
        let manager = MultiRaftDriver::new();
        let driver_clone = manager.clone();

        tokio::spawn(async move {
            driver_clone.main_loop().await;
        });

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());
        let mock_handler = MockHandleEvent::new();

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        let timers = manager.get_timer_service();
        let timer_id = timers.add_timer(&group_id, Event::HeartbeatTimeout, Duration::from_millis(100));
        
        // 立即取消
        timers.del_timer(timer_id);

        sleep(Duration::from_millis(150)).await;

        // 定时器已取消，不应收到事件
        let received_events = mock_handler.get_events();
        assert_eq!(received_events.len(), 0);
    }

    #[tokio::test]
    async fn test_send_to_nonexistent_group() {
        let manager = MultiRaftDriver::new();
        let group_id = RaftId::new("nonexistent".to_string(), "node1".to_string());

        let result = manager.dispatch_event(group_id, Event::HeartbeatTimeout);
        assert!(matches!(result, SendEventResult::NotFound));
    }
}

