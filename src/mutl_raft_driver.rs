use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc};
use tracing::info;

// Raft组状态枚举（原子操作安全）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftNodeStatus {
    Idle,    // 空闲状态（无未处理消息，未被Worker处理）
    Pending, // 待处理状态（有未处理消息，已加入active_map）
    Active,  // 活跃状态（正在被Worker处理）
}

// Raft事件类型（示例）
use std::sync::atomic::AtomicU8;

use crate::{Event, RaftId, TimerId};

// 原子RaftGroupStatus包装器
pub struct AtomicRaftNodeStatus(AtomicU8);

impl AtomicRaftNodeStatus {
    pub fn new(status: RaftNodeStatus) -> Self {
        Self(AtomicU8::new(status as u8))
    }

    pub fn load(&self, order: Ordering) -> RaftNodeStatus {
        match self.0.load(order) {
            0 => RaftNodeStatus::Idle,
            1 => RaftNodeStatus::Pending,
            2 => RaftNodeStatus::Active,
            _ => panic!("Invalid RaftGroupStatus value"),
        }
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
            Ok(val) => Ok(match val {
                0 => RaftNodeStatus::Idle,
                1 => RaftNodeStatus::Pending,
                2 => RaftNodeStatus::Active,
                _ => panic!("Invalid RaftGroupStatus value"),
            }),
            Err(val) => Err(match val {
                0 => RaftNodeStatus::Idle,
                1 => RaftNodeStatus::Pending,
                2 => RaftNodeStatus::Active,
                _ => panic!("Invalid RaftGroupStatus value"),
            }),
        }
    }
}

#[derive(Debug)]
struct TimerEvent {
    timer_id: TimerId,
    node_id: RaftId,
    event: Event,
    trigger_time: Instant,
    delay: Duration,
}

// Implement ordering for TimerEvent so it can be used in BinaryHeap (min-heap by trigger_time)
impl PartialEq for TimerEvent {
    fn eq(&self, other: &Self) -> bool {
        self.trigger_time.eq(&other.trigger_time)
    }
}

impl Eq for TimerEvent {}

impl PartialOrd for TimerEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Reverse order for min-heap
        other.trigger_time.partial_cmp(&self.trigger_time)
    }
}

impl Ord for TimerEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap
        other.trigger_time.cmp(&self.trigger_time)
    }
}

#[async_trait::async_trait]
pub trait HandleEventTrait: Send + Sync {
    async fn handle_event(&self, event: Event);
}

// Raft组核心数据（状态+通道+业务状态）
struct RaftGroupCore {
    status: AtomicRaftNodeStatus,         // 原子状态管理
    sender: mpsc::UnboundedSender<Event>, // 事件发送端
    recevier: tokio::sync::Mutex<mpsc::UnboundedReceiver<Event>>, // 事件接收端
    handle_event: Box<dyn HandleEventTrait>,
}

pub struct MultiRaftDriverInner {
    timer_service: Timers,
    groups: Mutex<HashMap<RaftId, Arc<RaftGroupCore>>>, // 所有Raft组
    active_map: Mutex<HashSet<RaftId>>,                 // 待处理组集合
    notify: Notify,                                     // Worker唤醒信号
    stop: AtomicBool,                                   // 停止标志
}

// Multi-Raft驱动核心
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

pub struct TimerInner {
    timer_id_counter: AtomicU64,
    timer_heap: Mutex<BinaryHeap<TimerEvent>>,
}

#[derive(Clone)]
pub struct Timers {
    inner: Arc<TimerInner>,
}

impl Timers {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TimerInner {
                timer_id_counter: AtomicU64::new(0),
                timer_heap: Mutex::new(BinaryHeap::new()),
            }),
        }
    }
}

impl Deref for Timers {
    type Target = TimerInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Timers {
    pub fn add_timer(&self, node_id: RaftId, event: Event, delay: Duration) -> TimerId {
        let timer_id = self.timer_id_counter.fetch_add(1, Ordering::Relaxed);
        let trigger_time = Instant::now() + delay;
        let timer_event = TimerEvent {
            timer_id,
            node_id,
            event,
            trigger_time,
            delay,
        };
        self.timer_heap.lock().unwrap().push(timer_event);
        timer_id
    }

    pub fn del_timer(&self, timer_id: TimerId) {
        let mut timer_heap = self.timer_heap.lock().unwrap();
        timer_heap.retain(|timer_event| timer_event.timer_id != timer_id);
    }

    fn process_expired_timers(&self) -> (Vec<TimerEvent>, Option<Duration>) {
        let now = Instant::now();
        let mut events = Vec::new();
        let mut timer_heap = self.timer_heap.lock().unwrap();

        while let Some(timer_event) = timer_heap.peek() {
            if timer_event.trigger_time <= now {
                let event = timer_heap.pop().unwrap();
                events.push(event);
            } else {
                // 未来时间 - 当前时间 = 正的剩余时间
                return (events, Some(timer_event.trigger_time - now));
            }
        }
        (events, None)
    }
}

#[derive(Debug)]
pub enum SendEventResult {
    Success,
    NotFound,
    SendFailed,
}

impl MultiRaftDriver {
    // 创建新的MultiRaftDriver
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MultiRaftDriverInner {
                timer_service: Timers::new(),
                groups: Mutex::new(HashMap::new()),
                active_map: Mutex::new(HashSet::new()),
                notify: Notify::new(),
                stop: AtomicBool::new(false),
            }),
        }
    }

    pub fn get_timer_service(&self) -> Timers {
        self.inner.timer_service.clone()
    }

    // 添加新的Raft组
    pub fn add_raft_group(&self, group_id: RaftId, handle_event: Box<dyn HandleEventTrait>) {
        let (tx, rx) = mpsc::unbounded_channel(); // 事件通道（缓冲区大小100）
        let core = RaftGroupCore {
            status: AtomicRaftNodeStatus::new(RaftNodeStatus::Idle),
            sender: tx,
            recevier: tokio::sync::Mutex::new(rx), // 使用Mutex包装接收端
            handle_event,
        };
        self.groups.lock().unwrap().insert(group_id, Arc::new(core));
    }

    pub fn del_raft_group(&self, group_id: &RaftId) {
        let mut groups = self.groups.lock().unwrap();
        if let Some(_core) = groups.remove(group_id) {
            info!("Removed Raft group: {}", group_id);
        }
    }

    async fn process_expired_timers(&self) -> Option<Duration> {
        let (events, duration) = self.timer_service.process_expired_timers();
        for event in events {
            let result = self.send_event(event.node_id.clone(), event.event.clone());
            if !matches!(result, SendEventResult::Success) {
                log::error!(
                    "send timer event failed {:?}, node_id: {}, event: {:?}, delay: {:?}",
                    result,
                    event.node_id,
                    event.event,
                    event.delay
                );
            }
        }
        duration
    }

    // 向指定Raft组发送事件
    pub fn send_event(&self, target: RaftId, event: Event) -> SendEventResult {
        let groups = self.groups.lock().unwrap();
        let Some(core) = groups.get(&target) else {
            return SendEventResult::NotFound; // 组不存在
        };

        // 1. 先发送事件到通道（确保消息不丢失）
        if core.sender.send(event).is_err() {
            return SendEventResult::SendFailed; // 通道已关闭
        }

        // 2. 根据当前状态决定是否需要激活组
        let current_status = core.status.load(Ordering::Acquire);
        match current_status {
            RaftNodeStatus::Idle => {
                // 从Idle -> Pending，加入active_map并唤醒Worker
                let swapped = core
                    .status
                    .compare_exchange(
                        RaftNodeStatus::Idle,
                        RaftNodeStatus::Pending,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok();

                if swapped {
                    let mut active_map = self.active_map.lock().unwrap();
                    active_map.insert(target.clone());
                    self.notify.notify_one();

                    //    info!("push target {} into active map", target);
                }
            }
            RaftNodeStatus::Pending | RaftNodeStatus::Active => {
                //info!("target is active")
            }
        }
        SendEventResult::Success
    }

    // 启动Worker（可启动多个）
    pub async fn main_loop(&self) {
        info!("Starting main loop for MultiRaftDriver");

        loop {
            // 处理过期定时器，获取下一个定时器的剩余时间
            // 等待逻辑：有定时器则等待到其到期，否则无限等待唤醒
            if let Some(duration) = self.process_expired_timers().await {
                tokio::select! {
                        _ = tokio::time::sleep(duration) => {
                  //          info!("Timer expired, processing expired timers");
                            continue;
                        },
                        _ = self.notify.notified() => {
                //            info!("Worker notified, processing active nodes");
                        },
                    }
            } else {
                self.notify.notified().await;
                //info!("Worker notified (no timer), processing active nodes");
            }

            // 检查停止标志
            if self.stop.load(Ordering::Relaxed) {
                info!("Stop signal received, exiting main loop");
                break;
            }

            // 处理活跃节点
            self.process_active_nodes().await;
        }
    }
    // 处理active_map中的所有待处理组
    async fn process_active_nodes(&self) {
        let pendings = {
            let mut active_map = self.active_map.lock().unwrap();
            std::mem::take(&mut *active_map)
        };

        for node_id in pendings {
            let self_clone = self.clone();
            tokio::spawn(async move {
                self_clone.process_single_group(node_id).await;
            });
        }
    }

    // 处理单个Raft组的所有事件
    async fn process_single_group(&self, node_id: RaftId) {
        //info!("Starting to process single group: {}", node_id);

        let core = {
            let groups = self.groups.lock().unwrap(); // 非mut锁
            groups.get(&node_id).cloned() // 假设RaftGroupCore实现了Clone，或仅获取引用
        };
        let Some(core) = core else {
            info!("Group {} not found, skipping processing", node_id);
            return;
        }; // 组已删除
        // 1. 尝试获取处理权（Pending -> Active）
        let swapped = core
            .status
            .compare_exchange(
                RaftNodeStatus::Pending,
                RaftNodeStatus::Active,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();

        if !swapped {
            info!(
                "Group {} status not pending, current status: {:?}, skipping processing",
                node_id,
                core.status.load(Ordering::Acquire)
            );
            return; // 已被其他Worker处理
        }

        // 2. 克隆接收端用于处理（避免持有groups锁）

        let handle_event = &core.handle_event;

        // 3. 循环处理所有事件

        let mut rx = core.recevier.lock().await;
        loop {
            match rx.try_recv() {
                Ok(event) => {
                    //        info!("Processing event for node {}: {:?}", node_id, event);
                    handle_event.handle_event(event).await;
                    //      info!("Completed processing event for node {}", node_id);
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    // info!(
                    //     "No more events in queue for node {}, attempting to transition to idle",
                    //     node_id
                    // );

                    let swapped = core
                        .status
                        .compare_exchange(
                            RaftNodeStatus::Active,
                            RaftNodeStatus::Idle,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok();

                    if swapped {
                        // info!(
                        //     "Successfully transitioned node {} from Active to Idle",
                        //     node_id
                        // );
                        let has_remaining = !rx.is_empty();
                        // info!(
                        //     "Checked for remaining messages in node {} queue: has_remaining = {}",
                        //     node_id, has_remaining
                        // );
                        if has_remaining {
                            let swapped = core
                                .status
                                .compare_exchange(
                                    RaftNodeStatus::Idle,
                                    RaftNodeStatus::Active,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_ok();
                            if swapped {
                                info!(
                                    "Successfully reacquired processing rights for node {}, continuing to process",
                                    node_id
                                );
                                continue;
                            } else {
                                info!(
                                    "Failed to reacquire processing rights for node {}, another worker took over",
                                    node_id
                                );
                                break;
                            }
                        } else {
                            // info!(
                            //     "No remaining messages for node {}, transitioning to idle",
                            //     node_id
                            // );
                            break;
                        }
                    } else {
                        info!(
                            "Failed to transition node {} from Active to Idle, another state change occurred",
                            node_id
                        );
                        break;
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    core.status.store(RaftNodeStatus::Idle, Ordering::Release);
                    info!("Channel disconnected for node {}, marking as idle", node_id);
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*; // 导入被测试模块的所有内容
    use crate::{Event, RaftId}; // 导入需要的类型
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use tokio::{self, time::sleep}; // 确保 tokio 用于异步测试

    #[derive(Clone)] // 为了方便在测试中共享
    struct MockHandleEvent {
        pub events_received: Arc<Mutex<Vec<Event>>>,
    }

    impl MockHandleEvent {
        fn new() -> Self {
            Self {
                events_received: Arc::new(Mutex::new(Vec::new())),
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

    // --- 测试用例 ---

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
            manager.send_event(group_id.clone(), Event::HeartbeatTimeout),
            SendEventResult::Success
        )); // 应该成功，说明组存在

        sleep(Duration::from_millis(30)).await;

        assert!(mock_handler.get_events().len() == 1);
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

        let test_event = Event::HeartbeatTimeout; // 使用一个简单的事件进行测试

        let result = manager.send_event(group_id.clone(), test_event.clone());

        assert!(
            matches!(result, SendEventResult::Success),
            "Sending event to existing group should succeed"
        );

        // 给 Worker 一点时间来处理事件
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // 验证事件是否被处理（即传递给了 MockHandleEvent）
        let received_events = mock_handler.get_events();
        assert_eq!(
            received_events.len(),
            1,
            "Handler should have received one event"
        );
        // Remove the equality check since Event doesn't implement PartialEq
        // Just verify that an event was received
    }

    #[tokio::test]
    async fn test_send_event_failure() {
        let manager = MultiRaftDriver::new();
        let driver_clone = manager.clone();
        tokio::spawn(async move {
            driver_clone.main_loop().await;
        });

        let mock_handler = MockHandleEvent::new();

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        // 2. 发送事件
        let event1 = Event::HeartbeatTimeout;
        let event2 = Event::ElectionTimeout; // 发送两个事件以测试 Pending 状态的持续
        assert!(matches!(
            manager.send_event(group_id.clone(), event1.clone()),
            SendEventResult::Success
        ));
        assert!(matches!(
            manager.send_event(group_id.clone(), event2.clone()),
            SendEventResult::Success
        )); // 快速连续发送
        // 4. 验证事件被处理

        // 给 Worker 一点时间来处理事件
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let received_events = mock_handler.get_events();
        assert_eq!(
            received_events.len(),
            2,
            "Handler should have received two events"
        );
        // Note: We can't compare Event values directly since Event doesn't implement PartialEq
        // Just verify that we received the expected number of events
        // 这个测试主要验证事件被处理，Worker 机制在运行。
    }

    #[tokio::test]
    async fn test_timer_service_integration() {
        use tokio::time::Duration;
        let manager = MultiRaftDriver::new();

        let driver_clone = manager.clone();
        tokio::spawn(async move {
            driver_clone.main_loop().await;
        });

        let mock_handler = MockHandleEvent::new();

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        // 注册一个定时器，100ms 后触发
        let test_event = Event::HeartbeatTimeout;
        let delay = Duration::from_millis(100);

        let timers = manager.get_timer_service();
        timers.add_timer(group_id, test_event, delay);

        tokio::time::sleep(Duration::from_millis(150)).await;

        // 确保等待没有超时
        let received_events = mock_handler.get_events();
        assert_eq!(
            received_events.len(),
            1,
            "Handler should have received two events"
        );
    }
}
