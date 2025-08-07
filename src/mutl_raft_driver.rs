use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc};

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
                    active_map.insert(target);
                    let _ = self.notify.notify_one(); // 唤醒Worker
                }
            }
            RaftNodeStatus::Pending | RaftNodeStatus::Active => {
                // 已在处理流程中，无需额外操作
            }
        }
        SendEventResult::Success
    }

    // 启动Worker（可启动多个）
    pub async fn main_loop(&self) {
        loop {
            // 处理过期定时器，获取下一个定时器的剩余时间
            // 等待逻辑：有定时器则等待到其到期，否则无限等待唤醒
            if let Some(duration) = self.process_expired_timers().await {
                tokio::select! {
                    _ = tokio::time::sleep(duration) => {
                        continue; // 定时器到期，继续处理
                    },
                    _ = self.notify.notified() => {},
                }
            } else {
                // 无定时器，等待唤醒（新事件或停止信号）
                self.notify.notified().await;
            }

            // 检查停止标志
            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            // 处理活跃节点
            self.process_active_nodes().await;
        }
    }
    // 处理active_map中的所有待处理组
    async fn process_active_nodes(&self) {
        // 一次性取出所有待处理组（减少锁竞争）
        let pendings = {
            let mut active_map = self.active_map.lock().unwrap();
            std::mem::take(&mut *active_map)
        };

        // 逐个处理组
        for node_id in pendings {
            self.process_single_group(node_id).await;
        }
    }

    // 处理单个Raft组的所有事件
    async fn process_single_group(&self, node_id: RaftId) {
        // 获取组核心数据
        let core = {
            let groups = self.groups.lock().unwrap(); // 非mut锁
            groups.get(&node_id).cloned() // 假设RaftGroupCore实现了Clone，或仅获取引用
        };
        let Some(core) = core else {
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
            return; // 已被其他Worker处理
        }

        // 2. 克隆接收端用于处理（避免持有groups锁）

        let handle_event = &core.handle_event;

        // 3. 循环处理所有事件
        loop {
            match core.recevier.lock().await.try_recv() {
                Ok(event) => {
                    // 处理事件并获取响应
                    handle_event.handle_event(event).await;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    // 暂时无消息，尝试转为Idle并检查是否有新消息
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
                        // 转为Idle后检查是否有新消息
                        let has_remaining = !core.recevier.lock().await.is_empty();
                        if has_remaining {
                            // 尝试抢回处理权
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
                                continue; // 抢回成功，继续处理
                            } else {
                                // 抢回失败，由其他Worker处理
                                break;
                            }
                        } else {
                            // 无新消息，保持Idle
                            break;
                        }
                    } else {
                        // 状态已被修改（如其他事件触发了Pending）
                        break;
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    // 通道关闭，清理状态
                    core.status.store(RaftNodeStatus::Idle, Ordering::Release);
                    return;
                }
            }
        }
    }
}
