# 分片分裂解决方案总结

## 1. 概述

分片分裂是分布式存储系统实现水平扩展的核心机制，用于将一个大分片拆分为两个小分片，以解决数据量过大、QPS过高或集群容量扩展的问题。本解决方案基于"快照+增量日志+缓存切换"的设计思路，通过优化快照传输、细化状态机、完善异常处理等方式，实现了高可用、数据一致、可观测和可回滚的分片分裂功能。

## 2. 核心设计目标

- **数据一致性**：分裂过程中不丢失、不重复数据
- **高可用性**：最小化服务中断时间（目标 < 1秒）
- **零停机**：服务在分裂过程中持续可用
- **可观测性**：分裂进度可查询、可监控
- **可回滚**：分裂失败可安全回滚

## 3. 整体流程

### 3.1 六个阶段

```
┌─────────────────────────────────────────────────────────────────┐
│                     分裂前状态                                   │
│  ┌─────────────────────────────────────┐                        │
│  │        shard_0001                   │                        │
│  │    slots: [0, 8192)                 │                        │
│  │    数据量: 10GB                     │                        │
│  └─────────────────────────────────────┘                        │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ trigger_split(split_slot=4096)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 1: 准备阶段 (Preparing)                                    │
│  - 验证源分片状态 (Normal, 未分裂)                              │
│  - 验证分裂点有效性 (split_slot ∈ (start, end))                │
│  - 验证目标分片存在且健康                                        │
│  - 创建 SplitTask                                                │
│  - 更新源/目标分片状态为 Splitting                              │
│  - 在路由表中添加分裂信息                                        │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ target_ready
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 2: 快照传输 (SnapshotTransfer)                            │
│  源分片:                                                          │
│    - 获取 Raft 最新快照 (index=N, term=T)                       │
│    - 保护快照不被删除                                            │
│    - 筛选 slot ∈ [4096, 8192) 的数据                            │
│    - 分块传输到目标分片                                          │
│                                                                  │
│  目标分片:                                                        │
│    - 接收快照数据                                                │
│    - 通过 merge_from_snapshot 加载                               │
│    - 设置 applied_index = N                                     │
│    - 通过 Raft InstallSnapshot 复制到 Follower                  │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ snapshot_done
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 3: 增量追赶 (CatchingUp)                                  │
│  源分片:                                                          │
│    - 筛选 slot ∈ [4096, 8192) 的写操作日志                      │
│    - 按 Raft Index 顺序转发到目标分片                            │
│    - 定期上报进度                                                │
│                                                                  │
│  目标分片:                                                        │
│    - 接收日志条目                                                │
│    - 按顺序 apply 到状态机                                       │
│    - 更新 applied_index                                          │
│                                                                  │
│  追赶指标:                                                        │
│    delay = source.last_index - target.applied_index              │
│    当 delay < catch_up_threshold (100) 时进入下一阶段           │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ delay < threshold
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 4: 请求缓冲 (Buffering)                                     │
│  源分片:                                                          │
│    - 对 slot ∈ [4096, 8192) 的写请求进入缓存队列                │
│    - 不处理，只缓存（避免数据覆盖）                              │
│    - 继续转发增量日志                                            │
│                                                                  │
│  目标分片:                                                        │
│    - 继续追赶最后几条日志                                        │
│    - 直到 applied_index == source.last_index                    │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ caught_up
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 5: 路由切换 (Switching)                                     │
│  Pilot:                                                           │
│    - 原子更新路由表版本                                          │
│    - 更新槽位映射:                                               │
│      * shard_0001: [0, 4096)                                    │
│      * shard_0002: [4096, 8192)                                 │
│    - 通知所有 Node 路由表更新                                    │
│                                                                  │
│  源分片:                                                          │
│    - 对缓存的请求批量返回 MOVED                                  │
│    - 格式: MOVED <slot> <target_addr>                           │
│                                                                  │
│  客户端:                                                          │
│    - 收到 MOVED 后重定向到目标分片                               │
│    - 重新执行请求                                                │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ switched
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 6: 清理 (Cleanup)                                           │
│  源分片:                                                          │
│    - 删除 slot ∈ [4096, 8192) 的数据（可延迟执行）              │
│    - 清除分裂状态                                                │
│    - 状态恢复为 Normal                                           │
│    - 释放快照和日志保护                                          │
│                                                                  │
│  Pilot:                                                           │
│    - 更新 SplitTask 状态为 Completed                             │
│    - 从路由表移除分裂信息                                        │
│    - 更新分片元数据                                              │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     分裂后状态                                   │
│  ┌─────────────────┐  ┌─────────────────┐                      │
│  │  shard_0001     │  │  shard_0002     │                      │
│  │ slots:[0,4096)  │  │ slots:[4096,8192)│                     │
│  │ 数据量: ~5GB    │  │ 数据量: ~5GB    │                      │
│  │ 状态: Normal    │  │ 状态: Normal    │                      │
│  └─────────────────┘  └─────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

## 4. 关键技术设计

### 4.1 快照传输机制

#### 4.1.1 复用 Raft 快照

**核心原则**: 必须复用 Raft 协议生成的标准快照，确保与后续日志的连贯性

**设计要点**:
- Raft 快照包含 `index` 和 `term`，与日志条目有明确的对应关系
- 快照的 `index` 是后续增量日志的起始点
- 如果使用独立快照，无法保证与 Raft 日志的连贯性

**实现**:
```rust
// 获取 Raft 层最新快照
let raft_snapshot = raft_state.get_latest_snapshot().await?;
let snapshot_index = raft_snapshot.index;  // 后续日志的起始点
let snapshot_term = raft_snapshot.term;
```

#### 4.1.2 筛选槽位范围

**问题**: Raft 快照包含整个分片的数据，但分裂只需要部分槽位的数据

**解决方案**: 在 StateMachine 层直接遍历数据，只序列化目标槽位范围

**实现**:
```rust
fn create_split_snapshot(
    &self,
    slot_start: u32,
    slot_end: u32,
    total_slots: u32,
) -> Result<Vec<u8>, String> {
    // 直接遍历数据，只序列化目标槽位范围的数据
    let mut entries = Vec::new();
    
    for (key, entry) in &self.data {
        let slot = calculate_slot(key, total_slots);
        if slot >= slot_start && slot < slot_end {
            entries.push(SnapshotEntry {
                key: key.clone(),
                value: entry.value.clone(),
                expire_at: entry.expire_at,
            });
        }
    }
    
    bincode::serialize(&SnapshotData { entries })
}
```

#### 4.1.3 快照保护机制

**问题**: 传输期间快照可能被删除，日志可能被截断

**解决方案**:
```rust
struct SnapshotProtection {
    in_use_snapshots: HashSet<u64>,  // 正在使用的快照 index
    protected_log_ranges: Vec<(u64, u64)>,  // 受保护的日志范围
}

// 标记快照为使用中，防止被删除
raft_state.mark_snapshot_in_use(snapshot_index).await?;

// 防止日志被截断
raft_state.protect_log_range(0, snapshot_index).await?;
```

#### 4.1.4 传输目标选择

**决策**: 发送给目标分片的 Leader

**理由**:
- Leader 是唯一可以写入的节点
- 其他节点通过 Raft 的 `InstallSnapshot` 机制自动复制
- 简化实现，避免多节点协调

**流程**:
```
源分片 Leader
    │
    │ 1. 获取 Raft 快照
    │ 2. 筛选槽位范围
    │ 3. 发送到目标分片 Leader
    │
    ▼
目标分片 Leader
    │
    │ 1. 接收快照
    │ 2. 调用 merge_from_snapshot
    │ 3. 设置 applied_index = snapshot_index
    │ 4. 通过 Raft InstallSnapshot 复制到 Follower
    │
    ▼
目标分片所有节点
```

#### 4.1.5 快照加载机制

**方案**: Leader 直接调用 `StateMachine::merge_from_snapshot()`

**实现**:
```rust
// 目标 Leader 加载快照
state_machine.store().merge_from_snapshot(&snapshot_data)?;
state_machine.set_applied_index(snapshot_index).await?;

// 通过 Raft InstallSnapshot 复制到 Follower
for follower in target_followers {
    send_install_snapshot(follower, snapshot_data.clone()).await?;
}
```

### 4.2 增量日志转发机制

#### 4.2.1 日志筛选

**筛选条件**: `slot >= split_slot` 的写操作

**筛选位置**: 在 Raft 层或应用层筛选

**顺序保证**: 严格按照 Raft Index 顺序转发

#### 4.2.2 可靠转发机制

**设计**: 源分片作为"伪领导者"，使用类似 AppendEntries 的机制

**实现**:
```rust
struct LogForwarder {
    next_index: u64,           // 下一个要转发的日志索引
    match_index: u64,          // 目标分片已确认的日志索引
    pending_logs: Vec<LogEntry>, // 待转发的日志
}

// 筛选并转发日志
for log_entry in raft_log.entries_from(snapshot_index + 1) {
    if should_forward(&log_entry, split_slot) {
        forward_to_target(target_shard, log_entry).await?;
        // 等待目标分片确认
        wait_for_ack().await?;
    }
}
```

#### 4.2.3 进度追踪

**延迟计算**: `delay = source.last_index - target.applied_index`

**上报机制**: Node 定期上报 `applied_index` 到 Pilot

**进入条件**: 当 `delay < catch_up_threshold` (默认 100) 时进入缓冲阶段

### 4.3 请求缓冲机制

#### 4.3.1 缓冲触发

**条件**: `delay < catch_up_threshold`

**范围**: 对 `slot >= split_slot` 的写请求进入缓存队列

**行为**: 不处理请求，只缓存（避免数据覆盖）

#### 4.3.2 缓冲管理

**最大缓冲数**: `buffer_max_size` (10000)

**超时时间**: `buffer_timeout` (5秒)

**超时处理**: 返回 `TRYAGAIN`，触发回滚

**实现**:
```rust
struct BufferedRequest {
    command: Command,
    slot: u32,
    timestamp: Instant,
    response_tx: oneshot::Sender<Result<RespValue, String>>,
}

struct SplitBuffer {
    requests: VecDeque<BufferedRequest>,
    max_size: usize,
    timeout: Duration,
}
```

### 4.4 路由切换机制

#### 4.4.1 原子路由更新

**版本号机制**: 路由表版本号递增，确保原子性

**更新内容**:
- 更新槽位映射: `shard_0001: [0, 4096)`, `shard_0002: [4096, 8192)`
- 更新分片节点映射
- 移除分裂信息

**通知机制**: 通过 watch 机制通知所有 Node

#### 4.4.2 批量 MOVED 响应

**触发时机**: 路由切换完成后

**响应格式**: `MOVED <slot> <target_addr>`

**批量处理**: 对缓存队列中的所有请求批量返回 MOVED

**实现**:
```rust
// Pilot 原子更新路由表
routing_table.assign_slots("shard_0001", 0, split_slot);
routing_table.assign_slots("shard_0002", split_slot, source.end);
routing_table.remove_splitting_shard("shard_0001");
routing_table.bump_version();  // 版本号递增
pilot.notify_routing_watchers().await;  // 通知所有 Node

// Node 处理缓存请求
for buffered_req in buffer.requests {
    let slot = calculate_slot(&buffered_req.command.get_key());
    let target_addr = routing_table.get_target_addr(slot);
    buffered_req.response_tx.send(
        Err(format!("MOVED {} {}", slot, target_addr))
    ).ok();
}
```

## 5. 数据一致性保证

### 5.1 快照 + 增量日志

- **快照传输**: 传输历史数据（到 `snapshot_index`）
- **增量日志**: 按顺序重放 `snapshot_index + 1` 之后的所有写操作
- **保证**: 数据完整性和顺序性

### 5.2 请求缓冲

- **目的**: 在路由切换前缓存新请求，避免数据覆盖
- **机制**: 切换后批量返回 MOVED，客户端重定向

### 5.3 原子路由更新

- **机制**: 路由表版本号机制确保所有 Node 同时切换
- **保证**: 避免路由不一致导致的数据丢失或重复

## 6. 异常处理

### 6.1 追赶太慢

**检测**: 持续 5 分钟 `delay > 1000`

**处理**: 
- 限流源分片写入
- 增加目标分片资源
- 取消分裂，稍后重试

### 6.2 缓存超时

**检测**: 缓存时间 > `buffer_timeout`

**处理**: 回滚，返回 `TRYAGAIN`

### 6.3 目标分片崩溃

**处理**: 源分片数据完整，清理分裂状态，稍后重试

### 6.4 网络分区

**处理**: 超时检测，等待恢复，超过阈值则回滚

### 6.5 快照传输失败

**处理**: 重试或回滚，释放保护机制

## 7. 性能优化

### 7.1 快照传输优化

- **增量快照**: 只传输自上次快照以来的变更数据
- **并行传输**: 源分片并行生成，目标分片并行接收
- **压缩校验**: 使用 LZ4 压缩和 CRC32 校验

### 7.2 源分片负载保护

- **限流机制**: 对快照生成、日志转发实施限流
- **后台执行**: 分裂相关操作在后台以低优先级执行

### 7.3 大规模集群优化

- **分层 Pilot 架构**: 全局 Pilot + 区域 Pilot
- **智能调度**: 避免同时执行过多分裂任务

## 8. 状态机设计

### 8.1 状态定义

```rust
pub enum SplitStatus {
    Preparing,              // 准备阶段
    SnapshotTransfer,       // 快照传输
    CatchingUp,            // 增量追赶
    Buffering,             // 请求缓冲
    Switching,             // 路由切换
    Cleanup,               // 清理阶段
    Completed,             // 已完成
    Failed(String),        // 失败
    Cancelled,             // 已取消
}
```

### 8.2 状态转换

```
Normal → Preparing → SnapshotTransfer → CatchingUp → Buffering → Switching → Cleanup → Normal
                                                                      ↓
                                                                  Failed/Cancelled
```

### 8.3 支持暂停/恢复

- 允许在任意状态暂停分裂
- 后续可恢复执行
- 提高灵活性，应对临时资源紧张

## 9. 关键数据结构

### 9.1 SplitTask

```rust
pub struct SplitTask {
    pub id: String,
    pub source_shard: ShardId,
    pub target_shard: ShardId,
    pub split_slot: u32,
    pub status: SplitStatus,
    pub progress: SplitProgress,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

### 9.2 SplitProgress

```rust
pub struct SplitProgress {
    pub snapshot_done: bool,           // 快照是否完成
    pub snapshot_index: u64,          // 快照时的 Raft Index
    pub source_last_index: u64,       // 源分片最新 Index
    pub target_applied_index: u64,    // 目标分片已应用 Index
    pub buffered_requests: usize,     // 缓存的请求数
}

// 计算延迟
pub fn delay(&self) -> u64 {
    self.source_last_index.saturating_sub(self.target_applied_index)
}
```

### 9.3 SnapshotProtection

```rust
struct SnapshotProtection {
    in_use_snapshots: HashSet<u64>,  // 正在使用的快照 index
    protected_log_ranges: Vec<(u64, u64)>,  // 受保护的日志范围
}
```

## 10. 实现状态

### 10.1 已完成 ✅

1. **数据结构**
   - `SplitTask`, `SplitStatus`, `SplitProgress`
   - `ShardSplitState`, `SplittingShardInfo`
   - `SplitPhase` 枚举

2. **Pilot 端**
   - `SplitManager` 分裂协调器
   - HTTP API（触发分裂、查询进度、取消分裂）
   - 路由表分裂状态同步

3. **Node 端**
   - 路由表分裂状态检测
   - `MOVED` / `TRYAGAIN` 响应处理
   - `should_move_for_split` 逻辑

### 10.2 待实现 🚧（高优先级）

1. **快照传输**
   - [ ] 在 RaftState 中添加快照保护机制
   - [ ] 实现 `create_split_snapshot` 槽位范围筛选
   - [ ] 实现分裂快照传输 gRPC 服务
   - [ ] 实现目标分片接收和组装逻辑
   - [ ] 实现通过 Raft InstallSnapshot 复制到 Follower
   - [ ] 实现保护释放机制

2. **增量日志转发**
   - [ ] 源分片筛选 `slot >= split_slot` 的日志
   - [ ] 实现可靠日志转发机制
   - [ ] 转发到目标分片 Raft 组
   - [ ] 目标分片按顺序 apply
   - [ ] 实现断点续传

3. **请求缓冲队列**
   - [ ] 实现 `SplitBuffer` 缓存机制
   - [ ] 设置最大缓冲数和超时
   - [ ] 切换完成后批量返回 MOVED

4. **进度上报**
   - [ ] Node 定期上报 `applied_index` 到 Pilot
   - [ ] Pilot 计算延迟并更新 `SplitProgress`
   - [ ] 支持 API 查询分裂进度

## 11. 关键技术决策总结

### 11.1 快照传输

**决策**: 复用 Raft 快照，筛选槽位范围，发送给目标 Leader

**理由**:
- 保证与后续日志的连贯性（`snapshot_index` 是日志起始点）
- Leader 负责写入，其他节点通过 Raft 自动复制
- 简化实现，避免多节点协调

### 11.2 日志转发

**决策**: 源分片筛选并转发日志，目标分片按顺序 apply

**理由**:
- 保证数据顺序性
- 支持断点续传
- 可靠性保证（确认机制）

### 11.3 请求缓冲

**决策**: 在路由切换前缓存目标范围的写请求

**理由**:
- 避免数据覆盖（新写入不会覆盖目标分片数据）
- 实现零停机切换
- 批量返回 MOVED，减少客户端重试

### 11.4 路由切换

**决策**: 原子更新路由表版本号，通过 watch 机制通知 Node

**理由**:
- 版本号机制确保原子性
- Watch 机制确保所有 Node 同步更新
- 避免路由不一致

## 12. 总结

本解决方案采用**快照 + 增量日志 + 请求缓冲 + 原子路由切换**的策略，实现了：

1. **数据一致性**: 通过快照和增量日志保证数据完整性
2. **零停机**: 通过请求缓冲和 MOVED 响应实现 < 1秒中断
3. **可观测性**: 通过进度追踪和监控指标提供可观测性
4. **可回滚**: 通过完善的异常处理机制支持安全回滚
5. **高性能**: 通过优化设计减少对源分片的影响

当前框架已搭建完成，核心的数据传输和同步逻辑待实现。下一步应优先实现快照传输和增量日志转发机制。
