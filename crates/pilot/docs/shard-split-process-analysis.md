# Shard Slot 分裂过程详细分析

## 概述

Shard Slot 分裂是将一个大分片（Shard）按照槽位（Slot）范围拆分为两个小分片的过程。这是分布式存储系统实现水平扩展的核心机制。

## 分裂目标

- **数据一致性**：分裂过程中不丢失、不重复数据
- **高可用性**：最小化服务中断时间（目标 < 1秒）
- **零停机**：服务在分裂过程中持续可用
- **可观测性**：分裂进度可查询、可监控
- **可回滚**：分裂失败可安全回滚

## 整体流程概览

```
┌─────────────────────────────────────────────────────────────────┐
│                     分裂前状态                                   │
│  ┌─────────────────────────────────────┐                        │
│  │        shard_0001                   │                        │
│  │    slots: [0, 8192)                 │                        │
│  │    数据量: 10GB                     │                        │
│  │    状态: Normal                     │                        │
│  └─────────────────────────────────────┘                        │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ trigger_split(split_slot=4096)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   阶段 1: 准备阶段 (Preparing)                    │
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
│             阶段 2: 快照传输 (SnapshotTransfer)                  │
│  源分片:                                                          │
│    - 创建快照 (snapshot_index = N)                              │
│    - 筛选 slot ∈ [4096, 8192) 的数据                            │
│    - 分块传输到目标分片                                          │
│                                                                  │
│  目标分片:                                                        │
│    - 接收快照数据                                                │
│    - 加载到状态机                                                │
│    - 设置 applied_index = N                                     │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ snapshot_done
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│           阶段 3: 增量追赶 (CatchingUp)                          │
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
│             阶段 4: 请求缓冲 (Buffering)                         │
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
│             阶段 5: 路由切换 (Switching)                          │
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
│             阶段 6: 清理 (Cleanup)                               │
│  源分片:                                                          │
│    - 删除 slot ∈ [4096, 8192) 的数据（可延迟执行）              │
│    - 清除分裂状态                                                │
│    - 状态恢复为 Normal                                           │
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

## 详细阶段分析

### 阶段 1: 准备阶段 (Preparing)

**参与者**: Pilot, 源分片, 目标分片

**步骤**:

1. **Pilot 验证源分片**
   - 分片存在且状态为 `Normal`
   - 分片未处于分裂状态
   - 分裂点 `split_slot` 在分片范围内: `start < split_slot < end`

2. **Pilot 验证目标分片**
   - 目标分片已存在（必须预先创建）
   - 目标分片状态为 `Normal` 且健康（有 leader，满足副本数）
   - 目标分片未处于分裂状态
   - 目标分片的 `key_range` 匹配分裂点: `[split_slot, source.end)`

3. **创建 SplitTask**
   ```rust
   SplitTask {
       id: "split_shard_0001_4096_1234567890",
       source_shard: "shard_0001",
       target_shard: "shard_0002",
       split_slot: 4096,
       status: SplitStatus::Preparing,
       progress: SplitProgress::default(),
   }
   ```

4. **更新分片状态**
   - 源分片: 设置 `split_state`，状态变为 `Splitting`
   - 目标分片: 设置 `split_state`，状态变为 `Splitting`

5. **更新路由表**
   - 在 `routing_table.splitting_shards` 中添加分裂信息
   - Node 通过路由表同步获取分裂状态

**关键数据结构**:
```rust
ShardSplitState {
    split_task_id: String,
    split_slot: u32,
    role: SplitRole,  // Source or Target
}

SplittingShardInfo {
    source_shard: ShardId,
    target_shard: ShardId,
    split_slot: u32,
    source_range: KeyRange,
    target_nodes: Vec<NodeId>,
    phase: SplitPhase::Preparing,
}
```

### 阶段 2: 快照传输 (SnapshotTransfer)

**参与者**: 源分片, 目标分片

**步骤**:

1. **源分片创建快照**
   - 调用 `StateMachine::create_split_snapshot(slot_start, slot_end)`
   - 筛选 `slot ∈ [split_slot, source.end)` 的数据
   - 记录快照时的 Raft Index: `snapshot_index = N`

2. **快照传输**
   - 分块传输（chunk_size = 1MB）
   - 传输协议: gRPC 或 HTTP
   - 包含元数据: `snapshot_index`, `slot_range`, `data`

3. **目标分片加载快照**
   - 调用 `StateMachine::merge_from_snapshot(snapshot)`
   - 合并到现有数据（不清空）
   - 设置 `applied_index = snapshot_index`

**关键方法**:
```rust
// redisstore/src/traits.rs
fn create_split_snapshot(
    &self,
    slot_start: u32,
    slot_end: u32,
    total_slots: u32,
) -> Result<Vec<u8>, String>;

fn merge_from_snapshot(&self, snapshot: &[u8]) -> Result<usize, String>;
```

**数据流**:
```
源分片 StateMachine
    │
    │ create_split_snapshot(4096, 8192, 16384)
    │ 筛选: slot_for_key(key) ∈ [4096, 8192)
    │
    ▼
快照数据 (序列化)
    │
    │ 分块传输
    │
    ▼
目标分片 StateMachine
    │
    │ merge_from_snapshot(snapshot)
    │ applied_index = snapshot_index
    │
    ▼
数据已加载
```

### 阶段 3: 增量追赶 (CatchingUp)

**参与者**: 源分片, 目标分片

**步骤**:

1. **源分片筛选日志**
   - 从 `snapshot_index + 1` 开始
   - 筛选 `slot >= split_slot` 的写操作
   - 按 Raft Index 顺序转发

2. **目标分片应用日志**
   - 接收日志条目
   - 按顺序 apply 到状态机
   - 更新 `applied_index`

3. **进度追踪**
   - 定期计算延迟: `delay = source.last_index - target.applied_index`
   - 上报进度到 Pilot
   - Pilot 更新 `SplitProgress`

**追赶指标**:
```rust
SplitProgress {
    snapshot_done: true,
    snapshot_index: 1000,
    source_last_index: 1500,
    target_applied_index: 1450,
    delay: 50,  // 1500 - 1450
}
```

**进入下一阶段条件**:
- `delay < catch_up_threshold` (默认 100)
- 表示目标分片已接近源分片的最新状态

**数据流**:
```
源分片 Raft Log
    │
    │ Index: N+1, N+2, N+3, ...
    │ 筛选: slot >= split_slot
    │
    ▼
筛选后的日志条目
    │
    │ 转发
    │
    ▼
目标分片 Raft Group
    │
    │ 按顺序 apply
    │ applied_index++
    │
    ▼
数据同步中...
```

### 阶段 4: 请求缓冲 (Buffering)

**参与者**: 源分片, 目标分片, 客户端

**步骤**:

1. **源分片进入缓冲模式**
   - 检测到 `delay < threshold`
   - 对 `slot >= split_slot` 的写请求进入缓存队列
   - 不处理请求，只缓存（避免数据覆盖）

2. **目标分片继续追赶**
   - 继续接收增量日志
   - 直到 `applied_index == source.last_index`

3. **请求缓存**
   ```rust
   struct BufferedRequest {
       command: Command,
       slot: u32,
       timestamp: Instant,
   }
   ```

**关键机制**:
- **为什么需要缓冲**: 防止在路由切换前，新写入的数据覆盖目标分片的数据
- **缓冲超时**: 如果超过 `buffer_timeout` (5秒)，返回 `TRYAGAIN`
- **最大缓冲数**: `buffer_max_size` (10000)

**请求流**:
```
客户端写请求 (slot >= split_slot)
    │
    │
    ▼
源分片 Node
    │
    │ 检查: should_start_buffering?
    │
    ▼
缓存队列 (不处理)
    │
    │ 等待路由切换
    │
    ▼
切换后返回 MOVED
```

### 阶段 5: 路由切换 (Switching)

**参与者**: Pilot, 源分片, 目标分片, 客户端

**步骤**:

1. **Pilot 原子更新路由表**
   ```rust
   // 更新槽位映射
   routing_table.assign_slots("shard_0001", 0, 4096);
   routing_table.assign_slots("shard_0002", 4096, 8192);
   
   // 更新分片节点映射
   routing_table.set_shard_nodes("shard_0002", target_nodes);
   
   // 移除分裂信息
   routing_table.remove_splitting_shard("shard_0001");
   
   // 版本号递增
   routing_table.bump_version();
   ```

2. **通知 Node 路由表更新**
   - 通过 watch 机制通知所有 Node
   - Node 刷新本地路由表

3. **源分片处理缓存请求**
   - 批量返回 `MOVED <slot> <target_addr>`
   - 格式: `MOVED 4096 node2:6379`

4. **客户端重定向**
   - 收到 `MOVED` 后，重定向到目标分片
   - 重新执行请求

**路由表变化**:
```
分裂前:
slots[0..4096)   -> shard_0001
slots[4096..8192) -> shard_0001

分裂后:
slots[0..4096)   -> shard_0001
slots[4096..8192) -> shard_0002
```

**Node 处理逻辑**:
```rust
// node/src/router.rs
pub fn should_move_for_split(&self, key: &[u8], shard_id: &str) -> Option<(&String, &String)> {
    let split_info = self.routing_table.get_split_info(shard_id)?;
    
    // 检查是否在切换阶段
    if split_info.phase != SplitPhase::Switched {
        return None;
    }
    
    // 检查 slot 是否在目标范围
    let slot = Self::slot_for_key(key);
    if split_info.is_slot_moving(slot) {
        let target_addr = self.routing_table.node_addrs
            .get(split_info.target_nodes.first()?)?;
        return Some((&split_info.target_shard, target_addr));
    }
    
    None
}
```

### 阶段 6: 清理 (Cleanup)

**参与者**: 源分片, Pilot

**步骤**:

1. **源分片清理数据**
   - 删除 `slot ∈ [split_slot, end)` 的数据
   - 调用 `delete_keys_in_slot_range(split_slot, end, 16384)`
   - 可延迟执行（不影响服务）

2. **清除分裂状态**
   - 源分片: `clear_split_state()`, 状态恢复为 `Normal`
   - 目标分片: `clear_split_state()`, 状态恢复为 `Normal`

3. **Pilot 更新元数据**
   - `SplitTask` 状态更新为 `Completed`
   - 从路由表移除分裂信息
   - 保存元数据

## 关键机制

### 1. 数据一致性保证

**问题**: 如何保证分裂过程中不丢失、不重复数据？

**解决方案**:
- **快照传输**: 传输历史数据（到 `snapshot_index`）
- **增量日志**: 按顺序重放 `snapshot_index + 1` 之后的所有写操作
- **请求缓冲**: 在切换前缓存新请求，避免数据覆盖
- **原子切换**: 路由表版本号原子更新，确保所有 Node 同时切换

### 2. 零停机切换

**问题**: 如何实现零停机切换？

**解决方案**:
- **请求缓冲**: 在切换前缓存目标范围的写请求
- **MOVED 响应**: 切换后批量返回 `MOVED`，客户端自动重定向
- **原子路由更新**: 路由表版本号机制确保一致性

### 3. 进度追踪

**问题**: 如何追踪分裂进度？

**解决方案**:
```rust
SplitProgress {
    snapshot_done: bool,           // 快照是否完成
    snapshot_index: u64,          // 快照时的 Raft Index
    source_last_index: u64,       // 源分片最新 Index
    target_applied_index: u64,    // 目标分片已应用 Index
    buffered_requests: usize,     // 缓存的请求数
}

// 计算延迟
delay = source_last_index - target_applied_index
```

### 4. 异常处理

**场景 1: 追赶太慢**
- 检测: 持续 5 分钟 `delay > 1000`
- 处理: 限流源分片写入 / 增加目标分片资源 / 取消分裂

**场景 2: 缓存超时**
- 检测: 缓存时间 > `buffer_timeout`
- 处理: 回滚，返回 `TRYAGAIN`

**场景 3: 目标分片崩溃**
- 处理: 源分片数据完整，清理分裂状态，稍后重试

**场景 4: 网络分区**
- 处理: 超时检测，等待恢复，超过阈值则回滚

## 当前实现状态

### ✅ 已完成

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

### 🚧 待实现

1. **快照传输**
   - [ ] 实现 `create_split_snapshot` 槽位范围过滤
   - [ ] 实现快照传输协议（gRPC/HTTP）
   - [ ] 目标分片加载快照

2. **增量日志转发**
   - [ ] 源分片筛选 `slot >= split_slot` 的日志
   - [ ] 转发到目标分片 Raft 组
   - [ ] 目标分片按顺序 apply

3. **请求缓冲队列**
   - [ ] 实现 `SplitBuffer` 缓存机制
   - [ ] 设置最大缓冲数和超时
   - [ ] 切换后批量返回 `MOVED`

4. **进度上报**
   - [ ] Node 定期上报 `applied_index` 到 Pilot
   - [ ] Pilot 计算延迟并更新进度
   - [ ] API 查询分裂进度

## 总结

Shard Slot 分裂是一个复杂的分布式操作，涉及多个阶段和参与者。核心设计思路是：

1. **快照 + 增量日志**: 保证数据完整性
2. **请求缓冲 + MOVED**: 实现零停机切换
3. **原子路由更新**: 保证一致性
4. **进度追踪**: 提供可观测性
5. **异常处理**: 保证可靠性

当前框架已搭建完成，核心的数据传输和同步逻辑待实现。
