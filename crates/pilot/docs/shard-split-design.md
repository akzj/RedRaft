# Shard Split 设计文档

## 概述

分片分裂（Shard Split）是将一个大分片拆分为两个小分片的过程，用于：
- 单分片数据量过大时的水平扩展
- 单分片 QPS 过高时的负载分散
- 集群容量扩展

## 设计目标

1. **数据一致性** - 分裂过程中不丢失、不重复数据
2. **高可用性** - 最小化服务中断时间（目标 < 1秒）
3. **可观测性** - 分裂进度可查询、可监控
4. **可回滚** - 分裂失败可安全回滚

## 整体流程

```
分裂前:
┌─────────────────────────────────────┐
│           shard_0001                │
│         slots: [0, 8192)            │
│         数据量: 10GB                │
└─────────────────────────────────────┘

分裂后:
┌─────────────────┐  ┌─────────────────┐
│   shard_0001    │  │   shard_0002    │
│  slots: [0,4096)│  │ slots:[4096,8192)│
│  数据量: ~5GB   │  │  数据量: ~5GB   │
└─────────────────┘  └─────────────────┘
```

## 分裂方案：快照 + 增量日志 + 缓存切换

### 方案选择理由

| 方案 | 优点 | 缺点 | 选择 |
|------|------|------|------|
| 停写分裂 | 简单 | 服务中断 | ✗ |
| 双写分裂 | 零停机 | 数据覆盖问题 | ✗ |
| 快照+增量Log | 零停机、顺序正确 | 实现复杂 | ✓ |

### 核心思路

1. **快照传输** - 传输历史数据
2. **增量日志重放** - 按 Raft Index 顺序追赶
3. **缓存+MOVED** - 追平时缓存请求，切换后返回 MOVED

## 详细设计

### 阶段 1: 准备阶段

```
Pilot                     源 Shard                   目标 Shard
  │                           │                           │
  │── TriggerSplit ──────────▶│                           │
  │   {                       │                           │
  │     split_key: 4096,      │                           │
  │     target_shard_id,      │                           │
  │     target_nodes          │                           │
  │   }                       │                           │
  │                           │                           │
  │                           │ 1. 标记状态 = Splitting   │
  │                           │ 2. 记录 split_key        │
  │                           │                           │
  │◀── SplitPrepared ─────────│                           │
  │                           │                           │
  │                       创建目标 Shard ────────────────▶│
  │                           │                           │
```

### 阶段 2: 快照传输

```
源 Shard                              目标 Shard
    │                                      │
    │  1. 创建快照                          │
    │     snapshot_index = N               │
    │                                      │
    │  2. 筛选 slot ∈ [split_key, end)     │
    │     的数据                           │
    │                                      │
    │────── SnapshotChunk ────────────────▶│
    │       {                              │
    │         snapshot_index: N,           │ 3. 加载快照数据
    │         slot_range: [4096, 8192),    │
    │         data: [...],                 │ 4. 设置 applied_index = N
    │         done: false                  │
    │       }                              │
    │                                      │
    │────── SnapshotChunk (done=true) ────▶│
    │                                      │
```

### 阶段 3: 增量追赶

```
源 Shard                              目标 Shard
    │                                      │
    │  Log: [N+1][N+2][N+3]...             │
    │                                      │
    │  筛选 slot ∈ [4096, 8192) 的日志     │
    │                                      │
    │─────── LogEntry N+1 ────────────────▶│ apply
    │─────── LogEntry N+2 ────────────────▶│ apply
    │─────── LogEntry N+3 ────────────────▶│ apply
    │             ...                      │
    │                                      │
    │  定期上报进度:                        │
    │  delay = source.last_index           │
    │        - target.applied_index        │
    │                                      │
```

**追赶指标：**
- `delay = 源 last_index - 目标 applied_index`
- 当 `delay < catch_up_threshold (默认100)` 时进入下一阶段

### 阶段 4: 缓存 + 切换

```
当 delay < 阈值时：

┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  1. 源 Shard 对 [4096, 8192) 范围的写请求进入缓存模式        │
│                                                              │
│     Client A ─── SET foo ───▶ ┌─────────┐                   │
│     Client B ─── SET bar ───▶ │ 缓存队列 │ (不处理，只缓存)  │
│     Client C ─── DEL baz ────▶│ [A,B,C] │                   │
│                               └─────────┘                   │
│                                                              │
│  2. 目标 Shard 继续追赶最后几条 Log                          │
│                                                              │
│  3. 追平完成 (applied_index == source.last_index)            │
│                                                              │
│  4. Pilot 原子更新路由表                                     │
│     Version N+1:                                             │
│       shard_0001 -> [0, 4096)                                │
│       shard_0002 -> [4096, 8192)                             │
│                                                              │
│  5. 源 Shard 对缓存的请求批量返回 MOVED                       │
│                                                              │
│     Client A ◀── MOVED 4096 shard_0002 node2:6379           │
│     Client B ◀── MOVED 4096 shard_0002 node2:6379           │
│     Client C ◀── MOVED 4096 shard_0002 node2:6379           │
│                                                              │
│  6. Client 重定向到目标 Shard 执行                           │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 阶段 5: 清理

```
1. 源 Shard 删除 slot ∈ [4096, 8192) 的数据（可延迟执行）
2. 源 Shard 状态恢复为 Normal
3. 更新 Pilot 元数据
4. 分裂完成
```

## 状态机

```
                    ┌──────────┐
                    │  Normal  │
                    └────┬─────┘
                         │ trigger_split
                         ▼
                    ┌──────────┐
                    │Preparing │ ── 创建目标 Shard
                    └────┬─────┘
                         │ target_ready
                         ▼
                    ┌──────────┐
                    │Snapshot  │ ── 传输快照
                    │Transfer  │
                    └────┬─────┘
                         │ snapshot_done
                         ▼
                    ┌──────────┐
                    │CatchingUp│ ── 增量日志追赶
                    └────┬─────┘
                         │ delay < threshold
                         ▼
                    ┌──────────┐
                    │Buffering │ ── 缓存请求
                    └────┬─────┘
                         │ caught_up
                         ▼
                    ┌──────────┐
                    │Switching │ ── 切换路由
                    └────┬─────┘
                         │ switched
                         ▼
                    ┌──────────┐
                    │ Cleanup  │ ── 清理旧数据
                    └────┬─────┘
                         │ done
                         ▼
                    ┌──────────┐
                    │  Normal  │
                    └──────────┘
```

## 关键参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `catch_up_threshold` | 100 条 | 延迟低于此值进入缓存模式 |
| `buffer_timeout` | 5 秒 | 缓存请求最大等待时间 |
| `buffer_max_size` | 10000 | 最大缓存请求数 |
| `progress_interval` | 1 秒 | 进度上报间隔 |
| `snapshot_chunk_size` | 1MB | 快照分块大小 |

## 异常处理

### 场景 1: 追赶太慢

```
if 持续 5 分钟 delay > 1000:
    option A: 限流源 Shard 写入
    option B: 增加目标 Shard 资源
    option C: 取消分裂，稍后重试
```

### 场景 2: 缓存超时

```
if 缓存时间 > buffer_timeout:
    # 回滚
    1. 恢复源 Shard 正常处理
    2. 清空目标 Shard
    3. 标记分裂失败
    4. 对缓存请求返回 TRYAGAIN
```

### 场景 3: 目标 Shard 崩溃

```
# 安全回滚
1. 源 Shard 数据完整，继续服务
2. 清理分裂状态
3. 稍后重新触发分裂
```

### 场景 4: 网络分区

```
# 超时检测
if Pilot 与 Shard 通信超时:
    1. 等待网络恢复
    2. 超过阈值则回滚
```

## API 设计

### 触发分裂

```http
POST /api/v1/shards/{shard_id}/split
{
    "split_slot": 4096,                    // 分裂点
    "target_shard_id": "shard_0002",       // 可选，自动生成
    "target_nodes": ["node1", "node2"]     // 可选，自动选择
}

Response:
{
    "split_task_id": "split_xxx",
    "source_shard": "shard_0001",
    "target_shard": "shard_0002",
    "status": "preparing"
}
```

### 查询分裂进度

```http
GET /api/v1/splits/{split_task_id}

Response:
{
    "task_id": "split_xxx",
    "status": "catching_up",
    "progress": {
        "snapshot_done": true,
        "source_last_index": 10000,
        "target_applied_index": 9950,
        "delay": 50,
        "eta_seconds": 5
    }
}
```

### 取消分裂

```http
DELETE /api/v1/splits/{split_task_id}
```

## 数据结构

### SplitTask

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

pub enum SplitStatus {
    Preparing,
    SnapshotTransfer,
    CatchingUp,
    Buffering,
    Switching,
    Cleanup,
    Completed,
    Failed(String),
    Cancelled,
}

pub struct SplitProgress {
    pub snapshot_done: bool,
    pub snapshot_index: u64,
    pub source_last_index: u64,
    pub target_applied_index: u64,
    pub buffered_requests: usize,
}
```

### ShardInfo 扩展

```rust
pub struct ShardInfo {
    // ... existing fields ...
    
    /// 分裂状态（如果正在分裂）
    pub split_state: Option<ShardSplitState>,
}

pub struct ShardSplitState {
    pub split_task_id: String,
    pub split_slot: u32,
    pub role: SplitRole,  // Source or Target
}

pub enum SplitRole {
    Source,
    Target,
}
```

## 实现优先级

1. **Phase 1** - 基础框架
   - [ ] SplitTask 数据结构
   - [ ] SplitStatus 状态机
   - [ ] HTTP API 框架

2. **Phase 2** - 快照传输
   - [ ] 筛选槽位范围的快照
   - [ ] 快照传输协议
   - [ ] 目标 Shard 加载

3. **Phase 3** - 增量追赶
   - [ ] Log 筛选和转发
   - [ ] 进度追踪和上报
   - [ ] 追赶完成检测

4. **Phase 4** - 缓存切换
   - [ ] 请求缓存队列
   - [ ] 路由原子切换
   - [ ] MOVED 响应

5. **Phase 5** - 异常处理
   - [ ] 超时回滚
   - [ ] 故障恢复
   - [ ] 清理逻辑

## 参考

- [Redis Cluster Resharding](https://redis.io/docs/management/scaling/)
- [TiKV Region Split](https://docs.pingcap.com/tidb/stable/tikv-configuration-file#region-split)
- [CockroachDB Range Splits](https://www.cockroachlabs.com/docs/stable/architecture/distribution-layer.html)
