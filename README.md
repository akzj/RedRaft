# RedRaft - Redis-Compatible Distributed Key-Value Store

A distributed, high-availability, high-performance Redis-compatible key-value store built on Raft consensus algorithm.

## 项目概述

RedRaft (Raft-Lite) 是一个**兼容 Redis 协议的分布式键值存储系统**，使用 Raft 共识算法保证数据一致性和高可用性。相比传统 Redis，RedRaft 提供了更强的可靠性和一致性保证。项目采用 Multi-Raft 架构，支持横向扩展和高并发处理。

### 核心定位

- **Redis 兼容**: 支持 Redis 协议，可以直接使用 Redis 客户端
- **分布式**: 基于 Raft 共识，支持多节点集群
- **高可用**: 自动故障恢复，支持节点故障和网络分区
- **高性能**: Multi-Raft 架构，支持横向扩展
- **强一致性**: 使用 ReadIndex 和 LeaderLease 保证线性一致性

### 主要特性

- ✅ **Redis 协议兼容**: 支持大部分 Redis 命令（GET, SET, DEL, SCAN, HSET, ZADD 等）
- ✅ **Multi-Raft 架构** ⭐: 单个节点支持多个 Raft 组，并发处理，大幅提升吞吐量
- ✅ **动态分片管理**: 支持 Raft 组的创建、迁移、合并、分裂
- ✅ **实例漂移**: Raft 组可以在集群节点间"漂移"，实现动态负载均衡
- ✅ **线性一致性**: 使用 ReadIndex 和 LeaderLease 保证（每个组内部）
- ✅ **高可用**: 支持节点故障和网络分区自动恢复
- ✅ **持久化**: 所有数据持久化到磁盘，支持快照和日志恢复
- ✅ **数据分片**: 支持 Hash、List、Set、Sorted Set 等多种数据结构
- ✅ **异步批量处理**: 支持异步批量读取提交状态，优化同步性能
- ✅ **Bootstrap 快照**: 支持强制快照生成和分发，用于分片操作

## 项目结构

```
raft-lite/
├── crates/
│   ├── raft/          # Raft 共识算法实现（独立 crate）
│   │   ├── src/       # Raft 核心代码
│   │   ├── proto/     # gRPC 协议定义
│   │   └── tests/     # Raft 测试
│   ├── node/          # 节点实现（主项目）
│   │   └── src/       # RedRaft 节点实现
│   ├── storage/       # 存储层实现
│   │   └── src/       # 存储后端（Memory, RocksDB, Hybrid）
│   ├── resp/          # RESP 协议实现
│   ├── proto/         # Protocol Buffers 定义
│   ├── rr-core/       # 路由和分片核心逻辑
│   └── pilot/         # 客户端工具
├── DESIGN.md          # 架构设计文档
├── MULTI_RAFT.md      # Multi-Raft 详细设计
├── FEATURES.md        # 功能列表
└── STRUCTURE.md       # 代码结构说明
```

## 快速开始

### 构建项目

```bash
# 构建整个 workspace
cargo build --release

# 只构建 RedRaft 节点
cargo build --release -p redraft

# 只构建 Raft 库
cargo build --release -p raft
```

### 启动单节点

```bash
cargo run --release -p redraft -- \
    --node-id node1 \
    --data-dir ./data/node1 \
    --port 6379 \
    --redis-port 6380
```

### 启动 3 节点集群

**终端 1:**
```bash
cargo run --release -p redraft -- \
    --node-id node1 \
    --data-dir ./data/node1 \
    --port 5001 \
    --redis-port 6379 \
    --cluster node1=127.0.0.1:5001,node2=127.0.0.1:5002,node3=127.0.0.1:5003
```

**终端 2:**
```bash
cargo run --release -p redraft -- \
    --node-id node2 \
    --data-dir ./data/node2 \
    --port 5002 \
    --redis-port 6380 \
    --cluster node1=127.0.0.1:5001,node2=127.0.0.1:5002,node3=127.0.0.1:5003
```

**终端 3:**
```bash
cargo run --release -p redraft -- \
    --node-id node3 \
    --data-dir ./data/node3 \
    --port 5003 \
    --redis-port 6381 \
    --cluster node1=127.0.0.1:5001,node2=127.0.0.1:5002,node3=127.0.0.1:5003
```

## 使用示例

### 使用 Redis 客户端

```bash
# 使用 redis-cli 连接
redis-cli -p 6379

# 基本操作
127.0.0.1:6379> SET key1 value1
OK
127.0.0.1:6379> GET key1
"value1"
127.0.0.1:6379> DEL key1
(integer) 1

# Hash 操作
127.0.0.1:6379> HSET user:1 name "Alice" age 30
(integer) 2
127.0.0.1:6379> HGET user:1 name
"Alice"

# List 操作
127.0.0.1:6379> RPUSH list1 a b c
(integer) 3
127.0.0.1:6379> LRANGE list1 0 -1
1) "a"
2) "b"
3) "c"

# Set 操作
127.0.0.1:6379> SADD set1 1 2 3
(integer) 3
127.0.0.1:6379> SMEMBERS set1
1) "1"
2) "2"
3) "3"

# Sorted Set 操作
127.0.0.1:6379> ZADD zset1 1.0 "member1" 2.0 "member2"
(integer) 2
127.0.0.1:6379> ZRANGE zset1 0 -1
1) "member1"
2) "member2"
```

### 使用编程语言客户端

```python
import redis

# 连接 RedRaft（兼容 Redis 协议）
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# 基本操作
r.set('key1', 'value1')
value = r.get('key1')
r.delete('key1')

# Hash 操作
r.hset('user:1', mapping={'name': 'Alice', 'age': 30})
name = r.hget('user:1', 'name')

# List 操作
r.rpush('list1', 'a', 'b', 'c')
items = r.lrange('list1', 0, -1)

# Set 操作
r.sadd('set1', 1, 2, 3)
members = r.smembers('set1')

# Sorted Set 操作
r.zadd('zset1', {'member1': 1.0, 'member2': 2.0})
ranked = r.zrange('zset1', 0, -1)
```

```rust
use redis::Commands;

let client = redis::Client::open("redis://127.0.0.1:6379/")?;
let mut con = client.get_connection()?;

// 基本操作
con.set("key1", "value1")?;
let value: String = con.get("key1")?;
con.del("key1")?;

// Hash 操作
con.hset("user:1", "name", "Alice")?;
let name: String = con.hget("user:1", "name")?;
```

## Multi-Raft 核心优势

### 问题：单 Raft 串行提交

传统单 Raft 节点的问题：
- 所有请求必须串行处理
- 吞吐量受限于单组性能（~10,000 ops/s）
- 无法充分利用多核 CPU

### 解决方案：Multi-Raft 并发

RedRaft 的 Multi-Raft 架构：
- **多组并发**：单个节点运行多个 Raft 组（shard），每个组独立处理请求
- **数据分片**：通过键值哈希路由到不同组，实现数据分布
- **线性扩展**：吞吐量 ≈ 组数 × 单组吞吐量，支持横向扩展
- **动态管理**：支持创建、迁移、合并、分裂，实现弹性伸缩
- **独立状态机**：每个 Raft 组拥有独立的状态机和存储

### 性能对比

| 场景 | 单 Raft 组 | Multi-Raft (10 组) |
|------|-----------|-------------------|
| 写吞吐 | ~10,000 ops/s | ~100,000 ops/s |
| 读吞吐 | ~50,000 ops/s | ~500,000 ops/s |
| 并发能力 | 串行 | 10 组并发 |

### Raft 组动态管理

Raft 组可以在集群中动态创建、迁移、合并和分裂：

```bash
# 创建 Raft 组 shard_0
curl -X POST http://localhost:5001/admin/shard \
  -d '{"shard_id": "shard_0", "nodes": ["node1", "node2", "node3"]}'

# 迁移 shard_0 从 node1 到 node4（负载均衡）
# 包括：快照传输、日志重放、配置变更
curl -X POST http://localhost:5001/admin/shard/shard_0/migrate \
  -d '{"from": "node1", "to": "node4"}'

# 合并 shard_1 和 shard_2
# 包括：数据合并、配置变更、路由更新
curl -X POST http://localhost:5001/admin/shard/merge \
  -d '{"sources": ["shard_1", "shard_2"], "target": "shard_1"}'

# 分裂 shard_0 成两个组
# 包括：数据分片、快照生成、新组创建、日志重放
curl -X POST http://localhost:5001/admin/shard/shard_0/split \
  -d '{"new_shards": ["shard_0", "shard_10"]}'
```

### 核心架构特性

- **分片操作**：
  - ✅ **分裂 (Split)**: 支持按 slot 范围分裂，包括快照传输和日志重放
  - ✅ **合并 (Merge)**: 支持多个分片合并，数据一致性保证
  - ✅ **迁移 (Migrate)**: 支持分片在节点间迁移，实现负载均衡
  - ✅ **Bootstrap 快照**: 强制快照生成和分发，确保分片操作一致性

- **数据同步**：
  - ✅ **增量同步**: 支持基于 seq_index 的增量日志同步
  - ✅ **快照传输**: 支持分块快照传输和恢复
  - ✅ **异步批量处理**: 支持异步批量读取提交状态，优化同步性能

- **存储后端**：
  - ✅ **HybridStore**: 内存 + RocksDB 混合存储
  - ✅ **Copy-on-Write**: 快照时使用 COW 优化，避免数据复制
  - ✅ **多数据结构**: 支持 String、Hash、List、Set、Sorted Set

## 测试

### 运行功能测试

```bash
# 运行所有测试
cargo test

# 运行 Raft 库测试
cargo test -p raft

# 运行 RedRaft 节点测试
cargo test -p redraft

# 运行存储层测试
cargo test -p storage
```

### 运行一致性测试

```bash
# Raft 一致性测试
cargo test -p raft --test bootstrap_test
cargo test -p raft --test snapshot_test

# 分片操作测试
cargo test -p redraft --test split_test
```

## 监控

### 查看指标

访问 `http://localhost:5001/metrics` 查看 Prometheus 格式的指标。

### 主要指标

- `raftkv_operations_total`: 操作总数
- `raftkv_operation_duration_seconds`: 操作延迟
- `raftkv_raft_term`: 当前 Term
- `raftkv_raft_commit_index`: Commit Index
- `raftkv_storage_disk_usage_bytes`: 磁盘使用量

## 文档

- [架构设计](./DESIGN.md) - 系统架构和设计文档
- [Multi-Raft 详细设计](./MULTI_RAFT.md) - Multi-Raft 架构详细设计
- [功能列表](./FEATURES.md) - 完整功能列表
- [代码结构](./STRUCTURE.md) - 代码结构说明

## 开发计划

### 已完成 ✅

- [x] 项目设计和文档
- [x] Raft 共识算法实现
- [x] Phase 1: 基础实现（Redis 协议、基本 KV 操作）
- [x] Multi-Raft 架构和并发处理
- [x] 分片操作（创建、迁移、合并、分裂）
- [x] 快照和日志恢复
- [x] 多数据结构支持（String、Hash、List、Set、Sorted Set）
- [x] Bootstrap 快照机制
- [x] 异步批量处理优化

### 进行中 🚧

- [ ] Phase 2: 元数据管理集群
- [ ] Phase 3: 监控和指标完善
- [ ] Phase 4: 性能优化和压测

### 计划中 📋

- [ ] Phase 5: 高级功能（事务、Lua 脚本等）
- [ ] Phase 6: 运维工具和监控面板

## 许可证

MIT License
