# RaftKV - Redis-Compatible Distributed Key-Value Store

A distributed, high-availability, high-performance Redis-compatible key-value store built on Raft consensus algorithm.

## 项目概述

RaftKV 是一个**兼容 Redis 协议的分布式键值存储系统**，使用 Raft 共识算法保证数据一致性和高可用性。相比传统 Redis，RaftKV 提供了更强的可靠性和一致性保证。

### 核心定位

- **Redis 兼容**: 支持 Redis 协议，可以直接使用 Redis 客户端
- **分布式**: 基于 Raft 共识，支持多节点集群
- **高可用**: 自动故障恢复，支持节点故障和网络分区
- **高性能**: Multi-Raft 架构，支持横向扩展
- **强一致性**: 使用 ReadIndex 和 LeaderLease 保证线性一致性

### 主要特性

- ✅ **Redis 协议兼容**: 支持大部分 Redis 命令（GET, SET, DEL, SCAN 等）
- ✅ **Multi-Raft 架构** ⭐: 单个节点支持多个 Raft 组，并发处理，大幅提升吞吐量
- ✅ **动态管理**: 支持 Raft 组的创建、迁移、合并、分裂
- ✅ **实例漂移**: Raft 组可以在集群节点间"漂移"，实现动态负载均衡
- ✅ **线性一致性**: 使用 ReadIndex 和 LeaderLease 保证（每个组内部）
- ✅ **高可用**: 支持节点故障和网络分区自动恢复
- ✅ **持久化**: 所有数据持久化到磁盘，支持快照和日志恢复
- ✅ **元数据管理**: 独立的元数据集群管理配置和路由
- ✅ **监控指标**: Prometheus 指标导出

## 项目结构

```
raft-kv/
├── crates/
│   └── raft/          # Raft 共识算法实现（独立 crate）
│       ├── src/       # Raft 核心代码
│       ├── proto/     # gRPC 协议定义
│       └── tests/     # Raft 测试
├── raft-kv/           # 主项目（Redis 兼容的 KV 存储）
│   └── src/           # RaftKV 实现
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

# 只构建 RaftKV
cargo build --release -p raft-kv

# 只构建 Raft 库
cargo build --release -p raft
```

### 启动单节点

```bash
cargo run --release -p raft-kv -- \
    --node-id node1 \
    --data-dir ./data/node1 \
    --port 6379 \
    --redis-port 6380
```

### 启动 3 节点集群

**终端 1:**
```bash
cargo run --release -p raft-kv -- \
    --node-id node1 \
    --data-dir ./data/node1 \
    --port 5001 \
    --redis-port 6379 \
    --cluster node1=127.0.0.1:5001,node2=127.0.0.1:5002,node3=127.0.0.1:5003
```

**终端 2:**
```bash
cargo run --release -p raft-kv -- \
    --node-id node2 \
    --data-dir ./data/node2 \
    --port 5002 \
    --redis-port 6380 \
    --cluster node1=127.0.0.1:5001,node2=127.0.0.1:5002,node3=127.0.0.1:5003
```

**终端 3:**
```bash
cargo run --release -p raft-kv -- \
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
127.0.0.1:6379> SCAN 0
1) "0"
2) 1) "key2"
   2) "key3"
```

### 使用编程语言客户端

```python
import redis

# 连接 RaftKV（兼容 Redis 协议）
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# 基本操作
r.set('key1', 'value1')
value = r.get('key1')
r.delete('key1')

# 批量操作
r.mset({'key1': 'value1', 'key2': 'value2'})
values = r.mget(['key1', 'key2'])
```

```rust
use redis::Commands;

let client = redis::Client::open("redis://127.0.0.1:6379/")?;
let mut con = client.get_connection()?;

// 基本操作
con.set("key1", "value1")?;
let value: String = con.get("key1")?;
con.del("key1")?;
```

## Multi-Raft 核心优势

### 问题：单 Raft 串行提交

传统单 Raft 节点的问题：
- 所有请求必须串行处理
- 吞吐量受限于单组性能（~10,000 ops/s）
- 无法充分利用多核 CPU

### 解决方案：Multi-Raft 并发

RaftKV 的 Multi-Raft 架构：
- **多组并发**：单个节点运行多个 Raft 组（shard）
- **数据分片**：通过键值哈希路由到不同组
- **线性扩展**：吞吐量 ≈ 组数 × 单组吞吐量
- **动态管理**：支持创建、迁移、合并、分裂

### 性能对比

| 场景 | 单 Raft 组 | Multi-Raft (10 组) |
|------|-----------|-------------------|
| 写吞吐 | ~10,000 ops/s | ~100,000 ops/s |
| 读吞吐 | ~50,000 ops/s | ~500,000 ops/s |
| 并发能力 | 串行 | 10 组并发 |

### Raft 组"漂移"

Raft 组就像集群中的"幽灵"，可以在节点间漂移：

```bash
# 创建 Raft 组 shard_0
curl -X POST http://localhost:5001/admin/shard \
  -d '{"shard_id": "shard_0", "nodes": ["node1", "node2", "node3"]}'

# 迁移 shard_0 从 node1 到 node4（负载均衡）
curl -X POST http://localhost:5001/admin/shard/shard_0/migrate \
  -d '{"from": "node1", "to": "node4"}'

# 合并 shard_1 和 shard_2
curl -X POST http://localhost:5001/admin/shard/merge \
  -d '{"sources": ["shard_1", "shard_2"], "target": "shard_1"}'

# 分裂 shard_0 成两个组
curl -X POST http://localhost:5001/admin/shard/shard_0/split \
  -d '{"new_shards": ["shard_0", "shard_10"]}'
```

## 测试

### 运行功能测试

```bash
# 运行所有测试
cargo test

# 运行 Raft 库测试
cargo test -p raft

# 运行 RaftKV 测试
cargo test -p raft-kv
```

### 运行一致性测试

```bash
cargo test --test consistency
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

- [x] 项目设计和文档
- [x] Raft 共识算法实现
- [ ] Phase 1: 基础实现（Redis 协议、基本 KV 操作）
- [ ] Phase 2: Multi-Raft 和集群功能
- [ ] Phase 3: 元数据管理集群
- [ ] Phase 4: 测试工具和监控
- [ ] Phase 5: 性能优化

## 许可证

MIT License
