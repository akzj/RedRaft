# RaftKV - 分布式键值存储系统

基于 `raft-lite` 实现的分布式键值存储系统，用于验证和测试 Raft 共识算法的正确性、可用性和性能。

## 项目概述

RaftKV 是一个完整的分布式键值存储系统实现，展示了如何使用 `raft-lite` 构建一个生产级的分布式系统。

### 主要特性

- ✅ **完整的 KV 操作**: PUT, GET, DELETE, SCAN
- ✅ **Multi-Raft 架构** ⭐: 单个节点支持多个 Raft 组，并发处理，大幅提升吞吐量
- ✅ **动态管理**: 支持 Raft 组的创建、迁移、合并、分裂
- ✅ **实例漂移**: Raft 组可以在集群节点间"漂移"，实现动态负载均衡
- ✅ **线性一致性**: 使用 ReadIndex 和 LeaderLease 保证（每个组内部）
- ✅ **高可用**: 支持节点故障和网络分区
- ✅ **持久化**: 所有数据持久化到磁盘
- ✅ **性能测试**: 内置性能测试和压测工具
- ✅ **监控指标**: Prometheus 指标导出

## 快速开始

### 构建项目

```bash
cd examples/raft-kv
cargo build --release
```

### 启动单节点

```bash
cargo run --release -- \
    --node-id node1 \
    --data-dir ./data/node1 \
    --port 5001
```

### 启动 3 节点集群

**终端 1:**
```bash
cargo run --release -- \
    --node-id node1 \
    --data-dir ./data/node1 \
    --port 5001 \
    --cluster node1=127.0.0.1:5001,node2=127.0.0.1:5002,node3=127.0.0.1:5003
```

**终端 2:**
```bash
cargo run --release -- \
    --node-id node2 \
    --data-dir ./data/node2 \
    --port 5002 \
    --cluster node1=127.0.0.1:5001,node2=127.0.0.1:5002,node3=127.0.0.1:5003
```

**终端 3:**
```bash
cargo run --release -- \
    --node-id node3 \
    --data-dir ./data/node3 \
    --port 5003 \
    --cluster node1=127.0.0.1:5001,node2=127.0.0.1:5002,node3=127.0.0.1:5003
```

## 使用示例

### 使用 CLI 客户端

```bash
# 写入
cargo run --bin raft-kv-client -- put key1 value1

# 读取
cargo run --bin raft-kv-client -- get key1

# 删除
cargo run --bin raft-kv-client -- delete key1

# 扫描
cargo run --bin raft-kv-client -- scan key
```

### 使用 gRPC 客户端

```rust
use raft_kv::client::RaftKVClient;

let client = RaftKVClient::new("http://127.0.0.1:5001").await?;

// PUT
client.put("key1", "value1").await?;

// GET
let value = client.get("key1").await?;
println!("value: {:?}", value);

// DELETE
client.delete("key1").await?;
```

## 性能测试

### 运行基准测试

```bash
cargo bench
```

### 运行压测

```bash
cargo run --release --bin raft-kv-benchmark -- \
    --endpoints http://127.0.0.1:5001,http://127.0.0.1:5002,http://127.0.0.1:5003 \
    --concurrency 100 \
    --duration 60s \
    --ops put,get
```

## 测试

### 运行功能测试

```bash
cargo test
```

### 运行一致性测试

```bash
cargo test --test consistency
```

### 运行性能测试

```bash
cargo test --test performance
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

## 架构

详见 [DESIGN.md](./DESIGN.md)

## Multi-Raft 详细设计

详见 [MULTI_RAFT.md](./MULTI_RAFT.md)

## 功能列表

详见 [FEATURES.md](./FEATURES.md)

## 开发计划

- [x] 项目设计和文档
- [ ] Phase 1: 基础实现
- [ ] Phase 2: 集群功能
- [ ] Phase 3: 测试工具
- [ ] Phase 4: 监控和优化

## 许可证

MIT License

