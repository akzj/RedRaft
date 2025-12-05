# RaftKV - 分布式键值存储系统设计文档

## 项目概述

**RaftKV** 是基于 `raft-lite` 实现的分布式键值存储系统，用于验证和测试 Raft 共识算法的正确性、可用性和性能。

### 项目目标

1. **功能验证**：验证 Raft 算法的正确性（选举、日志复制、配置变更等）
2. **可用性测试**：测试系统在故障场景下的行为（节点宕机、网络分区等）
3. **性能分析**：评估系统在不同负载下的性能表现
4. **压测优化**：通过压测发现性能瓶颈并进行优化

## 架构设计

### 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                    RaftKV Client                         │
│  (PUT/GET/DELETE/SCAN operations)                       │
└──────────────────┬──────────────────────────────────────┘
                   │ gRPC/HTTP
                   │
    ┌──────────────┼──────────────┐
    │              │              │
┌───▼───┐    ┌─────▼─────┐   ┌───▼───┐
│Node 1 │    │  Node 2   │   │Node 3 │
│       │    │           │   │       │
│RaftKV │◄──►│  RaftKV   │◄──►│RaftKV │
│Server │    │  Server   │   │Server │
│       │    │           │   │       │
│┌─────┐│    │┌─────────┐│   │┌─────┐│
││Raft ││    ││  Raft   ││   ││Raft ││
││State││    ││  State  ││   ││State││
│└─────┘│    │└─────────┘│   │└─────┘│
│       │    │           │   │       │
│┌─────┐│    │┌─────────┐│   │┌─────┐│
││File ││    ││  File   ││   ││File ││
││Store││    ││  Store  ││   ││Store││
│└─────┘│    │└─────────┘│   │└─────┘│
└───────┘    └───────────┘   └───────┘
```

### 组件说明

1. **RaftKV Server**
   - 每个节点运行一个 RaftKV 服务器
   - 处理客户端请求（PUT/GET/DELETE/SCAN）
   - 与 Raft 状态机交互
   - 提供 gRPC 和 HTTP 接口

2. **Raft State Machine**
   - 使用 `raft-lite` 的 `RaftState`
   - 处理选举、日志复制、配置变更
   - 维护集群一致性

3. **FileStorage**
   - 使用 `raft-lite` 的 `FileStorage`
   - 持久化日志、硬状态、集群配置、快照
   - 支持数据恢复

4. **State Machine**
   - 将 Raft 日志应用到 KV 存储
   - 维护内存中的键值对
   - 支持快照和恢复

## 数据模型

### 键值对存储

```rust
pub struct KeyValue {
    pub key: Vec<u8>,      // 键（任意字节）
    pub value: Vec<u8>,    // 值（任意字节）
    pub version: u64,      // 版本号（单调递增）
    pub timestamp: u64,    // 时间戳
}
```

### 操作类型

```rust
pub enum KVOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    // 内部操作，不对外暴露
    NoOp,  // 用于测试
}
```

### 日志条目

- 每个 KV 操作作为一个 Raft 日志条目
- 日志条目包含：操作类型、键、值、客户端请求 ID

## API 设计

### gRPC API

```protobuf
service RaftKVService {
    // 写入键值对
    rpc Put(PutRequest) returns (PutResponse);
    
    // 读取键值对
    rpc Get(GetRequest) returns (GetResponse);
    
    // 删除键值对
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    
    // 范围扫描
    rpc Scan(ScanRequest) returns (ScanResponse);
    
    // 获取集群状态
    rpc GetClusterStatus(Empty) returns (ClusterStatus);
    
    // 获取节点信息
    rpc GetNodeInfo(Empty) returns (NodeInfo);
}
```

### HTTP REST API

```
PUT    /kv/{key}           - 写入键值对
GET    /kv/{key}           - 读取键值对
DELETE /kv/{key}           - 删除键值对
GET    /kv?prefix={prefix} - 范围扫描
GET    /status             - 集群状态
GET    /metrics            - 性能指标
```

## 一致性保证

### 线性一致性（Linearizability）

- 所有读写操作满足线性一致性
- 使用 ReadIndex 机制确保读操作的线性一致性
- LeaderLease 优化（可选，需要时钟同步）

### 写操作流程

```
Client → Leader → Raft Log → Commit → Apply → State Machine → Response
```

### 读操作流程

```
Client → Leader → ReadIndex → Wait for Apply → Read State Machine → Response
```

## 性能目标

### 延迟目标

- **写操作（P99）**: < 10ms（本地网络）
- **读操作（P99）**: < 5ms（使用 LeaderLease）
- **读操作（P99）**: < 10ms（使用 ReadIndex）

### 吞吐量目标

- **写吞吐**: > 10,000 ops/s（单节点）
- **读吞吐**: > 50,000 ops/s（单节点）
- **集群吞吐**: 线性扩展（3 节点 > 30,000 ops/s）

## 功能要点

### 核心功能

1. **基本操作**
   - ✅ PUT：写入键值对
   - ✅ GET：读取键值对
   - ✅ DELETE：删除键值对
   - ✅ SCAN：范围扫描（按前缀）

2. **集群管理**
   - ✅ 节点启动和加入集群
   - ✅ 节点退出和移除
   - ✅ 集群配置变更（动态添加/删除节点）
   - ✅ 集群状态查询

3. **故障处理**
   - ✅ Leader 故障转移
   - ✅ 节点重启数据恢复
   - ✅ 网络分区处理
   - ✅ 数据一致性保证

4. **持久化**
   - ✅ 所有操作持久化到磁盘
   - ✅ 快照支持（定期创建和恢复）
   - ✅ 数据恢复（重启后自动恢复）

### 测试功能

5. **功能测试**
   - ✅ 基本操作正确性测试
   - ✅ 并发操作测试
   - ✅ 故障场景测试
   - ✅ 数据一致性验证

6. **性能测试**
   - ✅ 延迟测试（P50/P95/P99）
   - ✅ 吞吐量测试
   - ✅ 压力测试工具
   - ✅ 性能基准测试

7. **监控和指标**
   - ✅ 操作延迟统计
   - ✅ 吞吐量统计
   - ✅ Raft 指标（term、commit index、apply index）
   - ✅ 存储指标（磁盘使用、段数量）
   - ✅ Prometheus metrics 导出

### 优化功能

8. **性能优化**
   - ✅ 批量写入优化
   - ✅ 读操作缓存（可选）
   - ✅ 连接池管理
   - ✅ 异步操作优化

9. **可观测性**
   - ✅ 结构化日志（tracing）
   - ✅ 性能指标导出
   - ✅ 健康检查端点
   - ✅ 调试接口

## 实现计划

### Phase 1: 基础实现
- [ ] 项目结构和依赖配置
- [ ] 基本 KV 状态机实现
- [ ] gRPC/HTTP 服务器
- [ ] 与 Raft 集成
- [ ] 基本 PUT/GET/DELETE 操作

### Phase 2: 集群功能
- [ ] 多节点集群支持
- [ ] 配置变更功能
- [ ] 节点加入/退出
- [ ] 集群状态查询

### Phase 3: 测试工具
- [ ] 功能测试套件
- [ ] 性能测试工具
- [ ] 压测工具
- [ ] 一致性验证工具

### Phase 4: 监控和优化
- [ ] Metrics 导出
- [ ] 性能分析工具
- [ ] 性能优化
- [ ] 文档完善

## 技术栈

- **Rust**: 主要开发语言
- **raft-lite**: Raft 共识算法实现
- **tokio**: 异步运行时
- **tonic**: gRPC 框架
- **axum/warp**: HTTP 框架（可选）
- **tracing**: 日志和追踪
- **prometheus**: 指标导出（可选）

## 目录结构

```
examples/raft-kv/
├── Cargo.toml              # 项目配置
├── DESIGN.md               # 设计文档（本文件）
├── README.md               # 使用说明
├── src/
│   ├── main.rs            # 服务器入口
│   ├── server.rs          # 服务器实现
│   ├── state_machine.rs   # KV 状态机
│   ├── client.rs          # 客户端实现
│   ├── api/
│   │   ├── grpc.rs        # gRPC 服务
│   │   └── http.rs        # HTTP 服务
│   └── metrics.rs         # 指标收集
├── benches/
│   ├── throughput.rs      # 吞吐量基准测试
│   └── latency.rs         # 延迟基准测试
└── tests/
    ├── functional.rs      # 功能测试
    ├── consistency.rs     # 一致性测试
    └── performance.rs     # 性能测试
```

## 使用场景

1. **功能验证**
   ```bash
   cargo test --example raft-kv
   ```

2. **性能测试**
   ```bash
   cargo bench --example raft-kv
   ```

3. **压测**
   ```bash
   cargo run --example raft-kv --release -- --benchmark
   ```

4. **集群演示**
   ```bash
   # 启动 3 节点集群
   cargo run --example raft-kv -- --node-id node1 --port 5001
   cargo run --example raft-kv -- --node-id node2 --port 5002
   cargo run --example raft-kv -- --node-id node3 --port 5003
   ```

## 后续扩展

- [ ] 事务支持
- [ ] 二级索引
- [ ] 数据分片
- [ ] 多数据中心支持
- [ ] 备份和恢复工具

