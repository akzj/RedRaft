# RaftKV 项目结构

## 目录结构

```
examples/raft-kv/
├── Cargo.toml              # 项目配置和依赖
├── DESIGN.md               # 架构设计文档
├── FEATURES.md             # 功能要点清单
├── README.md               # 使用说明
├── STRUCTURE.md            # 项目结构说明（本文件）
│
├── src/                    # 源代码目录
│   ├── main.rs            # 服务器入口点
│   ├── server.rs          # 服务器主逻辑
│   ├── state_machine.rs   # KV 状态机实现
│   ├── client.rs          # 客户端实现
│   ├── api/               # API 实现
│   │   ├── mod.rs
│   │   ├── grpc.rs        # gRPC 服务实现
│   │   └── http.rs        # HTTP REST API 实现
│   ├── metrics.rs          # 指标收集和导出
│   └── config.rs          # 配置管理
│
├── benches/                # 性能基准测试
│   ├── throughput.rs      # 吞吐量基准测试
│   └── latency.rs         # 延迟基准测试
│
└── tests/                  # 集成测试
    ├── functional.rs       # 功能测试
    ├── consistency.rs     # 一致性测试
    └── performance.rs     # 性能测试
```

## 模块说明

### 核心模块

#### `main.rs`
- 程序入口点
- 命令行参数解析
- 服务器启动和关闭

#### `server.rs`
- RaftKV 服务器主逻辑
- 集成 RaftState 和 FileStorage
- 请求路由和处理

#### `state_machine.rs`
- KV 状态机实现
- 将 Raft 日志应用到内存中的键值对
- 快照创建和恢复

#### `client.rs`
- 客户端实现
- 自动 Leader 发现和重定向
- 错误处理和重试

### API 模块

#### `api/grpc.rs`
- gRPC 服务定义和实现
- 使用 tonic 框架
- 处理 PUT/GET/DELETE/SCAN 请求

#### `api/http.rs`
- HTTP REST API 实现
- 使用 axum 或 warp 框架
- 提供 RESTful 接口

### 工具模块

#### `metrics.rs`
- 性能指标收集
- Prometheus 格式导出
- 指标统计和聚合

#### `config.rs`
- 配置管理
- 命令行参数解析
- 配置文件支持

## 测试模块

### `tests/functional.rs`
- 基本操作测试
- 并发操作测试
- 错误处理测试

### `tests/consistency.rs`
- 线性一致性验证
- 故障场景一致性测试
- 网络分区一致性测试

### `tests/performance.rs`
- 延迟测试
- 吞吐量测试
- 压力测试

## 基准测试

### `benches/throughput.rs`
- 吞吐量基准测试
- 不同负载场景
- 性能对比

### `benches/latency.rs`
- 延迟基准测试
- 延迟分布统计
- 性能分析

## 数据目录

运行时创建的数据目录结构：

```
data/
├── node1/
│   ├── logs/              # 日志段文件
│   └── snapshots/         # 快照文件
├── node2/
│   ├── logs/
│   └── snapshots/
└── node3/
    ├── logs/
    └── snapshots/
```

## 依赖关系

```
raft-kv
├── raft-lite (parent crate)
│   ├── tokio
│   ├── async-trait
│   └── ...
├── tonic (gRPC)
├── axum (HTTP, optional)
├── tracing (日志)
└── prometheus (指标, optional)
```

## 构建和运行

### 开发模式

```bash
cargo build
cargo run
```

### 发布模式

```bash
cargo build --release
cargo run --release
```

### 测试

```bash
cargo test
cargo test --test functional
cargo test --test consistency
```

### 基准测试

```bash
cargo bench
```

