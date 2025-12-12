# RedRaft Configuration Guide

RedRaft 支持通过 YAML 配置文件进行配置，所有参数都按功能模块分类组织。

## 配置文件格式

配置文件使用 YAML 格式，支持以下模块：

### 1. Node Configuration (节点配置)

```yaml
node:
  node_id: "node1" # 节点 ID
  shard_count: 3 # 分片数量（无 Pilot 时使用）
```

### 2. Network Configuration (网络配置)

```yaml
network:
  redis_addr: "127.0.0.1:6379" # Redis 服务器监听地址
  grpc_addr: "127.0.0.1:50051" # gRPC 服务地址（用于 Raft 通信）
```

### 3. Storage Configuration (存储配置)

```yaml
storage:
  data_dir: "./data" # 数据存储目录
```

### 4. Snapshot Configuration (快照配置)

```yaml
snapshot:
  chunk_size: 67108864 # Chunk 大小（未压缩，字节），默认 64MB
  zstd_level: 3 # ZSTD 压缩级别 (1-22)，默认 3
  chunk_check_interval_ms: 100 # Chunk 等待检查间隔（毫秒），默认 100ms
  chunk_timeout_secs: 300 # Chunk 等待超时（秒），默认 300s (5分钟)
  transfer_dir: "./data/snapshot_transfers" # 快照传输目录
```

### 5. Raft Configuration (Raft 配置)

```yaml
raft:
  request_timeout_secs: 5 # 请求超时（秒），默认 5s
  election_timeout_ms: 1000 # 选举超时（毫秒），默认 1000ms
  heartbeat_timeout_ms: 100 # 心跳超时（毫秒），默认 100ms
  leader_transfer_timeout_ms: 5000 # Leader 转移超时（毫秒），默认 5000ms
  apply_log_timeout_ms: 1000 # 应用日志超时（毫秒），默认 1000ms
  config_change_timeout_ms: 10000 # 配置变更超时（毫秒），默认 10000ms
```

### 6. Pilot Configuration (Pilot 配置，可选)

```yaml
pilot:
  pilot_addr: "http://127.0.0.1:8080" # Pilot 服务地址
  heartbeat_interval_secs: 10 # 心跳间隔（秒），默认 10s
  routing_refresh_interval_secs: 30 # 路由表刷新间隔（秒），默认 30s
  request_timeout_secs: 5 # 请求超时（秒），默认 5s
```

### 7. Server Configuration (服务器配置)

```yaml
server:
  routing_sync_interval_secs: 5 # 路由表同步间隔（秒），默认 5s
```

### 8. Log Configuration (日志配置)

```yaml
log:
  level: "info" # 日志级别: trace, debug, info, warn, error，默认 info
```

## 使用方法

### 从配置文件启动

```bash
redraft --config config.yaml
```

### 命令行参数覆盖

命令行参数会覆盖配置文件中的对应项：

```bash
redraft --config config.yaml --node-id node2 --redis-addr 0.0.0.0:6379
```

### 默认配置

如果不指定配置文件，将使用默认配置。可以通过 `--config` 参数指定配置文件路径。

## 配置示例

完整配置示例请参考 `config.example.yaml` 文件。
