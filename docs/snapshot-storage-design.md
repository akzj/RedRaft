# 快照存储设计文档

## 1. 架构概述

快照存储系统采用三层架构：
- **WAL (Write-Ahead Log)**: 记录所有写操作，用于增量恢复
- **Segment**: 定期全量快照，用于快速恢复和 WAL 清理
- **Chunk**: 64MB 压缩块，用于流式传输

## 2. 核心组件

### 2.1 WAL (Write-Ahead Log)

**设计目标**：
- 记录所有写操作，包含 `apply_index`、`shard_id` 和 `Command`
- 支持按 `apply_index` 跳过已应用的条目
- 自动轮转，避免单个文件过大

**文件格式**：
```
[entry_size: u32][entry_data: WalEntry]
```

**WalEntry 格式**：
```rust
pub struct WalEntry {
    pub apply_index: u64,
    pub shard_id: ShardId,
    pub command: Vec<u8>, // Serialized Command (bincode)
}
```

**元数据**：
- `wal_metadata.json`: 记录所有 WAL 文件及其 shard 范围
- 格式：`shard_id -> [apply_index_begin, apply_index_end)`

### 2.2 Segment

**设计目标**：
- 定期生成 Memory COW base 的全量快照
- 按 shard 组织，每个 shard 独立目录
- 支持快速恢复和 WAL 清理

**文件结构**：
```
segments/
  shard_0/
    metadata.json
    shard_0-{apply_index}.00001.seg
    shard_0-{apply_index}.00002.seg
    ...
  shard_1/
    ...
```

**SegmentMetadata 格式**：
```rust
pub struct SegmentMetadata {
    pub shard_id: ShardId,
    pub apply_index: u64,
    pub chunk_count: u32,
    pub total_size: u64,
    pub created_at: u64,
    pub checksums: Vec<u32>, // CRC32 for each chunk
    pub chunks: Vec<ChunkInfo>,
}
```

**生成策略**：
- 触发条件：WAL 大小 >= 阈值 或 时间间隔 >= 1小时
- 使用读锁，不阻塞写操作（COW 机制）
- 生成完成后，清理不需要的 WAL 文件

### 2.3 Chunk

**设计目标**：
- 64MB (未压缩) 大小的数据块
- zstd 压缩，CRC32 校验
- 支持流式传输

**文件格式**：
```
[header_size: u32][ChunkHeader][entry_count: u32][crc32: u32][compressed_data: Vec<u8>]
```

**ChunkHeader 格式**：
```rust
pub struct ChunkHeader {
    pub chunk_id: u32,
    pub entry_count: u32,
    pub uncompressed_size: u64,
    pub compressed_size: u64,
    pub crc32: u32,
}
```

## 3. 集成方案

### 3.1 HybridStore 扩展

```rust
pub struct HybridStore {
    shards: Arc<RwLock<HashMap<ShardId, LockedShardedStore>>>,
    shard_count: u32,
    wal_writer: Arc<RwLock<WalWriter>>,      // 新增
    snapshot_config: SnapshotConfig,         // 新增
}
```

### 3.2 命令执行流程

```
Raft apply_command(index, command)
  ↓
HybridStore::apply_with_index(index, command)
  ↓
1. 执行命令: store.apply(command)
2. 提取 key (从 command)
3. 写入 WAL: wal_writer.write_entry(index, command, key)
4. 返回结果
```

### 3.3 锁策略

- **读/写操作**: 使用读锁（共享访问）
- **Segment 生成**: 使用读锁（COW 机制，不阻塞写）
- **快照生成**: 短暂写锁（仅用于创建 checkpoint/COW）

### 3.4 恢复流程

```
1. 加载所有 shard 的 Segment
2. 从 Segment 恢复 Memory 数据
3. 读取 WAL entries (apply_index > segment.apply_index)
4. 应用 WAL entries
5. 继续从 Raft log 恢复
```

## 4. 配置参数

```rust
pub struct SnapshotConfig {
    pub base_dir: PathBuf,              // 快照存储根目录
    pub shard_count: u32,               // Shard 数量
    pub chunk_size: u64,                // Chunk 大小 (64MB)
    pub wal_size_threshold: u64,        // WAL 大小阈值 (100MB)
    pub segment_interval_secs: u64,      // Segment 生成间隔 (3600s)
    pub zstd_level: i32,                // zstd 压缩级别 (3)
}
```

## 5. 错误处理

- **WAL 写入失败**: 记录错误，但不影响命令执行
- **Segment 生成失败**: 记录错误，下次重试
- **恢复失败**: 报告错误，需要人工干预

## 6. 性能优化

- **COW 机制**: Memory 快照仅增加引用计数，不复制数据
- **非阻塞**: Segment 生成使用读锁，不阻塞写操作
- **流式传输**: 快照以 Chunk 为单位流式传输，避免大内存分配
- **并行处理**: 不支持并行压缩/解压（简化实现）

## 7. 文件清理策略

- Segment 生成完成后，计算最小 `apply_index`
- 删除所有 `apply_index_end <= min_apply_index` 的 WAL 文件
- 物理删除文件，更新 metadata

## 8. 测试策略

- **单元测试**: 测试 WAL 写入/读取、Segment 生成/读取
- **集成测试**: 测试完整恢复流程
- **性能测试**: 测试 Segment 生成对写操作的影响

