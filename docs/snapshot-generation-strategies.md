# 不同存储方案的快照生成策略

## 1. 问题分析

不同的存储方案有不同的数据组织方式和持久化机制，因此快照生成策略也应该不同：

- **Redis 内存存储**：数据全部在内存中，需要完整遍历生成快照
- **RocksDB 持久化存储**：数据已经在磁盘上，可以利用其快照机制
- **其他存储方案**：可能有各自的优化方式

## 2. 当前实现分析

### 2.1 Redis 内存存储（MemoryStore）

**当前实现**：
```rust
// crates/redisstore/src/memory.rs

fn create_snapshot(&self) -> Result<Vec<u8>, String> {
    let data = self.data.lock();
    bincode::serde::encode_to_vec(&*data, bincode::config::standard())
        .map_err(|e| format!("Failed to serialize snapshot: {}", e))
}
```

**特点**：
- 数据全部在内存中（`HashMap`）
- 需要完整遍历所有 key-value 对
- 序列化为二进制格式
- 适合小到中等规模的数据

**问题**：
- 对于大数据集（如 100GB），完整遍历和序列化会：
  - 占用大量内存（完整快照数据）
  - 阻塞主线程（如果同步执行）
  - 耗时较长

### 2.2 RocksDB 持久化存储（假设实现）

**RocksDB 的特点**：
- 数据已经在磁盘上（SST 文件）
- 支持快照（Snapshot）和检查点（Checkpoint）
- 支持增量快照
- 不需要完整遍历数据

**潜在实现**：
```rust
// 假设的 RocksDB 实现

impl RedisStore for RocksDBStore {
    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        // 方案 A: 使用 RocksDB Checkpoint
        // 直接复制 SST 文件，不需要遍历数据
        let checkpoint = self.db.checkpoint()?;
        checkpoint.create_checkpoint(&snapshot_path)?;
        
        // 打包 SST 文件
        self.package_sst_files(&snapshot_path)
    }
    
    fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<Vec<u8>, String> {
        // 方案 B: 使用 RocksDB Iterator + 过滤
        // 只遍历 slot_range 范围内的 key
        let mut snapshot_data = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        
        for item in iter {
            let (key, value) = item?;
            let slot = crc16::crc16(&key) % 16384;
            
            if slot >= slot_range.0 && slot < slot_range.1 {
                snapshot_data.push((key, value));
            }
        }
        
        bincode::encode_to_vec(&snapshot_data, bincode::config::standard())
    }
}
```

## 3. 不同存储方案的快照生成策略

### 3.1 Redis 内存存储（MemoryStore）

#### 策略 A: 完整遍历（当前实现）

**适用场景**：
- 数据量较小（< 10GB）
- 内存充足
- 对快照生成时间要求不高

**实现**：
```rust
fn create_snapshot(&self) -> Result<Vec<u8>, String> {
    let data = self.data.lock();
    // 完整序列化所有数据
    bincode::encode_to_vec(&*data, bincode::config::standard())
}
```

**优点**：
- 实现简单
- 快照完整、一致性好
- 恢复速度快（直接反序列化）

**缺点**：
- 大数据集时内存占用大
- 生成时间长
- 可能阻塞主线程

#### 策略 B: 增量快照（Incremental Snapshot）

**适用场景**：
- 数据量大（> 10GB）
- 需要频繁生成快照
- 可以容忍一定的复杂度

**实现思路**：
```rust
struct IncrementalSnapshot {
    base_snapshot_index: u64,  // 基础快照索引
    incremental_log: Vec<LogEntry>,  // 增量日志
}

fn create_incremental_snapshot(&self, base_index: u64) -> Result<IncrementalSnapshot, String> {
    // 1. 记录基础快照索引
    // 2. 记录自基础快照以来的所有变更
    // 3. 返回增量快照
}
```

**优点**：
- 快照体积小
- 生成速度快
- 可以基于上一个快照

**缺点**：
- 恢复需要应用增量日志
- 实现复杂
- 需要维护快照链

#### 策略 C: 流式快照（Streaming Snapshot）

**适用场景**：
- 数据量非常大（> 100GB）
- 需要边生成边传输
- 内存受限

**实现思路**：
```rust
fn create_snapshot_stream(&self) -> impl Stream<Item = Result<Vec<u8>, String>> {
    // 分块生成快照
    // 每次返回一个 chunk
    stream::unfold(self.data.lock(), |data| async move {
        // 生成下一个 chunk
        let chunk = data.next_chunk(1024 * 1024);  // 1MB chunks
        Some((chunk, data))
    })
}
```

**优点**：
- 内存占用小
- 可以边生成边传输
- 不阻塞主线程

**缺点**：
- 实现复杂
- 需要处理流式传输

### 3.2 RocksDB 持久化存储

#### 策略 A: Checkpoint 快照（推荐）

**适用场景**：
- 数据已经在磁盘上
- 需要快速生成快照
- 不需要遍历数据

**实现**：
```rust
fn create_snapshot(&self) -> Result<Vec<u8>, String> {
    // 1. 创建 RocksDB Checkpoint
    let checkpoint = self.db.checkpoint()?;
    let checkpoint_path = format!("/tmp/checkpoint_{}", timestamp());
    checkpoint.create_checkpoint(&checkpoint_path)?;
    
    // 2. 打包 SST 文件
    // 不需要遍历数据，直接复制文件
    self.package_sst_files(&checkpoint_path)
}
```

**优点**：
- **不需要遍历数据**，直接复制文件
- 生成速度快（文件系统操作）
- 内存占用小
- 快照体积小（RocksDB 压缩）

**缺点**：
- 需要 RocksDB 支持 Checkpoint
- 快照包含所有数据（不能只包含部分 slot）

#### 策略 B: Iterator + 过滤（用于分裂快照）

**适用场景**：
- 需要生成分裂快照（只包含部分 slot）
- 数据已经在磁盘上

**实现**：
```rust
fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<Vec<u8>, String> {
    let mut snapshot_data = Vec::new();
    
    // 使用 RocksDB Iterator，只遍历需要的 key
    let iter = self.db.iterator(rocksdb::IteratorMode::Start);
    
    for item in iter {
        let (key, value) = item?;
        let slot = crc16::crc16(&key) % 16384;
        
        // 只包含 slot_range 范围内的数据
        if slot >= slot_range.0 && slot < slot_range.1 {
            snapshot_data.push((key, value));
        }
    }
    
    bincode::encode_to_vec(&snapshot_data, bincode::config::standard())
}
```

**优点**：
- 只遍历需要的 key
- 快照体积小（只包含部分数据）
- 适合分裂场景

**缺点**：
- 仍然需要遍历（但可以跳过不需要的 key）
- 对于大数据集，遍历时间较长

#### 策略 C: 混合方案（Checkpoint + 过滤）

**适用场景**：
- 需要快速生成完整快照
- 也需要支持分裂快照

**实现**：
```rust
fn create_snapshot(&self) -> Result<Vec<u8>, String> {
    // 完整快照：使用 Checkpoint
    self.create_checkpoint_snapshot()
}

fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<Vec<u8>, String> {
    // 分裂快照：使用 Iterator + 过滤
    self.create_filtered_snapshot(slot_range)
}
```

### 3.3 其他存储方案

#### 方案 A: 对象存储（S3、OSS 等）

**策略**：
- 直接使用存储的版本控制功能
- 或者定期上传快照到对象存储
- 不需要在本地生成完整快照

#### 方案 B: 分布式存储（TiKV、CockroachDB 等）

**策略**：
- 利用分布式存储的快照机制
- 可能需要协调多个节点
- 使用 MVCC 快照

## 4. 设计建议

### 4.1 抽象快照生成接口

```rust
// crates/redisstore/src/traits.rs

pub trait SnapshotStrategy: Send + Sync {
    /// 创建完整快照
    fn create_full_snapshot(&self) -> Result<Vec<u8>, String>;
    
    /// 创建分裂快照（只包含指定 slot 范围）
    fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<Vec<u8>, String>;
    
    /// 创建增量快照（可选）
    fn create_incremental_snapshot(&self, base_index: u64) -> Result<Vec<u8>, String> {
        // 默认实现：不支持增量快照
        Err("Incremental snapshot not supported".to_string())
    }
    
    /// 创建流式快照（可选）
    fn create_streaming_snapshot(&self) -> Result<impl Stream<Item = Result<Vec<u8>, String>>, String> {
        // 默认实现：不支持流式快照
        Err("Streaming snapshot not supported".to_string())
    }
}
```

### 4.2 不同存储实现不同策略

```rust
// Redis 内存存储：完整遍历
impl SnapshotStrategy for MemoryStore {
    fn create_full_snapshot(&self) -> Result<Vec<u8>, String> {
        let data = self.data.lock();
        bincode::encode_to_vec(&*data, bincode::config::standard())
    }
    
    fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<Vec<u8>, String> {
        let data = self.data.lock();
        let filtered: HashMap<_, _> = data.iter()
            .filter(|(k, _)| {
                let slot = crc16::crc16(k) % 16384;
                slot >= slot_range.0 && slot < slot_range.1
            })
            .collect();
        bincode::encode_to_vec(&filtered, bincode::config::standard())
    }
}

// RocksDB 存储：Checkpoint 快照
impl SnapshotStrategy for RocksDBStore {
    fn create_full_snapshot(&self) -> Result<Vec<u8>, String> {
        // 使用 Checkpoint，不需要遍历
        let checkpoint = self.db.checkpoint()?;
        let checkpoint_path = format!("/tmp/checkpoint_{}", timestamp());
        checkpoint.create_checkpoint(&checkpoint_path)?;
        self.package_sst_files(&checkpoint_path)
    }
    
    fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<Vec<u8>, String> {
        // 使用 Iterator + 过滤
        let mut snapshot_data = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        
        for item in iter {
            let (key, value) = item?;
            let slot = crc16::crc16(&key) % 16384;
            
            if slot >= slot_range.0 && slot < slot_range.1 {
                snapshot_data.push((key, value));
            }
        }
        
        bincode::encode_to_vec(&snapshot_data, bincode::config::standard())
    }
}
```

### 4.3 统一接口

```rust
// crates/redisstore/src/traits.rs

pub trait RedisStore: Send + Sync {
    // ... 其他方法 ...
    
    /// 创建快照（由具体实现选择策略）
    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        // 默认实现：使用完整快照
        self.snapshot_strategy().create_full_snapshot()
    }
    
    /// 获取快照策略（由具体实现提供）
    fn snapshot_strategy(&self) -> &dyn SnapshotStrategy;
}
```

## 5. 性能对比

### 5.1 Redis 内存存储

| 策略 | 数据量 | 生成时间 | 内存占用 | 快照体积 |
|------|--------|---------|---------|---------|
| 完整遍历 | 10GB | ~10s | 10GB | 10GB |
| 完整遍历 | 100GB | ~100s | 100GB | 100GB |
| 增量快照 | 100GB | ~1s | 1GB | 1GB |
| 流式快照 | 100GB | ~100s | 100MB | 100GB |

### 5.2 RocksDB 存储

| 策略 | 数据量 | 生成时间 | 内存占用 | 快照体积 |
|------|--------|---------|---------|---------|
| Checkpoint | 100GB | ~5s | 100MB | 30GB (压缩后) |
| Iterator + 过滤 | 100GB (10% slot) | ~10s | 100MB | 3GB |
| Iterator + 过滤 | 100GB (完整) | ~100s | 100MB | 30GB |

## 6. 推荐方案

### 6.1 Redis 内存存储

**小数据集（< 10GB）**：
- 使用完整遍历（当前实现）
- 简单、可靠

**大数据集（> 10GB）**：
- 考虑增量快照或流式快照
- 或者切换到 RocksDB 等持久化存储

### 6.2 RocksDB 存储

**完整快照**：
- **使用 Checkpoint 机制**（推荐）
- 不需要遍历数据，直接复制文件
- 生成速度快，内存占用小

**分裂快照**：
- 使用 Iterator + 过滤
- 只遍历需要的 key
- 快照体积小

### 6.3 混合方案

- 完整快照：使用 Checkpoint（RocksDB）或完整遍历（MemoryStore）
- 分裂快照：使用 Iterator + 过滤（两种存储都支持）
- 根据数据量动态选择策略

## 7. 实现建议

1. **抽象快照策略接口**：`SnapshotStrategy` trait
2. **不同存储实现不同策略**：
   - MemoryStore: 完整遍历
   - RocksDBStore: Checkpoint（完整）+ Iterator（分裂）
3. **统一接口**：`RedisStore::create_snapshot()` 调用具体策略
4. **可配置**：允许配置快照生成策略
5. **监控**：记录快照生成时间、体积等指标

## 8. 总结

- **Redis 内存存储**：需要完整遍历，适合小数据集
- **RocksDB 存储**：**不需要完整遍历**，可以使用 Checkpoint 机制
- **分裂快照**：两种存储都需要遍历（但可以过滤）
- **设计原则**：抽象快照策略，不同存储实现不同策略
