# Shard 隔离与并发控制

## 1. 问题分析

### 1.1 问题场景

当在存储层加入 shard 概念后，生成快照时需要考虑：

- **锁的影响范围**：如果所有 shard 共享同一个锁，生成一个 shard 的快照时，会阻塞其他 shard 的读写操作
- **并发性能**：不同 shard 的操作应该可以并发执行
- **快照隔离**：生成 shard A 的快照时，不应该影响 shard B 的读写

### 1.2 设计目标

1. **Shard 独立锁**：每个 shard 有独立的锁，互不影响
2. **快照隔离**：生成快照时只锁定当前 shard
3. **并发性能**：不同 shard 的操作可以并发执行
4. **最小化锁时间**：快照生成时尽量不阻塞读写

## 2. Shard 独立锁设计

### 2.1 内存存储的 Shard 独立锁

```rust
// crates/redisstore/src/storage/memory_shard.rs

use parking_lot::RwLock;
use std::sync::Arc;

pub struct MemoryShardStorage {
    shard_id: ShardId,
    /// 每个 shard 有独立的读写锁
    data: Arc<RwLock<HashMap<Vec<u8>, Entry>>>,
    /// 快照生成锁（用于防止并发快照）
    snapshot_lock: Arc<tokio::sync::Mutex<()>>,
}

impl MemoryShardStorage {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            data: Arc::new(RwLock::new(HashMap::new())),
            snapshot_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    
    /// 读取操作（只锁定当前 shard）
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        // 只获取当前 shard 的读锁，不影响其他 shard
        let data = self.data.read();
        data.get(key).cloned()
    }
    
    /// 写入操作（只锁定当前 shard）
    pub fn put(&self, key: Vec<u8>, entry: Entry) {
        // 只获取当前 shard 的写锁，不影响其他 shard
        let mut data = self.data.write();
        data.insert(key, entry);
    }
    
    /// 创建快照（只锁定当前 shard）
    pub fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        // 1. 获取快照锁（防止并发快照）
        let _snapshot_guard = self.snapshot_lock.blocking_lock();
        
        // 2. 获取读锁（快照期间允许并发读，但不允许写）
        let data = self.data.read();
        
        // 3. 序列化（不持有写锁，不影响其他操作）
        let snapshot = bincode::encode_to_vec(&*data, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize shard snapshot: {}", e))?;
        
        // 4. 读锁自动释放，快照锁自动释放
        Ok(snapshot)
    }
    
    /// 创建快照（异步，最小化锁时间）
    pub async fn create_snapshot_async(&self) -> Result<Vec<u8>, String> {
        // 1. 获取快照锁
        let _snapshot_guard = self.snapshot_lock.lock().await;
        
        // 2. 快速获取数据快照（最小化锁时间）
        let data_snapshot = {
            let data = self.data.read();
            // 克隆数据（避免长时间持有锁）
            data.clone()
        };
        
        // 3. 在锁外序列化（不阻塞其他操作）
        let snapshot = bincode::encode_to_vec(&data_snapshot, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize shard snapshot: {}", e))?;
        
        Ok(snapshot)
    }
}
```

### 2.2 RocksDB 的 Shard 独立锁

```rust
// crates/redisstore/src/storage/rocksdb_shard.rs

use rocksdb::{DB, ColumnFamily};
use std::sync::Arc;
use parking_lot::RwLock;

pub struct RocksDBShardStorage {
    db: Arc<DB>,
    shard_id: ShardId,
    cf_handle: Arc<ColumnFamily>,
    /// 快照生成锁（用于防止并发快照）
    snapshot_lock: Arc<tokio::sync::Mutex<()>>,
}

impl RocksDBShardStorage {
    pub fn new(db: Arc<DB>, shard_id: ShardId) -> Result<Self, String> {
        let cf_name = format!("shard_{}", shard_id);
        let cf_handle = db.cf_handle(&cf_name)
            .ok_or_else(|| format!("ColumnFamily {} not found", cf_name))?;
        
        Ok(Self {
            db,
            shard_id,
            cf_handle: Arc::new(cf_handle),
            snapshot_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }
    
    /// 读取操作（RocksDB 本身支持并发读，不需要锁）
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        // RocksDB 的 ColumnFamily 操作是线程安全的
        // 不需要额外的锁，直接查询
        self.db.get_cf(&self.cf_handle, key)
            .map_err(|e| format!("RocksDB get error: {}", e))
    }
    
    /// 写入操作（RocksDB 本身支持并发写，不需要锁）
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        // RocksDB 的 ColumnFamily 操作是线程安全的
        // 不需要额外的锁，直接写入
        self.db.put_cf(&self.cf_handle, key, value)
            .map_err(|e| format!("RocksDB put error: {}", e))
    }
    
    /// 创建快照（使用 Checkpoint，不需要锁）
    pub fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        // 1. 获取快照锁（防止并发快照）
        let _snapshot_guard = self.snapshot_lock.blocking_lock();
        
        // 2. 创建 Checkpoint（RocksDB 的 Checkpoint 是快照，不影响读写）
        let checkpoint = self.db.checkpoint()?;
        let checkpoint_path = format!("/tmp/checkpoint_shard_{}_{}", self.shard_id, timestamp());
        checkpoint.create_checkpoint(&checkpoint_path)?;
        
        // 3. 打包 SST 文件（在后台线程执行，不阻塞）
        let cf_sst_files = self.package_cf_sst_files(&checkpoint_path, &self.cf_handle)?;
        
        // 4. 清理临时文件（可选，在后台执行）
        tokio::spawn(async move {
            let _ = std::fs::remove_dir_all(&checkpoint_path);
        });
        
        Ok(cf_sst_files)
    }
    
    /// 创建快照（异步，最小化锁时间）
    pub async fn create_snapshot_async(&self) -> Result<Vec<u8>, String> {
        // 1. 获取快照锁
        let _snapshot_guard = self.snapshot_lock.lock().await;
        
        // 2. 在后台线程创建 Checkpoint（不阻塞）
        let db = self.db.clone();
        let cf_handle = self.cf_handle.clone();
        let shard_id = self.shard_id;
        
        let checkpoint_result = tokio::task::spawn_blocking(move || {
            let checkpoint = db.checkpoint()?;
            let checkpoint_path = format!("/tmp/checkpoint_shard_{}_{}", shard_id, timestamp());
            checkpoint.create_checkpoint(&checkpoint_path)?;
            Ok::<_, String>(checkpoint_path)
        }).await??;
        
        // 3. 在后台线程打包 SST 文件
        let cf_sst_files = tokio::task::spawn_blocking(move || {
            Self::package_cf_sst_files(&checkpoint_result, &cf_handle)
        }).await??;
        
        Ok(cf_sst_files)
    }
}
```

## 3. Shard 管理器的并发控制

### 3.1 独立 Shard 存储

```rust
// crates/redisstore/src/storage/shard_manager.rs

use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;

pub struct ShardManager {
    /// 每个 shard 有独立的存储，互不影响
    shards: Arc<RwLock<HashMap<ShardId, Arc<dyn ShardStorage>>>>,
}

impl ShardManager {
    /// 获取 shard（不需要全局锁）
    pub fn get_shard(&self, shard_id: ShardId) -> Option<Arc<dyn ShardStorage>> {
        // 只获取读锁，快速查找
        let shards = self.shards.read();
        shards.get(&shard_id).cloned()
    }
    
    /// 创建所有 shard 的快照（并行，互不影响）
    pub async fn create_all_snapshots(&self) -> Result<HashMap<ShardId, Vec<u8>>, String> {
        let shards = self.shards.read();
        
        // 并行创建所有 shard 的快照，每个 shard 独立锁
        let futures: Vec<_> = shards.iter()
            .map(|(shard_id, shard)| {
                let shard = shard.clone();
                let shard_id = *shard_id;
                tokio::spawn(async move {
                    // 每个 shard 使用自己的锁，互不影响
                    let snapshot = shard.create_snapshot_async().await?;
                    Ok::<_, String>((shard_id, snapshot))
                })
            })
            .collect();
        
        let mut snapshots = HashMap::new();
        for future in futures {
            let (shard_id, snapshot) = future.await??;
            snapshots.insert(shard_id, snapshot);
        }
        
        Ok(snapshots)
    }
    
    /// 创建指定 shard 范围的快照（只影响涉及的 shard）
    pub async fn create_shard_range_snapshot(
        &self,
        shard_range: (ShardId, ShardId),
    ) -> Result<HashMap<ShardId, Vec<u8>>, String> {
        let shards = self.shards.read();
        
        // 只创建指定范围的 shard 快照
        let futures: Vec<_> = (shard_range.0..=shard_range.1)
            .filter_map(|shard_id| {
                shards.get(&shard_id).map(|shard| {
                    let shard = shard.clone();
                    let shard_id = shard_id;
                    tokio::spawn(async move {
                        // 只锁定涉及的 shard，不影响其他 shard
                        let snapshot = shard.create_snapshot_async().await?;
                        Ok::<_, String>((shard_id, snapshot))
                    })
                })
            })
            .collect();
        
        let mut snapshots = HashMap::new();
        for future in futures {
            let (shard_id, snapshot) = future.await??;
            snapshots.insert(shard_id, snapshot);
        }
        
        Ok(snapshots)
    }
}
```

## 4. 锁时间优化

### 4.1 最小化锁时间策略

```rust
impl MemoryShardStorage {
    /// 优化版本：最小化锁时间
    pub async fn create_snapshot_optimized(&self) -> Result<Vec<u8>, String> {
        // 1. 快速获取数据快照（最小化读锁时间）
        let data_snapshot = {
            let data = self.data.read();
            // 立即克隆，释放读锁
            data.clone()
        };
        
        // 2. 在锁外序列化（不阻塞其他操作）
        // 序列化可能很耗时，在锁外执行
        tokio::task::spawn_blocking(move || {
            bincode::encode_to_vec(&data_snapshot, bincode::config::standard())
                .map_err(|e| format!("Failed to serialize shard snapshot: {}", e))
        }).await?
    }
}
```

### 4.2 RocksDB Checkpoint 的优势

```rust
impl RocksDBShardStorage {
    /// RocksDB Checkpoint 的优势：
    /// 1. 不需要锁定数据（RocksDB 内部处理）
    /// 2. 快照期间不影响读写操作
    /// 3. 只是文件系统操作，速度快
    pub async fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        // RocksDB Checkpoint 是原子操作，不需要长时间锁
        // 只需要防止并发快照（使用 snapshot_lock）
        let _snapshot_guard = self.snapshot_lock.lock().await;
        
        // Checkpoint 操作很快，不影响读写
        let checkpoint = self.db.checkpoint()?;
        let checkpoint_path = format!("/tmp/checkpoint_shard_{}_{}", self.shard_id, timestamp());
        checkpoint.create_checkpoint(&checkpoint_path)?;
        
        // 打包 SST 文件可以在后台执行
        // ...
    }
}
```

## 5. 并发性能对比

### 5.1 锁时间对比

| 方案 | 锁时间 | 影响范围 |
|------|--------|---------|
| **全局锁** | 快照生成时间 | 所有 shard |
| **Shard 独立锁** | 快照生成时间 | 只影响当前 shard |
| **RocksDB Checkpoint** | 文件系统操作时间 | 几乎不影响读写 |

### 5.2 并发操作示例

```rust
// 场景：同时生成 shard 0 的快照，shard 1 进行读写操作

// Shard 0: 生成快照
tokio::spawn(async {
    shard_0.create_snapshot_async().await;
});

// Shard 1: 正常读写（不受影响）
tokio::spawn(async {
    shard_1.put(key, value).await;  // 可以正常执行
    shard_1.get(key).await;         // 可以正常执行
});

// 两个操作可以并发执行，互不影响
```

## 6. 实现建议

### 6.1 内存存储

1. **每个 shard 独立锁**：`Arc<RwLock<HashMap<...>>>`
2. **快照锁**：防止并发快照
3. **最小化锁时间**：快速克隆数据，在锁外序列化

### 6.2 RocksDB 存储

1. **ColumnFamily 独立**：每个 shard 对应一个 ColumnFamily
2. **RocksDB 并发**：RocksDB 本身支持并发读写，不需要额外锁
3. **Checkpoint 快照**：使用 Checkpoint，几乎不影响读写

### 6.3 Shard 管理器

1. **独立存储**：每个 shard 有独立的存储实例
2. **并行快照**：可以并行创建多个 shard 的快照
3. **最小化全局锁**：只在查找 shard 时使用读锁

## 7. 注意事项

### 7.1 快照一致性

- **时间戳**：所有 shard 使用相同的时间戳（在获取锁之前）
- **原子性**：每个 shard 的快照是原子的

### 7.2 性能考虑

- **锁粒度**：尽量使用细粒度锁（shard 级别）
- **锁时间**：最小化锁持有时间
- **并发快照**：可以并行创建多个 shard 的快照

### 7.3 错误处理

- **快照失败**：一个 shard 快照失败，不影响其他 shard
- **资源清理**：确保快照锁正确释放

## 8. 总结

- **Shard 独立锁**：每个 shard 有独立的锁，互不影响
- **快照隔离**：生成快照时只锁定当前 shard
- **并发性能**：不同 shard 的操作可以并发执行
- **最小化锁时间**：快速获取数据，在锁外序列化
- **RocksDB 优势**：Checkpoint 几乎不影响读写操作
