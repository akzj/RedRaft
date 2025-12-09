# 基于 Shard 的存储架构设计

## 1. 核心思想

### 1.1 问题分析

**当前问题**：
- 生成分裂快照时需要遍历所有 key，过滤出指定 slot 范围的数据
- 对于大数据集（100GB+），遍历时间很长
- 内存存储和 RocksDB 都需要遍历

**解决方案**：
- 在存储层加入天然的 shard 概念
- 每个 shard 对应一个独立的存储单元
- 快照时直接按 shard 复制，不需要遍历
- 查询时先定位 shard，再在 shard 内查询

### 1.2 架构优势

1. **快照生成**：
   - 不需要遍历所有 key
   - 直接按 shard 复制存储单元
   - 生成速度快（文件系统操作）

2. **查询性能**：
   - 先定位 shard，缩小查询范围
   - 减少不必要的 I/O

3. **数据隔离**：
   - 每个 shard 独立存储
   - 便于管理和维护

## 2. 存储层 Shard 设计

### 2.1 Shard 抽象

```rust
// crates/redisstore/src/storage/shard.rs

/// Shard ID（对应 slot 范围）
pub type ShardId = u16;

/// Shard 存储抽象
pub trait ShardStorage: Send + Sync {
    /// 获取 shard ID
    fn shard_id(&self) -> ShardId;
    
    /// 创建快照（整个 shard）
    fn create_snapshot(&self) -> Result<Vec<u8>, String>;
    
    /// 从快照恢复
    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String>;
    
    /// 获取 shard 中的 key 数量
    fn key_count(&self) -> usize;
    
    /// 获取 shard 大小（字节）
    fn size(&self) -> u64;
}

/// Shard 管理器
pub trait ShardManager: Send + Sync {
    /// 根据 key 获取 shard ID
    fn get_shard_id(&self, key: &[u8]) -> ShardId;
    
    /// 获取 shard 存储
    fn get_shard(&self, shard_id: ShardId) -> Option<Arc<dyn ShardStorage>>;
    
    /// 创建所有 shard 的快照
    fn create_all_snapshots(&self) -> Result<HashMap<ShardId, Vec<u8>>, String>;
    
    /// 创建指定 shard 范围的快照
    fn create_shard_range_snapshot(&self, shard_range: (ShardId, ShardId)) -> Result<HashMap<ShardId, Vec<u8>>, String>;
}
```

### 2.2 Key 到 Shard 的映射

```rust
// crates/redisstore/src/storage/shard.rs

/// 计算 key 的 shard ID
pub fn key_to_shard_id(key: &[u8], total_shards: u16) -> ShardId {
    // 使用 CRC16 计算 slot，然后映射到 shard
    let slot = crc16::crc16(key) % 16384;
    slot / (16384 / total_shards)
}

/// 计算 shard 对应的 slot 范围
pub fn shard_to_slot_range(shard_id: ShardId, total_shards: u16) -> (u16, u16) {
    let slots_per_shard = 16384 / total_shards;
    let start = shard_id * slots_per_shard;
    let end = if shard_id == total_shards - 1 {
        16384  // 最后一个 shard 包含剩余的 slot
    } else {
        (shard_id + 1) * slots_per_shard
    };
    (start, end)
}
```

## 3. RocksDB 使用 ColumnFamily 作为 Shard

### 3.1 ColumnFamily 的优势

**RocksDB ColumnFamily 特性**：
- 每个 ColumnFamily 有独立的 MemTable 和 SST 文件
- 可以独立进行快照、压缩、检查点等操作
- 支持独立配置（压缩算法、写缓冲区等）
- 查询时指定 ColumnFamily，性能更好

**使用 shard_id 作为 ColumnFamily 的意义**：
- ✅ **快照生成**：直接对 ColumnFamily 创建 Checkpoint，不需要遍历
- ✅ **分裂快照**：直接复制指定的 ColumnFamily
- ✅ **查询性能**：先定位 ColumnFamily，再查询，减少 I/O
- ✅ **数据隔离**：每个 shard 独立存储，便于管理

### 3.2 RocksDB Shard 存储实现

```rust
// crates/redisstore/src/storage/rocksdb_shard.rs

use rocksdb::{DB, ColumnFamilyDescriptor, Options, IteratorMode};

pub struct RocksDBShardStorage {
    db: Arc<DB>,
    shard_id: ShardId,
    cf_handle: Arc<rocksdb::ColumnFamily>,
}

impl RocksDBShardStorage {
    pub fn new(db: Arc<DB>, shard_id: ShardId) -> Result<Self, String> {
        // 获取或创建 ColumnFamily
        let cf_name = format!("shard_{}", shard_id);
        let cf_handle = db.cf_handle(&cf_name)
            .ok_or_else(|| format!("ColumnFamily {} not found", cf_name))?;
        
        Ok(Self {
            db,
            shard_id,
            cf_handle: Arc::new(cf_handle),
        })
    }
    
    /// 创建快照（使用 Checkpoint）
    pub fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        // 1. 创建 Checkpoint（只包含当前 ColumnFamily）
        let checkpoint = self.db.checkpoint()?;
        let checkpoint_path = format!("/tmp/checkpoint_shard_{}_{}", self.shard_id, timestamp());
        checkpoint.create_checkpoint(&checkpoint_path)?;
        
        // 2. 只打包当前 ColumnFamily 的 SST 文件
        let cf_sst_files = self.package_cf_sst_files(&checkpoint_path, &self.cf_handle)?;
        
        Ok(cf_sst_files)
    }
    
    /// 打包 ColumnFamily 的 SST 文件
    fn package_cf_sst_files(
        &self,
        checkpoint_path: &str,
        cf_handle: &rocksdb::ColumnFamily,
    ) -> Result<Vec<u8>, String> {
        // 获取 ColumnFamily 的 SST 文件列表
        let cf_name = cf_handle.name();
        let cf_path = format!("{}/{}", checkpoint_path, cf_name);
        
        // 打包所有 SST 文件
        let mut archive = Vec::new();
        for entry in std::fs::read_dir(&cf_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "sst") {
                let data = std::fs::read(&path)?;
                archive.extend_from_slice(&data);
            }
        }
        
        Ok(archive)
    }
}

impl ShardStorage for RocksDBShardStorage {
    fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    
    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        self.create_snapshot()
    }
    
    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String> {
        // 解包 SST 文件并恢复到 ColumnFamily
        // ...
    }
    
    fn key_count(&self) -> usize {
        // 使用 ColumnFamily 的统计信息
        let mut count = 0;
        let iter = self.db.iterator_cf(&self.cf_handle, IteratorMode::Start);
        for _ in iter {
            count += 1;
        }
        count
    }
    
    fn size(&self) -> u64 {
        // 获取 ColumnFamily 的大小
        // ...
    }
}
```

### 3.3 RocksDB Shard 管理器

```rust
// crates/redisstore/src/storage/rocksdb_shard_manager.rs

pub struct RocksDBShardManager {
    db: Arc<DB>,
    total_shards: u16,
    shards: Arc<RwLock<HashMap<ShardId, Arc<RocksDBShardStorage>>>>,
}

impl RocksDBShardManager {
    pub fn new(db_path: &str, total_shards: u16) -> Result<Self, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        
        // 创建所有 ColumnFamily
        let mut cfs = Vec::new();
        for shard_id in 0..total_shards {
            let cf_name = format!("shard_{}", shard_id);
            cfs.push(ColumnFamilyDescriptor::new(cf_name, Options::default()));
        }
        
        let db = DB::open_cf_descriptors(&opts, db_path, cfs)?;
        
        // 初始化所有 shard 存储
        let mut shards = HashMap::new();
        for shard_id in 0..total_shards {
            let shard = Arc::new(RocksDBShardStorage::new(db.clone(), shard_id)?);
            shards.insert(shard_id, shard);
        }
        
        Ok(Self {
            db: Arc::new(db),
            total_shards,
            shards: Arc::new(RwLock::new(shards)),
        })
    }
    
    /// 根据 key 获取 shard 并写入
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        let shard_id = key_to_shard_id(key, self.total_shards);
        let shard = self.get_shard(shard_id)
            .ok_or_else(|| format!("Shard {} not found", shard_id))?;
        
        // 写入到对应的 ColumnFamily
        let cf_handle = shard.cf_handle();
        self.db.put_cf(cf_handle, key, value)?;
        Ok(())
    }
    
    /// 根据 key 获取 shard 并读取
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let shard_id = key_to_shard_id(key, self.total_shards);
        let shard = self.get_shard(shard_id)
            .ok_or_else(|| format!("Shard {} not found", shard_id))?;
        
        // 从对应的 ColumnFamily 读取
        let cf_handle = shard.cf_handle();
        match self.db.get_cf(cf_handle, key)? {
            Some(value) => Ok(Some(value)),
            None => Ok(None),
        }
    }
}

impl ShardManager for RocksDBShardManager {
    fn get_shard_id(&self, key: &[u8]) -> ShardId {
        key_to_shard_id(key, self.total_shards)
    }
    
    fn get_shard(&self, shard_id: ShardId) -> Option<Arc<dyn ShardStorage>> {
        self.shards.read().get(&shard_id).cloned()
    }
    
    fn create_all_snapshots(&self) -> Result<HashMap<ShardId, Vec<u8>>, String> {
        let mut snapshots = HashMap::new();
        let shards = self.shards.read();
        
        // 并行创建所有 shard 的快照
        let futures: Vec<_> = shards.iter()
            .map(|(shard_id, shard)| {
                let shard = shard.clone();
                let shard_id = *shard_id;
                tokio::spawn(async move {
                    let snapshot = shard.create_snapshot()?;
                    Ok::<_, String>((shard_id, snapshot))
                })
            })
            .collect();
        
        for future in futures {
            let (shard_id, snapshot) = future.await??;
            snapshots.insert(shard_id, snapshot);
        }
        
        Ok(snapshots)
    }
    
    fn create_shard_range_snapshot(
        &self,
        shard_range: (ShardId, ShardId),
    ) -> Result<HashMap<ShardId, Vec<u8>>, String> {
        let mut snapshots = HashMap::new();
        let shards = self.shards.read();
        
        // 只创建指定范围的 shard 快照
        for shard_id in shard_range.0..=shard_range.1 {
            if let Some(shard) = shards.get(&shard_id) {
                let snapshot = shard.create_snapshot()?;
                snapshots.insert(shard_id, snapshot);
            }
        }
        
        Ok(snapshots)
    }
}
```

## 4. 内存存储的 Shard 实现

### 4.1 内存 Shard 存储

```rust
// crates/redisstore/src/storage/memory_shard.rs

pub struct MemoryShardStorage {
    shard_id: ShardId,
    data: Arc<RwLock<HashMap<Vec<u8>, Entry>>>,
}

impl MemoryShardStorage {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl ShardStorage for MemoryShardStorage {
    fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    
    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        // 直接序列化整个 shard 的数据
        let data = self.data.read();
        bincode::encode_to_vec(&*data, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize shard snapshot: {}", e))
    }
    
    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String> {
        let data: HashMap<Vec<u8>, Entry> = 
            bincode::decode_from_slice(snapshot, bincode::config::standard())
                .map(|(data, _)| data)
                .map_err(|e| format!("Failed to deserialize shard snapshot: {}", e))?;
        
        let mut current_data = self.data.write();
        *current_data = data;
        Ok(())
    }
    
    fn key_count(&self) -> usize {
        self.data.read().len()
    }
    
    fn size(&self) -> u64 {
        // 估算大小
        let data = self.data.read();
        data.iter().map(|(k, v)| k.len() + v.size()).sum::<usize>() as u64
    }
}
```

### 4.2 内存 Shard 管理器

```rust
// crates/redisstore/src/storage/memory_shard_manager.rs

pub struct MemoryShardManager {
    total_shards: u16,
    shards: Arc<RwLock<HashMap<ShardId, Arc<MemoryShardStorage>>>>,
}

impl MemoryShardManager {
    pub fn new(total_shards: u16) -> Self {
        let mut shards = HashMap::new();
        for shard_id in 0..total_shards {
            shards.insert(shard_id, Arc::new(MemoryShardStorage::new(shard_id)));
        }
        
        Self {
            total_shards,
            shards: Arc::new(RwLock::new(shards)),
        }
    }
    
    /// 根据 key 获取 shard 并写入
    pub fn put(&self, key: Vec<u8>, entry: Entry) {
        let shard_id = key_to_shard_id(&key, self.total_shards);
        let shard = self.get_shard(shard_id).unwrap();
        let mut data = shard.data.write();
        data.insert(key, entry);
    }
    
    /// 根据 key 获取 shard 并读取
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        let shard_id = key_to_shard_id(key, self.total_shards);
        let shard = self.get_shard(shard_id)?;
        let data = shard.data.read();
        data.get(key).cloned()
    }
}

impl ShardManager for MemoryShardManager {
    fn get_shard_id(&self, key: &[u8]) -> ShardId {
        key_to_shard_id(key, self.total_shards)
    }
    
    fn get_shard(&self, shard_id: ShardId) -> Option<Arc<dyn ShardStorage>> {
        self.shards.read().get(&shard_id).cloned()
    }
    
    fn create_all_snapshots(&self) -> Result<HashMap<ShardId, Vec<u8>>, String> {
        let mut snapshots = HashMap::new();
        let shards = self.shards.read();
        
        for (shard_id, shard) in shards.iter() {
            let snapshot = shard.create_snapshot()?;
            snapshots.insert(*shard_id, snapshot);
        }
        
        Ok(snapshots)
    }
    
    fn create_shard_range_snapshot(
        &self,
        shard_range: (ShardId, ShardId),
    ) -> Result<HashMap<ShardId, Vec<u8>>, String> {
        let mut snapshots = HashMap::new();
        let shards = self.shards.read();
        
        for shard_id in shard_range.0..=shard_range.1 {
            if let Some(shard) = shards.get(&shard_id) {
                let snapshot = shard.create_snapshot()?;
                snapshots.insert(shard_id, snapshot);
            }
        }
        
        Ok(snapshots)
    }
}
```

## 5. 统一存储接口

### 5.1 RedisStore 扩展

```rust
// crates/redisstore/src/traits.rs

pub trait RedisStore: Send + Sync {
    // ... 其他方法 ...
    
    /// 创建快照（使用 shard 机制）
    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        let shard_manager = self.shard_manager();
        let shard_snapshots = shard_manager.create_all_snapshots()?;
        
        // 序列化所有 shard 快照
        self.serialize_shard_snapshots(&shard_snapshots)
    }
    
    /// 创建分裂快照（只包含指定 slot 范围）
    fn create_split_snapshot(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        let shard_manager = self.shard_manager();
        
        // 计算涉及的 shard 范围
        let shard_start = slot_start / (total_slots / shard_manager.total_shards());
        let shard_end = slot_end / (total_slots / shard_manager.total_shards());
        
        // 只创建涉及 shard 的快照
        let shard_snapshots = shard_manager.create_shard_range_snapshot((
            shard_start as u16,
            shard_end as u16,
        ))?;
        
        self.serialize_shard_snapshots(&shard_snapshots)
    }
    
    /// 获取 shard 管理器
    fn shard_manager(&self) -> &dyn ShardManager;
    
    /// 序列化 shard 快照
    fn serialize_shard_snapshots(
        &self,
        snapshots: &HashMap<ShardId, Vec<u8>>,
    ) -> Result<Vec<u8>, String> {
        bincode::encode_to_vec(snapshots, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize shard snapshots: {}", e))
    }
}
```

### 5.2 查询流程

```rust
// 查询 key
pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
    // 1. 计算 shard_id
    let shard_id = self.shard_manager().get_shard_id(key);
    
    // 2. 获取 shard 存储
    let shard = self.shard_manager().get_shard(shard_id)?;
    
    // 3. 在 shard 内查询
    match shard.backend_type() {
        StorageBackend::RocksDB => {
            // 从 RocksDB ColumnFamily 查询
            self.rocksdb_manager.get(key, shard_id)
        }
        StorageBackend::Memory => {
            // 从内存 shard 查询
            self.memory_manager.get(key, shard_id)
        }
    }
}
```

## 6. 性能对比

### 6.1 快照生成性能

| 方案 | 100GB 数据生成时间 | 是否需要遍历 |
|------|------------------|-------------|
| **当前方案（遍历）** | ~100s | 是 |
| **Shard 方案（RocksDB）** | ~5s | 否（直接复制 ColumnFamily） |
| **Shard 方案（内存）** | ~10s | 否（直接序列化 shard） |

### 6.2 查询性能

| 方案 | 查询步骤 | I/O 次数 |
|------|---------|---------|
| **当前方案** | 1. 查询所有数据<br>2. 过滤 | 高 |
| **Shard 方案** | 1. 计算 shard_id<br>2. 在 shard 内查询 | 低（只查询一个 shard） |

## 7. ColumnFamily 的优势总结

### 7.1 快照生成

- ✅ **不需要遍历**：直接对 ColumnFamily 创建 Checkpoint
- ✅ **速度快**：文件系统操作，不涉及数据遍历
- ✅ **体积小**：只包含指定 ColumnFamily 的数据

### 7.2 分裂快照

- ✅ **精确**：直接复制指定的 ColumnFamily
- ✅ **快速**：不需要过滤，直接复制文件

### 7.3 查询性能

- ✅ **定位快**：先定位 ColumnFamily，再查询
- ✅ **I/O 少**：只查询一个 ColumnFamily，减少 I/O

### 7.4 数据隔离

- ✅ **独立存储**：每个 shard 独立存储
- ✅ **独立配置**：可以为每个 ColumnFamily 配置不同的压缩算法等
- ✅ **便于管理**：可以独立进行压缩、检查点等操作

## 8. 实现建议

### 8.1 阶段 1: 基础架构

1. 定义 `ShardStorage` 和 `ShardManager` trait
2. 实现 key 到 shard_id 的映射
3. 实现 shard 到 slot 范围的映射

### 8.2 阶段 2: RocksDB Shard

1. 使用 ColumnFamily 作为 shard
2. 实现 `RocksDBShardStorage`
3. 实现 `RocksDBShardManager`

### 8.3 阶段 3: 内存 Shard

1. 实现 `MemoryShardStorage`
2. 实现 `MemoryShardManager`

### 8.4 阶段 4: 统一接口

1. 扩展 `RedisStore` trait
2. 实现快照生成（使用 shard）
3. 实现分裂快照（使用 shard）

## 9. 注意事项

### 9.1 Shard 数量选择

- **太少**：shard 内数据量大，快照仍然慢
- **太多**：管理开销大，ColumnFamily 数量多
- **推荐**：根据数据量选择，通常 64-256 个 shard

### 9.2 动态 Shard 调整

- 如果需要动态调整 shard 数量，需要数据迁移
- 可以考虑使用一致性哈希

### 9.3 兼容性

- 需要保证与现有代码的兼容性
- 可以逐步迁移到 shard 架构

## 10. 总结

- **Shard 概念**：在存储层加入 shard 概念，避免遍历
- **RocksDB ColumnFamily**：使用 shard_id 作为 ColumnFamily 是有意义的
- **快照生成**：直接按 shard 复制，不需要遍历
- **查询性能**：先定位 shard，再查询，减少 I/O
- **数据隔离**：每个 shard 独立存储，便于管理
