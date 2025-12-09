# 多存储混合场景下的快照生成策略

## 1. 问题分析

### 1.1 复杂场景

在兼容 Redis 协议的场景下，需要支持多个存储后端混合：

- **RocksDB**：存储 String、Hash 等 key-value 数据
- **内存存储**：存储 List、ZSet 等复杂数据结构
- **超时机制（TTL）**：需要持久化 TTL 信息
- **Stream**：需要基于磁盘的存储（可能使用 RocksDB 或其他）

### 1.2 快照生成的挑战

1. **多存储后端协调**：
   - 需要同时从多个存储后端生成快照
   - 保证快照的一致性（原子性）
   - 处理不同存储的序列化方式

2. **数据结构差异**：
   - RocksDB：SST 文件（可以直接复制）
   - 内存存储：需要序列化
   - Stream：可能需要特殊处理

3. **TTL 信息**：
   - 需要持久化 TTL 信息
   - 恢复时需要重新设置 TTL

4. **一致性保证**：
   - 多个存储后端的快照必须对应同一个时间点
   - 避免数据不一致

## 2. 架构设计

### 2.1 存储后端抽象

```rust
// crates/redisstore/src/storage/mod.rs

/// 存储后端类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackend {
    /// RocksDB 存储（用于 String、Hash 等）
    RocksDB,
    /// 内存存储（用于 List、ZSet 等）
    Memory,
    /// 磁盘存储（用于 Stream 等）
    Disk,
}

/// 存储后端 trait
pub trait StorageBackend: Send + Sync {
    /// 存储后端类型
    fn backend_type(&self) -> StorageBackend;
    
    /// 创建快照
    fn create_snapshot(&self) -> Result<SnapshotData, String>;
    
    /// 从快照恢复
    fn restore_from_snapshot(&self, snapshot: &SnapshotData) -> Result<(), String>;
    
    /// 创建分裂快照（只包含指定 slot 范围）
    fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<SnapshotData, String>;
}

/// 快照数据
#[derive(Debug, Clone)]
pub struct SnapshotData {
    /// 存储后端类型
    pub backend: StorageBackend,
    /// 快照数据（格式由具体后端决定）
    pub data: Vec<u8>,
    /// 元数据（checksum、大小等）
    pub metadata: SnapshotMetadata,
}

/// 快照元数据
#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    /// 快照索引
    pub index: u64,
    /// 快照大小
    pub size: u64,
    /// Checksum
    pub checksum: Vec<u8>,
    /// 包含的 key 数量
    pub key_count: usize,
    /// 包含的 slot 范围（如果是分裂快照）
    pub slot_range: Option<(u16, u16)>,
}
```

### 2.2 多存储协调器

```rust
// crates/redisstore/src/storage/coordinator.rs

/// 多存储协调器
pub struct MultiStorageCoordinator {
    /// RocksDB 存储（String、Hash）
    rocksdb: Arc<dyn StorageBackend>,
    /// 内存存储（List、ZSet）
    memory: Arc<dyn StorageBackend>,
    /// 磁盘存储（Stream）
    disk: Arc<dyn StorageBackend>,
    /// TTL 管理器
    ttl_manager: Arc<TTLManager>,
}

impl MultiStorageCoordinator {
    /// 创建完整快照（协调所有存储后端）
    pub async fn create_snapshot(&self) -> Result<MultiStorageSnapshot, String> {
        // 1. 获取快照时间戳（用于一致性）
        let snapshot_timestamp = std::time::SystemTime::now();
        
        // 2. 并行创建各存储后端的快照
        let (rocksdb_snapshot, memory_snapshot, disk_snapshot, ttl_snapshot) = tokio::join!(
            self.rocksdb.create_snapshot(),
            self.memory.create_snapshot(),
            self.disk.create_snapshot(),
            self.ttl_manager.create_snapshot(),
        );
        
        // 3. 检查错误
        let rocksdb_snapshot = rocksdb_snapshot?;
        let memory_snapshot = memory_snapshot?;
        let disk_snapshot = disk_snapshot?;
        let ttl_snapshot = ttl_snapshot?;
        
        // 4. 组合快照
        Ok(MultiStorageSnapshot {
            timestamp: snapshot_timestamp,
            rocksdb: rocksdb_snapshot,
            memory: memory_snapshot,
            disk: disk_snapshot,
            ttl: ttl_snapshot,
        })
    }
    
    /// 创建分裂快照
    pub async fn create_split_snapshot(
        &self,
        slot_range: (u16, u16),
    ) -> Result<MultiStorageSnapshot, String> {
        let snapshot_timestamp = std::time::SystemTime::now();
        
        // 并行创建各存储后端的分裂快照
        let (rocksdb_snapshot, memory_snapshot, disk_snapshot, ttl_snapshot) = tokio::join!(
            self.rocksdb.create_split_snapshot(slot_range),
            self.memory.create_split_snapshot(slot_range),
            self.disk.create_split_snapshot(slot_range),
            self.ttl_manager.create_split_snapshot(slot_range),
        );
        
        let rocksdb_snapshot = rocksdb_snapshot?;
        let memory_snapshot = memory_snapshot?;
        let disk_snapshot = disk_snapshot?;
        let ttl_snapshot = ttl_snapshot?;
        
        Ok(MultiStorageSnapshot {
            timestamp: snapshot_timestamp,
            rocksdb: rocksdb_snapshot,
            memory: memory_snapshot,
            disk: disk_snapshot,
            ttl: ttl_snapshot,
        })
    }
    
    /// 从快照恢复
    pub async fn restore_from_snapshot(
        &self,
        snapshot: &MultiStorageSnapshot,
    ) -> Result<(), String> {
        // 按顺序恢复（可能需要处理依赖关系）
        self.rocksdb.restore_from_snapshot(&snapshot.rocksdb).await?;
        self.memory.restore_from_snapshot(&snapshot.memory).await?;
        self.disk.restore_from_snapshot(&snapshot.disk).await?;
        self.ttl_manager.restore_from_snapshot(&snapshot.ttl).await?;
        
        Ok(())
    }
}

/// 多存储快照
#[derive(Debug, Clone)]
pub struct MultiStorageSnapshot {
    /// 快照时间戳（用于一致性）
    pub timestamp: std::time::SystemTime,
    /// RocksDB 快照
    pub rocksdb: SnapshotData,
    /// 内存存储快照
    pub memory: SnapshotData,
    /// 磁盘存储快照
    pub disk: SnapshotData,
    /// TTL 快照
    pub ttl: SnapshotData,
}
```

## 3. 各存储后端的快照实现

### 3.1 RocksDB 存储（String、Hash）

```rust
// crates/redisstore/src/storage/rocksdb_backend.rs

pub struct RocksDBBackend {
    db: Arc<rocksdb::DB>,
    /// Key 前缀（用于区分不同类型）
    key_prefix: Vec<u8>,
}

impl StorageBackend for RocksDBBackend {
    fn backend_type(&self) -> StorageBackend {
        StorageBackend::RocksDB
    }
    
    fn create_snapshot(&self) -> Result<SnapshotData, String> {
        // 方案 A: 使用 Checkpoint（推荐，不需要遍历）
        let checkpoint = self.db.checkpoint()?;
        let checkpoint_path = format!("/tmp/checkpoint_{}", timestamp());
        checkpoint.create_checkpoint(&checkpoint_path)?;
        
        // 打包 SST 文件
        let snapshot_data = self.package_sst_files(&checkpoint_path)?;
        
        // 计算元数据
        let metadata = self.calculate_metadata(&snapshot_data)?;
        
        Ok(SnapshotData {
            backend: StorageBackend::RocksDB,
            data: snapshot_data,
            metadata,
        })
    }
    
    fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<SnapshotData, String> {
        // 分裂快照：使用 Iterator + 过滤
        let mut snapshot_entries = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        
        for item in iter {
            let (key, value) = item?;
            
            // 检查 key 是否在 slot_range 范围内
            let slot = crc16::crc16(&key) % 16384;
            if slot >= slot_range.0 && slot < slot_range.1 {
                snapshot_entries.push((key, value));
            }
        }
        
        // 序列化
        let snapshot_data = bincode::encode_to_vec(&snapshot_entries, bincode::config::standard())?;
        
        let metadata = SnapshotMetadata {
            index: 0,  // 由上层设置
            size: snapshot_data.len() as u64,
            checksum: self.calculate_checksum(&snapshot_data),
            key_count: snapshot_entries.len(),
            slot_range: Some(slot_range),
        };
        
        Ok(SnapshotData {
            backend: StorageBackend::RocksDB,
            data: snapshot_data,
            metadata,
        })
    }
    
    fn restore_from_snapshot(&self, snapshot: &SnapshotData) -> Result<(), String> {
        match snapshot.backend {
            StorageBackend::RocksDB => {
                // 如果是 Checkpoint 快照，直接恢复文件
                if self.is_checkpoint_snapshot(&snapshot.data) {
                    self.restore_from_checkpoint(&snapshot.data)?;
                } else {
                    // 如果是序列化快照，反序列化并写入
                    let entries: Vec<(Vec<u8>, Vec<u8>)> = 
                        bincode::decode_from_slice(&snapshot.data, bincode::config::standard())?;
                    
                    for (key, value) in entries {
                        self.db.put(key, value)?;
                    }
                }
                Ok(())
            }
            _ => Err("Invalid backend type".to_string()),
        }
    }
}
```

### 3.2 内存存储（List、ZSet）

```rust
// crates/redisstore/src/storage/memory_backend.rs

pub struct MemoryBackend {
    /// List 数据
    lists: Arc<RwLock<HashMap<Vec<u8>, Vec<Vec<u8>>>>>,
    /// ZSet 数据
    zsets: Arc<RwLock<HashMap<Vec<u8>, BTreeMap<f64, Vec<u8>>>>>,
}

impl StorageBackend for MemoryBackend {
    fn backend_type(&self) -> StorageBackend {
        StorageBackend::Memory
    }
    
    fn create_snapshot(&self) -> Result<SnapshotData, String> {
        // 需要完整遍历并序列化
        let lists = self.lists.read();
        let zsets = self.zsets.read();
        
        #[derive(Serialize)]
        struct MemorySnapshot {
            lists: HashMap<Vec<u8>, Vec<Vec<u8>>>,
            zsets: HashMap<Vec<u8>, BTreeMap<f64, Vec<u8>>>,
        }
        
        let snapshot = MemorySnapshot {
            lists: lists.clone(),
            zsets: zsets.clone(),
        };
        
        let snapshot_data = bincode::encode_to_vec(&snapshot, bincode::config::standard())?;
        
        let metadata = SnapshotMetadata {
            index: 0,
            size: snapshot_data.len() as u64,
            checksum: self.calculate_checksum(&snapshot_data),
            key_count: lists.len() + zsets.len(),
            slot_range: None,
        };
        
        Ok(SnapshotData {
            backend: StorageBackend::Memory,
            data: snapshot_data,
            metadata,
        })
    }
    
    fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<SnapshotData, String> {
        // 过滤指定 slot 范围的数据
        let lists = self.lists.read();
        let zsets = self.zsets.read();
        
        let filtered_lists: HashMap<_, _> = lists
            .iter()
            .filter(|(k, _)| {
                let slot = crc16::crc16(k) % 16384;
                slot >= slot_range.0 && slot < slot_range.1
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        let filtered_zsets: HashMap<_, _> = zsets
            .iter()
            .filter(|(k, _)| {
                let slot = crc16::crc16(k) % 16384;
                slot >= slot_range.0 && slot < slot_range.1
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        #[derive(Serialize)]
        struct MemorySnapshot {
            lists: HashMap<Vec<u8>, Vec<Vec<u8>>>,
            zsets: HashMap<Vec<u8>, BTreeMap<f64, Vec<u8>>>,
        }
        
        let snapshot = MemorySnapshot {
            lists: filtered_lists,
            zsets: filtered_zsets,
        };
        
        let snapshot_data = bincode::encode_to_vec(&snapshot, bincode::config::standard())?;
        
        let metadata = SnapshotMetadata {
            index: 0,
            size: snapshot_data.len() as u64,
            checksum: self.calculate_checksum(&snapshot_data),
            key_count: filtered_lists.len() + filtered_zsets.len(),
            slot_range: Some(slot_range),
        };
        
        Ok(SnapshotData {
            backend: StorageBackend::Memory,
            data: snapshot_data,
            metadata,
        })
    }
    
    fn restore_from_snapshot(&self, snapshot: &SnapshotData) -> Result<(), String> {
        let snapshot: MemorySnapshot = 
            bincode::decode_from_slice(&snapshot.data, bincode::config::standard())?;
        
        let mut lists = self.lists.write();
        let mut zsets = self.zsets.write();
        
        *lists = snapshot.lists;
        *zsets = snapshot.zsets;
        
        Ok(())
    }
}
```

### 3.3 磁盘存储（Stream）

```rust
// crates/redisstore/src/storage/stream_backend.rs

pub struct StreamBackend {
    /// 使用 RocksDB 存储 Stream 数据
    db: Arc<rocksdb::DB>,
    /// Stream 元数据（在内存中）
    stream_metadata: Arc<RwLock<HashMap<Vec<u8>, StreamMetadata>>>,
}

impl StorageBackend for StreamBackend {
    fn backend_type(&self) -> StorageBackend {
        StorageBackend::Disk
    }
    
    fn create_snapshot(&self) -> Result<SnapshotData, String> {
        // 1. 创建 RocksDB Checkpoint（Stream 数据）
        let checkpoint = self.db.checkpoint()?;
        let checkpoint_path = format!("/tmp/stream_checkpoint_{}", timestamp());
        checkpoint.create_checkpoint(&checkpoint_path)?;
        let stream_data = self.package_sst_files(&checkpoint_path)?;
        
        // 2. 序列化 Stream 元数据
        let metadata = self.stream_metadata.read();
        let metadata_data = bincode::encode_to_vec(&*metadata, bincode::config::standard())?;
        
        // 3. 组合快照
        #[derive(Serialize)]
        struct StreamSnapshot {
            stream_data: Vec<u8>,      // RocksDB Checkpoint 数据
            metadata: Vec<u8>,          // Stream 元数据
        }
        
        let snapshot = StreamSnapshot {
            stream_data,
            metadata: metadata_data,
        };
        
        let snapshot_data = bincode::encode_to_vec(&snapshot, bincode::config::standard())?;
        
        Ok(SnapshotData {
            backend: StorageBackend::Disk,
            data: snapshot_data,
            metadata: self.calculate_metadata(&snapshot_data)?,
        })
    }
    
    fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<SnapshotData, String> {
        // 过滤指定 slot 范围的 Stream
        let metadata = self.stream_metadata.read();
        let filtered_metadata: HashMap<_, _> = metadata
            .iter()
            .filter(|(k, _)| {
                let slot = crc16::crc16(k) % 16384;
                slot >= slot_range.0 && slot < slot_range.1
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        // 从 RocksDB 中提取对应的 Stream 数据
        let mut stream_entries = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        
        for item in iter {
            let (key, value) = item?;
            let slot = crc16::crc16(&key) % 16384;
            if slot >= slot_range.0 && slot < slot_range.1 {
                stream_entries.push((key, value));
            }
        }
        
        #[derive(Serialize)]
        struct StreamSnapshot {
            stream_data: Vec<(Vec<u8>, Vec<u8>)>,
            metadata: HashMap<Vec<u8>, StreamMetadata>,
        }
        
        let snapshot = StreamSnapshot {
            stream_data: stream_entries,
            metadata: filtered_metadata,
        };
        
        let snapshot_data = bincode::encode_to_vec(&snapshot, bincode::config::standard())?;
        
        Ok(SnapshotData {
            backend: StorageBackend::Disk,
            data: snapshot_data,
            metadata: self.calculate_metadata(&snapshot_data)?,
        })
    }
    
    fn restore_from_snapshot(&self, snapshot: &SnapshotData) -> Result<(), String> {
        let snapshot: StreamSnapshot = 
            bincode::decode_from_slice(&snapshot.data, bincode::config::standard())?;
        
        // 恢复 RocksDB 数据
        for (key, value) in snapshot.stream_data {
            self.db.put(key, value)?;
        }
        
        // 恢复元数据
        let mut metadata = self.stream_metadata.write();
        *metadata = snapshot.metadata;
        
        Ok(())
    }
}
```

### 3.4 TTL 管理器

```rust
// crates/redisstore/src/storage/ttl_manager.rs

pub struct TTLManager {
    /// TTL 信息（key -> expiration timestamp）
    ttl_map: Arc<RwLock<HashMap<Vec<u8>, u64>>>,
}

impl TTLManager {
    pub fn create_snapshot(&self) -> Result<SnapshotData, String> {
        let ttl_map = self.ttl_map.read();
        
        // 序列化 TTL 信息
        let snapshot_data = bincode::encode_to_vec(&*ttl_map, bincode::config::standard())?;
        
        Ok(SnapshotData {
            backend: StorageBackend::Memory,  // TTL 存储在内存中
            data: snapshot_data,
            metadata: self.calculate_metadata(&snapshot_data)?,
        })
    }
    
    pub fn create_split_snapshot(&self, slot_range: (u16, u16)) -> Result<SnapshotData, String> {
        let ttl_map = self.ttl_map.read();
        
        let filtered_ttl: HashMap<_, _> = ttl_map
            .iter()
            .filter(|(k, _)| {
                let slot = crc16::crc16(k) % 16384;
                slot >= slot_range.0 && slot < slot_range.1
            })
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        
        let snapshot_data = bincode::encode_to_vec(&filtered_ttl, bincode::config::standard())?;
        
        Ok(SnapshotData {
            backend: StorageBackend::Memory,
            data: snapshot_data,
            metadata: self.calculate_metadata(&snapshot_data)?,
        })
    }
    
    pub fn restore_from_snapshot(&self, snapshot: &SnapshotData) -> Result<(), String> {
        let ttl_map: HashMap<Vec<u8>, u64> = 
            bincode::decode_from_slice(&snapshot.data, bincode::config::standard())?;
        
        let mut current_ttl = self.ttl_map.write();
        *current_ttl = ttl_map;
        
        // 重新设置 TTL 定时器
        self.schedule_ttl_cleanup();
        
        Ok(())
    }
}
```

## 4. 统一快照接口

### 4.1 RedisStore trait 扩展

```rust
// crates/redisstore/src/traits.rs

pub trait RedisStore: Send + Sync {
    // ... 其他方法 ...
    
    /// 创建快照（协调所有存储后端）
    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        // 调用协调器创建快照
        let multi_snapshot = self.coordinator().create_snapshot()?;
        
        // 序列化为统一格式
        self.serialize_multi_snapshot(&multi_snapshot)
    }
    
    /// 创建分裂快照
    fn create_split_snapshot(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        let slot_range = (slot_start as u16, slot_end as u16);
        let multi_snapshot = self.coordinator().create_split_snapshot(slot_range)?;
        self.serialize_multi_snapshot(&multi_snapshot)
    }
    
    /// 从快照恢复
    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String> {
        let multi_snapshot = self.deserialize_multi_snapshot(snapshot)?;
        self.coordinator().restore_from_snapshot(&multi_snapshot)
    }
    
    /// 获取协调器
    fn coordinator(&self) -> &MultiStorageCoordinator;
    
    /// 序列化多存储快照
    fn serialize_multi_snapshot(&self, snapshot: &MultiStorageSnapshot) -> Result<Vec<u8>, String> {
        bincode::encode_to_vec(snapshot, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize snapshot: {}", e))
    }
    
    /// 反序列化多存储快照
    fn deserialize_multi_snapshot(&self, data: &[u8]) -> Result<MultiStorageSnapshot, String> {
        bincode::decode_from_slice(data, bincode::config::standard())
            .map(|(snapshot, _)| snapshot)
            .map_err(|e| format!("Failed to deserialize snapshot: {}", e))
    }
}
```

## 5. 一致性保证

### 5.1 快照时间戳

```rust
// 所有存储后端使用同一个时间戳
let snapshot_timestamp = std::time::SystemTime::now();

// 并行创建快照（但使用相同的时间戳）
let (rocksdb_snapshot, memory_snapshot, disk_snapshot, ttl_snapshot) = tokio::join!(
    self.rocksdb.create_snapshot_at(snapshot_timestamp),
    self.memory.create_snapshot_at(snapshot_timestamp),
    self.disk.create_snapshot_at(snapshot_timestamp),
    self.ttl_manager.create_snapshot_at(snapshot_timestamp),
);
```

### 5.2 写锁保护（可选）

```rust
// 在创建快照时，可以短暂加写锁（但会影响性能）
pub async fn create_snapshot(&self) -> Result<MultiStorageSnapshot, String> {
    // 获取写锁，确保快照一致性
    let _guard = self.write_lock.lock().await;
    
    let snapshot_timestamp = std::time::SystemTime::now();
    
    // 创建快照...
}
```

## 6. 性能优化

### 6.1 并行快照生成

```rust
// 使用 tokio::join! 并行创建各存储后端的快照
let (rocksdb_snapshot, memory_snapshot, disk_snapshot, ttl_snapshot) = tokio::join!(
    self.rocksdb.create_snapshot(),
    self.memory.create_snapshot(),
    self.disk.create_snapshot(),
    self.ttl_manager.create_snapshot(),
);
```

### 6.2 增量快照（可选）

```rust
// 对于内存存储，可以考虑增量快照
impl MemoryBackend {
    fn create_incremental_snapshot(&self, base_index: u64) -> Result<SnapshotData, String> {
        // 只记录自基础快照以来的变更
        // ...
    }
}
```

### 6.3 流式快照（可选）

```rust
// 对于大数据集，可以使用流式快照
impl StorageBackend for RocksDBBackend {
    fn create_streaming_snapshot(&self) -> impl Stream<Item = Result<Vec<u8>, String>> {
        // 分块生成快照
        // ...
    }
}
```

## 7. 实现建议

### 7.1 阶段 1: 基础架构

1. 定义 `StorageBackend` trait
2. 实现 `MultiStorageCoordinator`
3. 实现各存储后端的快照接口

### 7.2 阶段 2: 完整快照

1. 实现 RocksDB 的 Checkpoint 快照
2. 实现内存存储的序列化快照
3. 实现 Stream 的快照
4. 实现 TTL 的快照

### 7.3 阶段 3: 分裂快照

1. 实现各存储后端的分裂快照
2. 协调器协调分裂快照生成

### 7.4 阶段 4: 优化

1. 并行快照生成
2. 增量快照（可选）
3. 流式快照（可选）

## 8. 总结

- **多存储后端**：需要协调器统一管理
- **不同策略**：RocksDB 使用 Checkpoint，内存存储使用序列化
- **一致性保证**：使用相同的时间戳，并行生成快照
- **TTL 处理**：单独快照，恢复时重新设置
- **Stream 处理**：RocksDB 数据 + 元数据组合快照
