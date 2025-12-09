# 存储层原生分片架构设计

## 核心思想突破：从"遍历筛选"到"直接定位"

### 传统应用层分片的痛点

```rust
// 灾难性的全表遍历 + 条件筛选
fn create_split_snapshot(&self, slot_range: (u32, u32)) -> Result<Vec<u8>, String> {
    let data = self.data.read();
    let entries: HashMap<...> = data
        .iter()                                    // 遍历所有key！
        .filter(|(k, e)| !e.is_expired())         // 过滤过期
        .filter(|(k, _)| {                        // 计算slot并筛选
            let slot = slot_for_key(k, total_slots);  
            slot >= slot_range.0 && slot < slot_range.1  
        })
        .collect();                                // O(N)复杂度
}
```

**问题分析**：
- ❌ **O(N)遍历复杂度**：数据量越大越慢
- ❌ **内存峰值**：需要加载所有数据到内存
- ❌ **CPU浪费**：大量key被计算slot后丢弃
- ❌ **阻塞时间长**：读写锁持有时间过久

### 原生分片存储的优势

```rust
// 直接定位目标分片，零遍历！
fn create_shard_snapshot(&self, shard_id: u32) -> Result<Vec<u8>, String> {
    let shard = self.shards.get(&shard_id)      // O(1)直接定位！
        .context("Shard not found")?;
    
    shard.create_snapshot()                     // 只处理该分片数据
}
```

**优势分析**：
- ✅ **O(1)定位复杂度**：与总数据量无关
- ✅ **零内存浪费**：只处理目标分片数据
- ✅ **CPU高效**：无需计算和筛选
- ✅ **锁粒度细**：只锁定目标分片

## RocksDB列族作为分片存储方案

### 列族分片架构设计

```rust
/// 基于RocksDB列族的原生分片存储
pub struct NativeShardRocksDB {
    db: Arc<DB>,
    
    /// 分片到列族的映射
    shard_cfs: Arc<RwLock<HashMap<u32, ColumnFamily>>>,
    
    /// 列族管理器
    cf_manager: ColumnFamilyManager,
    
    /// 分片元数据
    shard_metadata: Arc<RwLock<HashMap<u32, ShardMetadata>>>,
}

/// 分片元数据结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMetadata {
    pub shard_id: u32,
    pub slot_range: (u32, u32),           // 负责的slot范围
    pub key_count: u64,                    // key数量
    pub data_size: u64,                    // 数据大小
    pub create_time: SystemTime,          // 创建时间
    pub last_access: SystemTime,          // 最后访问时间
    pub cf_name: String,                  // 列族名称
}

/// 列族管理器
pub struct ColumnFamilyManager {
    db: Arc<DB>,
    options: ColumnFamilyOptions,
}

impl ColumnFamilyManager {
    /// 为分片创建专用列族
    fn create_shard_column_family(&self, shard_id: u32) -> Result<ColumnFamily, Error> {
        let cf_name = format!("shard_{:04x}", shard_id);
        
        let mut cf_options = ColumnFamilyOptions::default();
        
        // 针对分片特性的优化配置
        cf_options.set_compression_type(DBCompressionType::Lz4);  // 快速压缩
        cf_options.set_write_buffer_size(64 * 1024 * 1024);     // 64MB写缓冲
        cf_options.set_target_file_size_base(16 * 1024 * 1024); // 16MB文件大小
        cf_options.set_level_zero_file_num_compaction_trigger(4); // L0压缩触发
        
        self.db.create_cf(&cf_name, &cf_options)
            .map_err(|e| Error::ColumnFamilyCreationFailed(cf_name, e))
    }
    
    /// 获取分片的列族
    fn get_shard_column_family(&self, shard_id: u32) -> Result<ColumnFamily, Error> {
        let cf_name = format!("shard_{:04x}", shard_id);
        self.db.cf_handle(&cf_name)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name))
    }
}
```

### 分片感知的Key存储

```rust
impl NativeShardRocksDB {
    /// 存储key-value到指定分片
    fn put_to_shard(&self, shard_id: u32, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let cf = self.get_or_create_shard_cf(shard_id)?;
        
        // key直接存储，无需shard前缀（列族已隔离）
        self.db.put_cf(&cf, &key, &value)?;
        
        // 更新分片统计信息
        self.update_shard_stats(shard_id, 1, key.len() + value.len() as i64)?;
        
        Ok(())
    }
    
    /// 从分片获取key
    fn get_from_shard(&self, shard_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let cf = self.get_shard_column_family(shard_id)?;
        
        let value = self.db.get_cf(&cf, key)?;
        
        // 更新访问时间
        self.update_shard_access_time(shard_id)?;
        
        Ok(value)
    }
    
    /// 删除分片中的key
    fn delete_from_shard(&self, shard_id: u32, key: &[u8]) -> Result<(), Error> {
        let cf = self.get_shard_column_family(shard_id)?;
        
        // 先获取value大小用于统计更新
        let old_value = self.db.get_cf(&cf, key)?;
        let old_size = old_value.as_ref().map(|v| v.len()).unwrap_or(0);
        
        self.db.delete_cf(&cf, key)?;
        
        // 更新分片统计信息
        self.update_shard_stats(shard_id, -1, -(key.len() as i64 + old_size as i64))?;
        
        Ok(())
    }
}
```

### 分片感知的快照机制

```rust
impl NativeShardRocksDB {
    /// 创建单个分片的快照 - O(M)复杂度，M=分片数据量
    fn create_shard_snapshot(&self, shard_id: u32) -> Result<Vec<u8>, Error> {
        let cf = self.get_shard_column_family(shard_id)?;
        
        // 使用RocksDB的专用迭代器，只扫描目标列族
        let mut iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        
        let mut entries = Vec::new();
        while iter.valid() {
            if let Some((key, value)) = iter.item() {
                entries.push((key.to_vec(), value.to_vec()));
            }
            iter.next();
        }
        
        // 序列化分片数据
        let snapshot_data = bincode::serialize(&ShardSnapshotData {
            shard_id,
            entries,
            metadata: self.get_shard_metadata(shard_id)?,
        })?;
        
        Ok(snapshot_data)
    }
    
    /// 创建多个分片的并行快照
    async fn create_multi_shard_snapshot(&self, shard_ids: Vec<u32>) -> Result<HashMap<u32, Vec<u8>>, Error> {
        use futures::future::join_all;
        
        // 并行创建各分片快照
        let futures: Vec<_> = shard_ids.into_iter().map(|shard_id| {
            let self_clone = self.clone();
            tokio::task::spawn_blocking(move || {
                let snapshot = self_clone.create_shard_snapshot(shard_id)?;
                Ok((shard_id, snapshot))
            })
        }).collect();
        
        let results = join_all(futures).await;
        
        let mut snapshots = HashMap::new();
        for result in results {
            let (shard_id, snapshot) = result??;
            snapshots.insert(shard_id, snapshot);
        }
        
        Ok(snapshots)
    }
    
    /// 从快照恢复分片
    fn restore_shard_from_snapshot(&self, snapshot_data: &[u8]) -> Result<(), Error> {
        let snapshot: ShardSnapshotData = bincode::deserialize(snapshot_data)?;
        
        // 获取或创建目标分片的列族
        let cf = self.get_or_create_shard_cf(snapshot.shard_id)?;
        
        // 批量写入恢复数据
        let mut batch = WriteBatch::new();
        for (key, value) in snapshot.entries {
            batch.put_cf(&cf, &key, &value);
            
            // 分批提交，避免过大的batch
            if batch.len() > 1000 {
                self.db.write(batch)?;
                batch = WriteBatch::new();
            }
        }
        
        if batch.len() > 0 {
            self.db.write(batch)?;
        }
        
        // 恢复分片元数据
        self.restore_shard_metadata(snapshot.shard_id, snapshot.metadata)?;
        
        Ok(())
    }
}
```

## 分片查询优化

### 智能查询路由

```rust
impl NativeShardRocksDB {
    /// 根据key路由到分片
    fn route_key_to_shard(&self, key: &[u8]) -> u32 {
        // 使用CRC16算法计算slot
        let slot = crc16::crc16(key) % TOTAL_SLOTS;
        
        // slot到分片的映射（例如：每个分片负责1024个slot）
        slot / SLOTS_PER_SHARD
    }
    
    /// 通用的分片感知的get操作
    fn shard_aware_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let shard_id = self.route_key_to_shard(key);
        self.get_from_shard(shard_id, key)
    }
    
    /// 通用的分片感知的put操作
    fn shard_aware_put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let shard_id = self.route_key_to_shard(&key);
        self.put_to_shard(shard_id, key, value)
    }
    
    /// 范围查询优化（slot范围）
    fn range_query_by_slots(&self, slot_range: (u32, u32)) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let mut all_results = Vec::new();
        
        // 计算涉及的shard范围
        let start_shard = slot_range.0 / SLOTS_PER_SHARD;
        let end_shard = (slot_range.1 - 1) / SLOTS_PER_SHARD + 1;
        
        // 并行查询各分片
        for shard_id in start_shard..end_shard {
            let shard_results = self.query_shard_by_slot_range(shard_id, slot_range)?;
            all_results.extend(shard_results);
        }
        
        Ok(all_results)
    }
    
    /// 查询分片内的slot范围
    fn query_shard_by_slot_range(&self, shard_id: u32, slot_range: (u32, u32)) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let cf = self.get_shard_column_family(shard_id)?;
        
        // 构建slot前缀范围
        let start_prefix = format!("slot:{:04x}:", slot_range.0.max(shard_id * SLOTS_PER_SHARD));
        let end_prefix = format!("slot:{:04x}:", slot_range.1.min((shard_id + 1) * SLOTS_PER_SHARD));
        
        let mut results = Vec::new();
        let mut iter = self.db.iterator_cf(&cf, IteratorMode::From(start_prefix.as_bytes(), Direction::Forward));
        
        while iter.valid() {
            if let Some((key, value)) = iter.item() {
                // 检查是否超出范围
                if key.as_ref() >= end_prefix.as_bytes() {
                    break;
                }
                
                results.push((key.to_vec(), value.to_vec()));
            }
            iter.next();
        }
        
        Ok(results)
    }
}
```

## 性能对比分析

### 快照性能对比

| 指标 | 应用层分片 | 原生分片存储 | 提升倍数 |
|------|------------|-------------|----------|
| **时间复杂度** | O(N) | O(M) | N/M 倍 |
| **内存占用** | O(N) | O(M) | N/M 倍 |
| **CPU利用率** | 高（大量筛选） | 低（直接定位） | 5-10x |
| **锁粒度** | 全局锁 | 分片锁 | 分片数 x |
| **并行度** | 低 | 高 | 分片数 x |
| **I/O效率** | 全表扫描 | 范围扫描 | 10-100x |

其中：
- N = 总数据量
- M = 单个分片数据量（M ≈ N / 分片数）

### 实际场景测试

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_shard_snapshot_performance() {
        let db = create_test_db_with_1million_keys();
        let native_shard_store = NativeShardRocksDB::new(db);
        
        // 测试原生分片快照
        let start = Instant::now();
        let shard_snapshot = native_shard_store.create_shard_snapshot(0).unwrap();
        let native_time = start.elapsed();
        
        // 对比：模拟应用层分片筛选
        let start = Instant::now();
        let app_layer_snapshot = simulate_app_layer_split_snapshot(0..1024); 
        let app_time = start.elapsed();
        
        println!("原生分片快照: {:?}", native_time);
        println!("应用层分片快照: {:?}", app_time);
        println!("性能提升: {:.2}x", app_time.as_secs_f64() / native_time.as_secs_f64());
        
        // 预期：1000万分片，1000倍性能提升
        assert!(app_time > native_time * 100);
    }
}
```

## 高级优化策略

### 1. 动态分片管理

```rust
impl NativeShardRocksDB {
    /// 分裂分片（当分片过大时）
    fn split_shard(&self, shard_id: u32, split_point: u32) -> Result<(u32, u32), Error> {
        let old_metadata = self.get_shard_metadata(shard_id)?;
        
        // 创建新的分片列族
        let new_shard_id = self.allocate_new_shard_id();
        let new_cf = self.create_shard_column_family(new_shard_id)?;
        
        // 数据迁移（异步后台任务）
        self.migrate_shard_data(shard_id, new_shard_id, split_point)?;
        
        // 更新元数据
        self.update_shard_ranges(shard_id, new_shard_id, split_point)?;
        
        Ok((shard_id, new_shard_id))
    }
    
    /// 合并分片（当分片过小时）
    fn merge_shards(&self, shard1: u32, shard2: u32) -> Result<u32, Error> {
        // 选择目标分片
        let target_shard = if self.get_shard_size(shard1) > self.get_shard_size(shard2) {
            shard1
        } else {
            shard2
        };
        
        let source_shard = if target_shard == shard1 { shard2 } else { shard1 };
        
        // 数据合并
        self.merge_shard_data(source_shard, target_shard)?;
        
        // 删除源分片
        self.delete_shard(source_shard)?;
        
        Ok(target_shard)
    }
}
```

### 2. 冷热分片分离

```rust
impl NativeShardRocksDB {
    /// 识别冷热分片
    fn classify_shard_temperature(&self, shard_id: u32) -> ShardTemperature {
        let metadata = self.get_shard_metadata(shard_id)?;
        let now = SystemTime::now();
        
        let days_since_access = now.duration_since(metadata.last_access)
            .unwrap_or(Duration::MAX)
            .as_days();
        
        match days_since_access {
            0..=1 => ShardTemperature::Hot,
            2..=7 => ShardTemperature::Warm,
            8..=30 => ShardTemperature::Cold,
            _ => ShardTemperature::Frozen,
        }
    }
    
    /// 冷热分层存储策略
    fn optimize_shard_storage(&self, shard_id: u32) -> Result<(), Error> {
        let temperature = self.classify_shard_temperature(shard_id)?;
        
        match temperature {
            ShardTemperature::Hot => {
                // 热数据：使用内存表 + 快速压缩
                self.optimize_for_hot_data(shard_id)?;
            }
            ShardTemperature::Cold => {
                // 冷数据：使用高压缩比 + 低频压缩
                self.optimize_for_cold_data(shard_id)?;
            }
            ShardTemperature::Frozen => {
                // 冻结数据：考虑归档到冷存储
                self.archive_shard_data(shard_id)?;
            }
            _ => {} // Warm数据保持默认配置
        }
        
        Ok(())
    }
}
```

## 结论

存储层原生支持分片概念是**架构上的重大升级**，带来以下核心优势：

1. **性能革命**：从O(N)到O(M)的复杂度降低（M=N/分片数）
2. **扩展性提升**：分片级别的独立管理和优化
3. **运维简化**：分片级别的快照、备份、迁移
4. **资源优化**：细粒度的资源分配和调度

**RocksDB列族作为分片存储**是这一设计的完美载体，充分利用了：
- 列族的物理隔离特性
- RocksDB的优秀压缩和性能
- 成熟的分布式数据库实践

这种设计将分片概念从应用层下沉到存储层，是**分布式数据库架构的重要演进方向**！