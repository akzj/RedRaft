//! Shard Router - 键值路由模块
//!
//! 负责将键值对路由到对应的 Raft 组（Shard）
//! 支持两种模式：
//! 1. 本地模式：使用固定 shard 数量的哈希
//! 2. Pilot 模式：使用从 Pilot 获取的路由表

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use tracing::{debug, info};

use raft::RaftId;
use crate::pilot_client::{RoutingTable, ShardSplitInfo};

/// 槽位总数（与 Pilot 一致）
pub const TOTAL_SLOTS: u32 = 16384;

/// Shard Router
/// 负责键值到 Raft 组的映射
#[derive(Clone)]
pub struct ShardRouter {
    /// Shard 数量（本地模式使用）
    shard_count: usize,
    /// 活跃的 Shard 列表
    active_shards: Arc<RwLock<HashSet<String>>>,
    /// Shard ID 到节点列表的映射
    shard_locations: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// 节点地址映射
    node_addrs: Arc<RwLock<HashMap<String, String>>>,
    /// 槽位到分片的映射（Pilot 模式使用）
    slots: Arc<RwLock<Vec<Option<String>>>>,
    /// 路由表版本
    routing_version: Arc<RwLock<u64>>,
    /// 是否使用 Pilot 路由
    use_pilot_routing: Arc<RwLock<bool>>,
    /// 正在分裂的分片 (source_shard_id -> SplitInfo)
    splitting_shards: Arc<RwLock<HashMap<String, ShardSplitInfo>>>,
}

impl ShardRouter {
    /// 创建本地模式路由器
    pub fn new(shard_count: usize) -> Self {
        let mut active_shards = HashSet::new();
        for i in 0..shard_count {
            active_shards.insert(format!("shard_{}", i));
        }

        Self {
            shard_count,
            active_shards: Arc::new(RwLock::new(active_shards)),
            shard_locations: Arc::new(RwLock::new(HashMap::new())),
            node_addrs: Arc::new(RwLock::new(HashMap::new())),
            slots: Arc::new(RwLock::new(vec![None; TOTAL_SLOTS as usize])),
            routing_version: Arc::new(RwLock::new(0)),
            use_pilot_routing: Arc::new(RwLock::new(false)),
            splitting_shards: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 计算 key 的槽位（CRC16，与 Redis Cluster 兼容）
    pub fn slot_for_key(key: &[u8]) -> u32 {
        crc16(key) as u32 % TOTAL_SLOTS
    }

    /// 根据键计算 Shard ID
    pub fn route_key(&self, key: &[u8]) -> String {
        // 如果使用 Pilot 路由，优先使用槽位映射
        if *self.use_pilot_routing.read() {
            let slot = Self::slot_for_key(key);
            let slots = self.slots.read();
            if let Some(Some(shard_id)) = slots.get(slot as usize) {
                return shard_id.clone();
            }
        }
        
        // 回退到本地哈希路由
        let mut hasher = Sha256::new();
        hasher.update(key);
        let hash = hasher.finalize();
        
        let hash_u64 = u64::from_be_bytes([
            hash[0], hash[1], hash[2], hash[3],
            hash[4], hash[5], hash[6], hash[7],
        ]);
        
        let shard_id = (hash_u64 % self.shard_count as u64) as usize;
        format!("shard_{}", shard_id)
    }

    /// 获取键对应的 Raft 组 ID
    pub fn get_raft_group_id(&self, key: &[u8], node_id: &str) -> RaftId {
        let shard_id = self.route_key(key);
        RaftId::new(shard_id, node_id.to_string())
    }

    /// 获取键对应的 leader 节点地址
    pub fn get_leader_addr_for_key(&self, key: &[u8]) -> Option<String> {
        let shard_id = self.route_key(key);
        let shard_locations = self.shard_locations.read();
        let node_addrs = self.node_addrs.read();
        
        let nodes = shard_locations.get(&shard_id)?;
        let leader = nodes.first()?;
        node_addrs.get(leader).cloned()
    }

    /// 检查当前节点是否是某个 key 的 leader
    pub fn is_leader_for_key(&self, key: &[u8], node_id: &str) -> bool {
        let shard_id = self.route_key(key);
        let shard_locations = self.shard_locations.read();
        
        if let Some(nodes) = shard_locations.get(&shard_id) {
            nodes.first().map(|s| s == node_id).unwrap_or(false)
        } else {
            // 没有配置时，默认当前节点处理
            true
        }
    }

    /// 从 Pilot 路由表更新
    pub fn update_from_pilot(&self, routing_table: &RoutingTable) {
        let new_version = routing_table.version;
        let current_version = *self.routing_version.read();
        
        if new_version <= current_version {
            return;
        }

        // 更新槽位映射
        {
            let mut slots = self.slots.write();
            *slots = routing_table.slots.clone();
        }

        // 更新分片位置
        {
            let mut shard_locations = self.shard_locations.write();
            let mut active_shards = self.active_shards.write();
            
            shard_locations.clear();
            active_shards.clear();
            
            for (shard_id, nodes) in &routing_table.shard_nodes {
                shard_locations.insert(shard_id.clone(), nodes.clone());
                active_shards.insert(shard_id.clone());
            }
        }

        // 更新节点地址
        {
            let mut node_addrs = self.node_addrs.write();
            *node_addrs = routing_table.node_addrs.clone();
        }

        // 更新分裂状态
        {
            let mut splitting = self.splitting_shards.write();
            *splitting = routing_table.splitting_shards.clone();
            
            if !splitting.is_empty() {
                info!(
                    "Router: {} shards are splitting",
                    splitting.len()
                );
            }
        }

        // 更新版本并启用 Pilot 路由
        *self.routing_version.write() = new_version;
        *self.use_pilot_routing.write() = true;

        info!(
            "Router updated from pilot: version {}, {} shards, {} nodes",
            new_version,
            routing_table.shard_nodes.len(),
            routing_table.node_addrs.len()
        );
    }

    /// 获取路由表版本
    pub fn routing_version(&self) -> u64 {
        *self.routing_version.read()
    }

    /// 是否使用 Pilot 路由
    pub fn is_pilot_routing(&self) -> bool {
        *self.use_pilot_routing.read()
    }

    /// 添加 Shard 配置（本地模式）
    pub fn add_shard(&self, shard_id: String, nodes: Vec<String>) {
        let node_count = nodes.len();
        self.active_shards.write().insert(shard_id.clone());
        self.shard_locations.write().insert(shard_id, nodes);
        debug!("Added shard with {} nodes", node_count);
    }

    /// 删除 Shard
    pub fn remove_shard(&self, shard_id: &str) {
        self.active_shards.write().remove(shard_id);
        self.shard_locations.write().remove(shard_id);
        debug!("Removed shard: {}", shard_id);
    }

    /// 获取所有活跃的 Shard
    pub fn get_active_shards(&self) -> Vec<String> {
        self.active_shards.read().iter().cloned().collect()
    }

    /// 获取 Shard 的节点列表
    pub fn get_shard_nodes(&self, shard_id: &str) -> Option<Vec<String>> {
        self.shard_locations.read().get(shard_id).cloned()
    }

    /// 更新 Shard 配置
    pub fn update_shard(&self, shard_id: String, nodes: Vec<String>) {
        self.shard_locations.write().insert(shard_id.clone(), nodes);
        debug!("Updated shard: {}", shard_id);
    }

    /// 设置节点地址
    pub fn set_node_addr(&self, node_id: String, addr: String) {
        self.node_addrs.write().insert(node_id, addr);
    }

    /// 检查 Shard 是否存在
    pub fn has_shard(&self, shard_id: &str) -> bool {
        self.active_shards.read().contains(shard_id)
    }

    /// 获取 Shard 数量
    pub fn shard_count(&self) -> usize {
        if *self.use_pilot_routing.read() {
            self.active_shards.read().len()
        } else {
            self.shard_count
        }
    }

    /// 获取所有节点地址
    pub fn get_all_node_addrs(&self) -> HashMap<String, String> {
        self.node_addrs.read().clone()
    }

    /// 检查分片是否正在分裂
    pub fn is_shard_splitting(&self, shard_id: &str) -> bool {
        self.splitting_shards.read().contains_key(shard_id)
    }

    /// 获取分片的分裂信息
    pub fn get_split_info(&self, shard_id: &str) -> Option<ShardSplitInfo> {
        self.splitting_shards.read().get(shard_id).cloned()
    }

    /// 检查 key 是否应该 MOVED 到新分片（在分裂完成后）
    /// 
    /// 返回 Some((target_shard, target_addr)) 如果应该重定向
    pub fn should_move_for_split(&self, key: &[u8], shard_id: &str) -> Option<(String, String)> {
        let splitting = self.splitting_shards.read();
        let split_info = splitting.get(shard_id)?;
        
        let slot = Self::slot_for_key(key);
        
        // 如果槽位 >= split_slot，且分裂状态是 switching 或 completed
        // 则应该转移到目标分片
        if slot >= split_info.split_slot 
            && (split_info.status == "switching" || split_info.status == "completed") 
        {
            let shard_locations = self.shard_locations.read();
            let node_addrs = self.node_addrs.read();
            
            let target_nodes = shard_locations.get(&split_info.target_shard)?;
            let target_leader = target_nodes.first()?;
            let target_addr = node_addrs.get(target_leader)?;
            
            Some((split_info.target_shard.clone(), target_addr.clone()))
        } else {
            None
        }
    }

    /// 检查 key 是否在分裂的缓冲范围内（应该缓存请求）
    pub fn should_buffer_for_split(&self, key: &[u8], shard_id: &str) -> bool {
        let splitting = self.splitting_shards.read();
        if let Some(split_info) = splitting.get(shard_id) {
            let slot = Self::slot_for_key(key);
            // 在缓冲阶段，且 key 在分裂范围内
            slot >= split_info.split_slot && split_info.status == "buffering"
        } else {
            false
        }
    }
}

/// CRC16 实现（XMODEM 变种，与 Redis Cluster 兼容）
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for byte in data {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_key() {
        let router = ShardRouter::new(3);
        
        let key1 = b"key1";
        let key2 = b"key2";
        
        let shard1 = router.route_key(key1);
        let shard2 = router.route_key(key2);
        
        // 相同键应该路由到相同 Shard
        assert_eq!(router.route_key(key1), shard1);
        
        // 不同键可能路由到不同 Shard（取决于哈希）
        println!("key1 -> {}, key2 -> {}", shard1, shard2);
    }

    #[test]
    fn test_shard_management() {
        let router = ShardRouter::new(3);
        
        router.add_shard("shard_0".to_string(), vec!["node1".to_string(), "node2".to_string()]);
        assert!(router.has_shard("shard_0"));
        
        let nodes = router.get_shard_nodes("shard_0");
        assert_eq!(nodes, Some(vec!["node1".to_string(), "node2".to_string()]));
        
        router.remove_shard("shard_0");
        assert!(!router.has_shard("shard_0"));
    }

    #[test]
    fn test_slot_calculation() {
        // 测试槽位计算一致性
        let slot1 = ShardRouter::slot_for_key(b"hello");
        let slot2 = ShardRouter::slot_for_key(b"hello");
        assert_eq!(slot1, slot2);
        
        // 确保在范围内
        assert!(slot1 < TOTAL_SLOTS);
    }

    #[test]
    fn test_pilot_routing() {
        let router = ShardRouter::new(3);
        
        // 创建模拟路由表
        let mut routing_table = RoutingTable::default();
        routing_table.version = 1;
        
        // 分配槽位
        for i in 0..8192 {
            routing_table.slots[i] = Some("shard_0000".to_string());
        }
        for i in 8192..16384 {
            routing_table.slots[i] = Some("shard_0001".to_string());
        }
        
        routing_table.shard_nodes.insert(
            "shard_0000".to_string(),
            vec!["node1".to_string(), "node2".to_string()],
        );
        routing_table.shard_nodes.insert(
            "shard_0001".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
        );
        
        routing_table.node_addrs.insert("node1".to_string(), "127.0.0.1:6379".to_string());
        routing_table.node_addrs.insert("node2".to_string(), "127.0.0.1:6380".to_string());
        
        // 更新路由
        router.update_from_pilot(&routing_table);
        
        assert!(router.is_pilot_routing());
        assert_eq!(router.routing_version(), 1);
        assert_eq!(router.shard_count(), 2);
    }
}
