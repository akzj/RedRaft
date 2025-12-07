//! Shard Router - 键值路由模块
//!
//! 负责将键值对路由到对应的 Raft 组（Shard）

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use tracing::debug;

use raft::RaftId;

/// Shard Router
/// 负责键值到 Raft 组的映射
#[derive(Clone)]
pub struct ShardRouter {
    /// Shard 数量
    shard_count: usize,
    /// 活跃的 Shard 列表
    active_shards: Arc<RwLock<HashSet<String>>>,
    /// Shard ID 到节点列表的映射
    shard_locations: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl ShardRouter {
    pub fn new(shard_count: usize) -> Self {
        let mut active_shards = HashSet::new();
        for i in 0..shard_count {
            active_shards.insert(format!("shard_{}", i));
        }

        Self {
            shard_count,
            active_shards: Arc::new(RwLock::new(active_shards)),
            shard_locations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 根据键计算 Shard ID
    pub fn route_key(&self, key: &[u8]) -> String {
        // 使用 SHA256 哈希
        let mut hasher = Sha256::new();
        hasher.update(key);
        let hash = hasher.finalize();
        
        // 使用前 8 字节作为哈希值
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

    /// 添加 Shard 配置
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

    /// 检查 Shard 是否存在
    pub fn has_shard(&self, shard_id: &str) -> bool {
        self.active_shards.read().contains(shard_id)
    }

    /// 获取 Shard 数量
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }
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
}
