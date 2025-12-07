//! 路由表定义
//!
//! 用于将 key 哈希映射到分片，分片映射到节点

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{KeyRange, NodeId, ShardId, SplitRole, TOTAL_SLOTS};

/// 正在分裂的分片信息
///
/// 用于 Node 判断请求是否需要特殊处理
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplittingShardInfo {
    /// 源分片 ID
    pub source_shard: ShardId,
    /// 目标分片 ID
    pub target_shard: ShardId,
    /// 分裂点槽位 - 目标分片负责 [split_slot, source.end)
    pub split_slot: u32,
    /// 源分片原始范围
    pub source_range: KeyRange,
    /// 目标分片节点地址 (leader 优先)
    pub target_nodes: Vec<NodeId>,
    /// 当前分裂阶段
    pub phase: SplitPhase,
}

impl SplittingShardInfo {
    /// 检查槽位是否属于将被转移的范围
    pub fn is_slot_moving(&self, slot: u32) -> bool {
        slot >= self.split_slot && slot < self.source_range.end
    }

    /// 获取目标分片将负责的范围
    pub fn target_range(&self) -> KeyRange {
        KeyRange::new(self.split_slot, self.source_range.end)
    }

    /// 获取源分片分裂后的范围
    pub fn source_after_split_range(&self) -> KeyRange {
        KeyRange::new(self.source_range.start, self.split_slot)
    }
}

/// 分裂阶段（用于路由表传播）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitPhase {
    /// 准备/快照传输/追赶 - 源分片正常处理所有请求
    Preparing,
    /// 缓冲阶段 - 源分片缓存目标范围的写请求
    Buffering,
    /// 切换完成 - 返回 MOVED
    Switched,
}

/// 路由表
///
/// 包含从 key 到节点的完整映射信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingTable {
    /// 路由表版本号（每次变更递增）
    pub version: u64,
    /// 槽位到分片的映射 (slot_id -> shard_id)
    /// 长度固定为 TOTAL_SLOTS
    pub slots: Vec<Option<ShardId>>,
    /// 分片到节点的映射 (shard_id -> [node_ids])
    /// 第一个节点为 leader
    pub shard_nodes: HashMap<ShardId, Vec<NodeId>>,
    /// 节点地址映射 (node_id -> grpc_addr)
    pub node_addrs: HashMap<NodeId, String>,
    /// 正在分裂的分片 (source_shard_id -> info)
    #[serde(default)]
    pub splitting_shards: HashMap<ShardId, SplittingShardInfo>,
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutingTable {
    /// 创建空路由表
    pub fn new() -> Self {
        Self {
            version: 0,
            slots: vec![None; TOTAL_SLOTS as usize],
            shard_nodes: HashMap::new(),
            node_addrs: HashMap::new(),
            splitting_shards: HashMap::new(),
        }
    }

    /// 递增版本号
    pub fn bump_version(&mut self) {
        self.version += 1;
    }

    /// 计算 key 的槽位
    pub fn slot_for_key(key: &[u8]) -> u32 {
        // 使用 CRC16 算法（与 Redis Cluster 兼容）
        crc16(key) as u32 % TOTAL_SLOTS
    }

    /// 分配槽位到分片
    pub fn assign_slots(&mut self, shard_id: &ShardId, start: u32, end: u32) {
        for slot in start..end {
            if (slot as usize) < self.slots.len() {
                self.slots[slot as usize] = Some(shard_id.clone());
            }
        }
    }

    /// 设置分片的节点列表
    pub fn set_shard_nodes(&mut self, shard_id: ShardId, nodes: Vec<NodeId>) {
        self.shard_nodes.insert(shard_id, nodes);
    }

    /// 设置节点地址
    pub fn set_node_addr(&mut self, node_id: NodeId, addr: String) {
        self.node_addrs.insert(node_id, addr);
    }

    /// 移除节点
    pub fn remove_node(&mut self, node_id: &NodeId) {
        self.node_addrs.remove(node_id);
        // 从所有分片中移除该节点
        for nodes in self.shard_nodes.values_mut() {
            nodes.retain(|n| n != node_id);
        }
    }

    /// 根据 key 获取目标节点
    pub fn get_nodes_for_key(&self, key: &[u8]) -> Option<&Vec<NodeId>> {
        let slot = Self::slot_for_key(key);
        let shard_id = self.slots.get(slot as usize)?.as_ref()?;
        self.shard_nodes.get(shard_id)
    }

    /// 根据 key 获取 leader 节点
    pub fn get_leader_for_key(&self, key: &[u8]) -> Option<&NodeId> {
        self.get_nodes_for_key(key)?.first()
    }

    /// 获取 leader 节点地址
    pub fn get_leader_addr_for_key(&self, key: &[u8]) -> Option<&String> {
        let leader = self.get_leader_for_key(key)?;
        self.node_addrs.get(leader)
    }

    /// 获取所有分片 ID
    pub fn shard_ids(&self) -> Vec<&ShardId> {
        self.shard_nodes.keys().collect()
    }

    /// 检查路由表是否完整（所有槽位都已分配）
    pub fn is_complete(&self) -> bool {
        self.slots.iter().all(|s| s.is_some())
    }

    /// 统计未分配的槽位数
    pub fn unassigned_slot_count(&self) -> usize {
        self.slots.iter().filter(|s| s.is_none()).count()
    }

    /// 获取指定槽位的分片 ID
    pub fn get_shard_for_slot(&self, slot: u32) -> Option<&ShardId> {
        self.slots.get(slot as usize)?.as_ref()
    }

    // ==================== 分裂相关方法 ====================

    /// 添加正在分裂的分片信息
    pub fn add_splitting_shard(&mut self, info: SplittingShardInfo) {
        self.splitting_shards.insert(info.source_shard.clone(), info);
        self.bump_version();
    }

    /// 更新分裂阶段
    pub fn update_split_phase(&mut self, source_shard: &ShardId, phase: SplitPhase) -> bool {
        if let Some(info) = self.splitting_shards.get_mut(source_shard) {
            info.phase = phase;
            self.bump_version();
            true
        } else {
            false
        }
    }

    /// 移除分裂信息（分裂完成或取消时调用）
    pub fn remove_splitting_shard(&mut self, source_shard: &ShardId) -> Option<SplittingShardInfo> {
        let info = self.splitting_shards.remove(source_shard);
        if info.is_some() {
            self.bump_version();
        }
        info
    }

    /// 获取分裂信息
    pub fn get_splitting_shard(&self, source_shard: &ShardId) -> Option<&SplittingShardInfo> {
        self.splitting_shards.get(source_shard)
    }

    /// 根据槽位检查是否在分裂中
    /// 返回 Some((source_shard_id, info)) 如果这个槽位属于正在分裂的范围
    pub fn get_split_info_for_slot(&self, slot: u32) -> Option<(&ShardId, &SplittingShardInfo)> {
        for (shard_id, info) in &self.splitting_shards {
            if info.is_slot_moving(slot) {
                return Some((shard_id, info));
            }
        }
        None
    }

    /// 检查是否有活跃的分裂任务
    pub fn has_active_splits(&self) -> bool {
        !self.splitting_shards.is_empty()
    }

    /// 获取所有正在分裂的分片
    pub fn splitting_shard_ids(&self) -> Vec<&ShardId> {
        self.splitting_shards.keys().collect()
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
    fn test_slot_for_key() {
        // 测试槽位计算
        let slot1 = RoutingTable::slot_for_key(b"hello");
        let slot2 = RoutingTable::slot_for_key(b"hello");
        assert_eq!(slot1, slot2);

        let slot3 = RoutingTable::slot_for_key(b"world");
        // 不同 key 可能不同槽位（但不保证）
        assert!(slot1 < TOTAL_SLOTS);
        assert!(slot3 < TOTAL_SLOTS);
    }

    #[test]
    fn test_routing() {
        let mut rt = RoutingTable::new();
        
        // 分配槽位
        rt.assign_slots(&"shard1".to_string(), 0, 8192);
        rt.assign_slots(&"shard2".to_string(), 8192, 16384);
        
        // 设置分片节点
        rt.set_shard_nodes("shard1".to_string(), vec!["node1".to_string(), "node2".to_string()]);
        rt.set_shard_nodes("shard2".to_string(), vec!["node2".to_string(), "node3".to_string()]);
        
        // 设置节点地址
        rt.set_node_addr("node1".to_string(), "127.0.0.1:50051".to_string());
        rt.set_node_addr("node2".to_string(), "127.0.0.1:50052".to_string());
        rt.set_node_addr("node3".to_string(), "127.0.0.1:50053".to_string());
        
        assert!(rt.is_complete());
        assert_eq!(rt.unassigned_slot_count(), 0);
    }
}
