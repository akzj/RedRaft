//! 集群元数据

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{KeyRange, NodeId, NodeInfo, RoutingTable, ShardId, ShardInfo, TOTAL_SLOTS};

/// 集群元数据
///
/// 包含集群的完整状态信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetadata {
    /// 集群名称
    pub name: String,
    /// 创建时间
    pub created_at: DateTime<Utc>,
    /// 最后更新时间
    pub updated_at: DateTime<Utc>,
    /// 节点信息
    pub nodes: HashMap<NodeId, NodeInfo>,
    /// 分片信息
    pub shards: HashMap<ShardId, ShardInfo>,
    /// 路由表
    pub routing_table: RoutingTable,
    /// 默认副本因子
    pub default_replica_factor: u32,
    /// 预创建分片数
    pub initial_shard_count: u32,
}

impl ClusterMetadata {
    /// 创建新集群
    pub fn new(name: String) -> Self {
        let now = Utc::now();
        Self {
            name,
            created_at: now,
            updated_at: now,
            nodes: HashMap::new(),
            shards: HashMap::new(),
            routing_table: RoutingTable::new(),
            default_replica_factor: 3,
            initial_shard_count: 16,
        }
    }

    /// 初始化分片（预创建）
    pub fn init_shards(&mut self) {
        let shard_count = self.initial_shard_count;
        let slots_per_shard = TOTAL_SLOTS / shard_count;

        for i in 0..shard_count {
            let shard_id = format!("shard_{:04}", i);
            let start = i * slots_per_shard;
            let end = if i == shard_count - 1 {
                TOTAL_SLOTS // 最后一个分片包含剩余槽位
            } else {
                (i + 1) * slots_per_shard
            };

            let key_range = KeyRange::new(start, end);
            let shard = ShardInfo::new(shard_id.clone(), key_range, self.default_replica_factor);
            
            self.shards.insert(shard_id.clone(), shard);
            self.routing_table.assign_slots(&shard_id, start, end);
        }

        self.touch();
    }

    /// 创建新分片
    /// 
    /// # 参数
    /// - `shard_id`: 分片 ID（如果为 None，自动生成）
    /// - `slot_start`: 起始槽位
    /// - `slot_end`: 结束槽位（不包含）
    /// - `replica_nodes`: 初始副本节点列表
    /// 
    /// # 返回
    /// - Ok(ShardInfo): 创建成功
    /// - Err(String): 创建失败原因
    pub fn create_shard(
        &mut self,
        shard_id: Option<String>,
        slot_start: u32,
        slot_end: u32,
        replica_nodes: Vec<NodeId>,
    ) -> Result<ShardInfo, String> {
        // 验证槽位范围
        if slot_start >= slot_end {
            return Err("slot_start must be less than slot_end".to_string());
        }
        if slot_end > TOTAL_SLOTS {
            return Err(format!("slot_end must not exceed {}", TOTAL_SLOTS));
        }

        // 检查槽位是否已被分配
        for slot in slot_start..slot_end {
            if self.routing_table.get_shard_for_slot(slot).is_some() {
                return Err(format!("Slot {} is already assigned", slot));
            }
        }

        // 验证节点存在
        for node_id in &replica_nodes {
            if !self.nodes.contains_key(node_id) {
                return Err(format!("Node {} does not exist", node_id));
            }
        }

        // 生成分片 ID
        let shard_id = shard_id.unwrap_or_else(|| {
            let max_id = self.shards.keys()
                .filter_map(|k| k.strip_prefix("shard_").and_then(|n| n.parse::<u32>().ok()))
                .max()
                .unwrap_or(0);
            format!("shard_{:04}", max_id + 1)
        });

        // 检查分片 ID 是否已存在
        if self.shards.contains_key(&shard_id) {
            return Err(format!("Shard {} already exists", shard_id));
        }

        // 创建分片
        let key_range = KeyRange::new(slot_start, slot_end);
        let mut shard = ShardInfo::new(shard_id.clone(), key_range, self.default_replica_factor);

        // 添加副本节点
        for node_id in &replica_nodes {
            shard.add_replica(node_id.clone());
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.add_shard(shard_id.clone());
            }
        }

        // 设置第一个节点为 leader（如果有）
        if let Some(leader) = replica_nodes.first() {
            shard.set_leader(leader.clone());
            shard.status = super::ShardStatus::Normal;
        }

        // 更新路由表
        self.routing_table.assign_slots(&shard_id, slot_start, slot_end);
        self.routing_table.set_shard_nodes(shard_id.clone(), replica_nodes);

        // 存储分片
        let result = shard.clone();
        self.shards.insert(shard_id, shard);

        self.touch();
        Ok(result)
    }

    /// 更新时间戳
    fn touch(&mut self) {
        self.updated_at = Utc::now();
        self.routing_table.bump_version();
    }

    /// 注册节点
    pub fn register_node(&mut self, node: NodeInfo) -> bool {
        let node_id = node.id.clone();
        let grpc_addr = node.grpc_addr.clone();
        
        let is_new = !self.nodes.contains_key(&node_id);
        self.nodes.insert(node_id.clone(), node);
        self.routing_table.set_node_addr(node_id, grpc_addr);
        self.touch();
        
        is_new
    }

    /// 移除节点
    pub fn remove_node(&mut self, node_id: &NodeId) -> Option<NodeInfo> {
        let node = self.nodes.remove(node_id)?;
        self.routing_table.remove_node(node_id);
        
        // 从所有分片中移除该节点
        for shard in self.shards.values_mut() {
            shard.remove_replica(node_id);
        }
        
        self.touch();
        Some(node)
    }

    /// 更新节点心跳
    pub fn node_heartbeat(&mut self, node_id: &NodeId) -> bool {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.touch();
            true
        } else {
            false
        }
    }

    /// 获取在线节点
    pub fn online_nodes(&self) -> Vec<&NodeInfo> {
        use super::NodeStatus;
        self.nodes
            .values()
            .filter(|n| n.status == NodeStatus::Online)
            .collect()
    }

    /// 分配分片到节点
    pub fn assign_shard_to_node(&mut self, shard_id: &ShardId, node_id: &NodeId) -> bool {
        // 检查节点和分片是否存在
        if !self.nodes.contains_key(node_id) || !self.shards.contains_key(shard_id) {
            return false;
        }

        // 更新分片
        if let Some(shard) = self.shards.get_mut(shard_id) {
            shard.add_replica(node_id.clone());
        }

        // 更新节点
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.add_shard(shard_id.clone());
        }

        // 更新路由表
        if let Some(shard) = self.shards.get(shard_id) {
            self.routing_table.set_shard_nodes(shard_id.clone(), shard.replicas.clone());
        }

        self.touch();
        true
    }

    /// 设置分片 leader
    pub fn set_shard_leader(&mut self, shard_id: &ShardId, leader_id: &NodeId) -> bool {
        if let Some(shard) = self.shards.get_mut(shard_id) {
            if shard.replicas.contains(leader_id) {
                shard.set_leader(leader_id.clone());
                
                // 更新路由表，将 leader 放在第一位
                let mut nodes = shard.replicas.clone();
                if let Some(pos) = nodes.iter().position(|n| n == leader_id) {
                    nodes.remove(pos);
                    nodes.insert(0, leader_id.clone());
                }
                self.routing_table.set_shard_nodes(shard_id.clone(), nodes);
                
                self.touch();
                return true;
            }
        }
        false
    }

    /// 更新分片状态
    pub fn set_shard_status(&mut self, shard_id: &ShardId, status: super::ShardStatus) -> bool {
        if let Some(shard) = self.shards.get_mut(shard_id) {
            shard.status = status;
            self.touch();
            true
        } else {
            false
        }
    }

    /// 获取需要副本的分片（副本数不足）
    pub fn shards_needing_replicas(&self) -> Vec<&ShardInfo> {
        self.shards
            .values()
            .filter(|s| !s.is_replica_satisfied())
            .collect()
    }

    /// 获取没有 leader 的分片
    pub fn shards_without_leader(&self) -> Vec<&ShardInfo> {
        self.shards
            .values()
            .filter(|s| s.leader.is_none() && !s.replicas.is_empty())
            .collect()
    }

    /// 获取集群统计信息
    pub fn stats(&self) -> ClusterStats {
        use super::{NodeStatus, ShardStatus};
        
        ClusterStats {
            total_nodes: self.nodes.len(),
            online_nodes: self.nodes.values().filter(|n| n.status == NodeStatus::Online).count(),
            total_shards: self.shards.len(),
            healthy_shards: self.shards.values().filter(|s| s.is_healthy()).count(),
            migrating_shards: self.shards.values().filter(|s| s.status == ShardStatus::Migrating).count(),
            routing_version: self.routing_table.version,
            unassigned_slots: self.routing_table.unassigned_slot_count(),
        }
    }
}

/// 集群统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStats {
    pub total_nodes: usize,
    pub online_nodes: usize,
    pub total_shards: usize,
    pub healthy_shards: usize,
    pub migrating_shards: usize,
    pub routing_version: u64,
    pub unassigned_slots: usize,
}
