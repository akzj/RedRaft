//! Shard placement strategy

use crate::metadata::{NodeInfo, ShardInfo, NodeId};

/// Placement strategy
#[derive(Debug, Clone)]
pub struct PlacementStrategy {
    /// Whether to consider rack awareness
    pub rack_aware: bool,
    /// Whether to consider load balancing
    pub load_balance: bool,
}

impl Default for PlacementStrategy {
    fn default() -> Self {
        Self {
            rack_aware: false,
            load_balance: true,
        }
    }
}

impl PlacementStrategy {
    /// Select nodes for shard
    ///
    /// Returns up to `count` nodes suitable for hosting this shard
    pub fn select_nodes(
        &self,
        shard: &ShardInfo,
        available_nodes: &[NodeInfo],
        count: usize,
    ) -> Vec<NodeId> {
        if count == 0 || available_nodes.is_empty() {
            return Vec::new();
        }

        // Filter out nodes that already host this shard
        let mut candidates: Vec<_> = available_nodes
            .iter()
            .filter(|n| !shard.replicas.contains(&n.id))
            .filter(|n| n.hosted_shards.len() < n.capacity as usize)
            .cloned()
            .collect();

        if candidates.is_empty() {
            return Vec::new();
        }

        // Sort by load (lower load first)
        if self.load_balance {
            candidates.sort_by(|a, b| {
                let load_a = a.load();
                let load_b = b.load();
                load_a.partial_cmp(&load_b).unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        // If rack awareness enabled, try to distribute across different racks
        if self.rack_aware {
            let mut selected = Vec::new();
            let mut used_racks = std::collections::HashSet::new();

            // First select nodes from different racks
            for node in &candidates {
                if selected.len() >= count {
                    break;
                }
                let rack = node.labels.get("rack").cloned().unwrap_or_default();
                if !used_racks.contains(&rack) {
                    selected.push(node.id.clone());
                    used_racks.insert(rack);
                }
            }

            // If not enough, select from remaining nodes
            for node in &candidates {
                if selected.len() >= count {
                    break;
                }
                if !selected.contains(&node.id) {
                    selected.push(node.id.clone());
                }
            }

            selected
        } else {
            // Simply select first N
            candidates
                .into_iter()
                .take(count)
                .map(|n| n.id)
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str, shard_count: usize) -> NodeInfo {
        let mut node = NodeInfo::new(
            id.to_string(),
            format!("{}:50051", id),
            format!("{}:6379", id),
        );
        node.capacity = 10;
        for i in 0..shard_count {
            node.hosted_shards.insert(format!("existing_shard_{}", i));
        }
        node
    }

    #[test]
    fn test_select_nodes_by_load() {
        use crate::metadata::KeyRange;
        
        let strategy = PlacementStrategy::default();
        
        let nodes = vec![
            make_node("node1", 5),  // 50% load
            make_node("node2", 2),  // 20% load
            make_node("node3", 8),  // 80% load
        ];

        let shard = ShardInfo::new(
            "test_shard".to_string(),
            KeyRange::new(0, 100),
            3,
        );

        let selected = strategy.select_nodes(&shard, &nodes, 2);
        
        assert_eq!(selected.len(), 2);
        // Should select node2 and node1 with lower load
        assert_eq!(selected[0], "node2");
        assert_eq!(selected[1], "node1");
    }

    #[test]
    fn test_exclude_existing_replicas() {
        use crate::metadata::KeyRange;
        
        let strategy = PlacementStrategy::default();
        
        let nodes = vec![
            make_node("node1", 2),
            make_node("node2", 2),
            make_node("node3", 2),
        ];

        let mut shard = ShardInfo::new(
            "test_shard".to_string(),
            KeyRange::new(0, 100),
            3,
        );
        shard.replicas = vec!["node1".to_string()];

        let selected = strategy.select_nodes(&shard, &nodes, 2);
        
        assert_eq!(selected.len(), 2);
        assert!(!selected.contains(&"node1".to_string()));
    }
}
