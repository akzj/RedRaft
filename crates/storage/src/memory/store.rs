//! Unified memory store implementation
//!
//! Directly manages HashMap<ShardId, ShardData>
//! Each ShardData contains all data structures (List, Set, ZSet) for that shard
//!
//! Architecture:
//! - MemoryStore -> HashMap<ShardId, ShardData>
//! - ShardData -> HashMap<Vec<u8>, RedisValue> + ShardMetadata
//! - RedisValue can be List, Set, ZSet, etc.

use super::shard::{shard_for_key, slot_for_key, ShardId};
use super::shard_data::{LockedShardData, ShardData, ShardMetadata};
use super::{RedisValue, ZSetData};
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Unified memory store
///
/// Directly manages shards, each shard contains all data structures
#[derive(Clone)]
pub struct MemoryStore {
    /// shard_id -> ShardData (contains all data structures + metadata)
    pub(crate) shards: Arc<RwLock<HashMap<ShardId, LockedShardData>>>,
    /// Total number of shards
    shard_count: u32,
}

impl MemoryStore {
    /// Create new MemoryStore with specified shard count
    pub fn new(shard_count: u32) -> Self {
        Self {
            shards: Arc::new(RwLock::new(HashMap::new())),
            shard_count: shard_count.max(1), // At least 1 shard
        }
    }

    /// Get shard count
    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Get shard for key
    pub(crate) fn get_shard_id(&self, key: &[u8]) -> ShardId {
        shard_for_key(key, self.shard_count)
    }

    /// Get or create shard data
    pub(crate) fn get_or_create_shard(&self, shard_id: ShardId) -> LockedShardData {
        let mut shards = self.shards.write();
        shards
            .entry(shard_id)
            .or_insert_with(|| Arc::new(RwLock::new(ShardData::new())))
            .clone()
    }

    /// Get shard data (read-only, returns None if shard doesn't exist)
    pub(crate) fn get_shard(&self, shard_id: ShardId) -> Option<LockedShardData> {
        let shards = self.shards.read();
        shards.get(&shard_id).cloned()
    }

    /// Get shard for key (for routing/debugging)
    pub fn get_shard_for_key(&self, key: &[u8]) -> ShardId {
        self.get_shard_id(key)
    }

    /// Get slot for key (for routing/debugging)
    pub fn get_slot_for_key(&self, key: &[u8]) -> u32 {
        slot_for_key(key)
    }

    /// Get shard data with metadata (for snapshot operations)
    pub fn get_shard_data(&self, shard_id: ShardId) -> Option<LockedShardData> {
        self.get_shard(shard_id)
    }

    /// Set apply index for shard (called during snapshot creation)
    pub fn set_shard_apply_index(&self, shard_id: ShardId, apply_index: u64) {
        if let Some(shard_data) = self.get_shard(shard_id) {
            let mut shard = shard_data.write();
            shard.metadata_mut().set_apply_index(apply_index);
        }
    }

    /// Get apply index for shard
    pub fn get_shard_apply_index(&self, shard_id: ShardId) -> Option<u64> {
        self.get_shard(shard_id).and_then(|shard_data| {
            let shard = shard_data.read();
            shard.metadata().apply_index
        })
    }

    /// Get shard metadata
    pub fn get_shard_metadata(&self, shard_id: ShardId) -> Option<ShardMetadata> {
        self.get_shard(shard_id).map(|shard_data| {
            let shard = shard_data.read();
            shard.metadata().clone()
        })
    }

    /// Get all active shard IDs
    pub fn get_active_shards(&self) -> Vec<ShardId> {
        let shards = self.shards.read();
        shards.keys().copied().collect()
    }

    /// Get keys in a specific shard (for snapshot/scan operations)
    pub fn get_shard_keys(&self, shard_id: ShardId) -> Vec<Vec<u8>> {
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            shard.data().keys().cloned().collect()
        } else {
            Vec::new()
        }
    }

    // ========== List Operations ==========

    /// LPUSH: Insert elements from left (head)
    pub fn lpush(&self, key: Vec<u8>, values: Vec<Vec<u8>>) -> usize {
        let shard_id = self.get_shard_id(&key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        let list = match shard.data_mut().get_mut(&key) {
            Some(RedisValue::List(list)) => list,
            Some(_) => {
                // Type mismatch, replace with new list
                let mut new_list = VecDeque::new();
                for value in values.into_iter().rev() {
                    new_list.push_front(value);
                }
                let len = new_list.len();
                shard.data_mut().insert(key, RedisValue::List(new_list));
                return len;
            }
            None => {
                // Create new list
                let mut new_list = VecDeque::new();
                for value in values.into_iter().rev() {
                    new_list.push_front(value);
                }
                let len = new_list.len();
                shard
                    .data_mut()
                    .insert(key.clone(), RedisValue::List(new_list));
                return len;
            }
        };

        // Insert in reverse order to maintain correct sequence
        for value in values.into_iter().rev() {
            list.push_front(value);
        }

        list.len()
    }

    /// RPUSH: Insert elements from right (tail)
    pub fn rpush(&self, key: Vec<u8>, values: Vec<Vec<u8>>) -> usize {
        let shard_id = self.get_shard_id(&key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        let list = match shard.data_mut().get_mut(&key) {
            Some(RedisValue::List(list)) => list,
            Some(_) => {
                // Type mismatch, replace with new list
                let mut new_list = VecDeque::new();
                for value in values.iter() {
                    new_list.push_back(value.clone());
                }
                let len = new_list.len();
                shard.data_mut().insert(key, RedisValue::List(new_list));
                return len;
            }
            None => {
                // Create new list
                let mut new_list = VecDeque::new();
                for value in values.iter() {
                    new_list.push_back(value.clone());
                }
                let len = new_list.len();
                shard
                    .data_mut()
                    .insert(key.clone(), RedisValue::List(new_list));
                return len;
            }
        };

        for value in values {
            list.push_back(value);
        }

        list.len()
    }

    /// LPOP: Pop element from left (head)
    pub fn lpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let mut shard = shard_data.write();
            if let Some(RedisValue::List(list)) = shard.data_mut().get_mut(key) {
                let value = list.pop_front();
                if list.is_empty() {
                    shard.data_mut().remove(key);
                }
                return value;
            }
        }
        None
    }

    /// RPOP: Pop element from right (tail)
    pub fn rpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let mut shard = shard_data.write();
            if let Some(RedisValue::List(list)) = shard.data_mut().get_mut(key) {
                let value = list.pop_back();
                if list.is_empty() {
                    shard.data_mut().remove(key);
                }
                return value;
            }
        }
        None
    }

    /// LLEN: Get list length
    pub fn llen(&self, key: &[u8]) -> usize {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::List(list)) = shard.data().get(key) {
                return list.len();
            }
        }
        0
    }

    /// LRANGE: Get elements in range [start, end]
    pub fn lrange(&self, key: &[u8], start: i64, end: i64) -> Vec<Vec<u8>> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::List(list)) = shard.data().get(key) {
                let len = list.len() as i64;
                let start_idx = if start < 0 {
                    (len + start).max(0) as usize
                } else {
                    start.min(len - 1) as usize
                };

                let end_idx = if end < 0 {
                    (len + end).max(0) as usize
                } else {
                    end.min(len - 1) as usize
                };

                if start_idx > end_idx {
                    return Vec::new();
                }

                return list
                    .iter()
                    .skip(start_idx)
                    .take(end_idx - start_idx + 1)
                    .cloned()
                    .collect();
            }
        }
        Vec::new()
    }

    // ========== Set Operations ==========

    /// SADD: Add members to set
    pub fn sadd(&self, key: Vec<u8>, members: Vec<Vec<u8>>) -> usize {
        let shard_id = self.get_shard_id(&key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        let set = match shard.data_mut().get_mut(&key) {
            Some(RedisValue::Set(set)) => set,
            Some(_) => {
                // Type mismatch, replace with new set
                let mut new_set = HashSet::new();
                let mut added = 0;
                for member in members {
                    if new_set.insert(member) {
                        added += 1;
                    }
                }
                shard.data_mut().insert(key, RedisValue::Set(new_set));
                return added;
            }
            None => {
                // Create new set
                let mut new_set = HashSet::new();
                let mut added = 0;
                for member in members.iter() {
                    if new_set.insert(member.clone()) {
                        added += 1;
                    }
                }
                shard.data_mut().insert(key, RedisValue::Set(new_set));
                return added;
            }
        };

        let mut added = 0;
        for member in members {
            if set.insert(member) {
                added += 1;
            }
        }

        added
    }

    /// SREM: Remove members from set
    pub fn srem(&self, key: &[u8], members: &[Vec<u8>]) -> usize {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let mut shard = shard_data.write();
            if let Some(RedisValue::Set(set)) = shard.data_mut().get_mut(key) {
                let mut removed = 0;
                for member in members {
                    if set.remove(member) {
                        removed += 1;
                    }
                }

                if set.is_empty() {
                    shard.data_mut().remove(key);
                }

                return removed;
            }
        }
        0
    }

    /// SISMEMBER: Check if member exists in set
    pub fn sismember(&self, key: &[u8], member: &[u8]) -> bool {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Set(set)) = shard.data().get(key) {
                return set.contains(member);
            }
        }
        false
    }

    /// SMEMBERS: Get all members
    pub fn smembers(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Set(set)) = shard.data().get(key) {
                return set.iter().cloned().collect();
            }
        }
        Vec::new()
    }

    /// SCARD: Get set cardinality (size)
    pub fn scard(&self, key: &[u8]) -> usize {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Set(set)) = shard.data().get(key) {
                return set.len();
            }
        }
        0
    }

    // ========== ZSet Operations ==========

    /// ZADD: Add members with scores
    pub fn zadd(&self, key: Vec<u8>, members: Vec<(f64, Vec<u8>)>) -> usize {
        let shard_id = self.get_shard_id(&key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        let zset = match shard.data_mut().get_mut(&key) {
            Some(RedisValue::ZSet(zset)) => zset,
            Some(_) => {
                // Type mismatch, replace with new zset
                let mut new_zset = ZSetData::new();
                let mut added = 0;
                for (score, member) in members {
                    if new_zset.dict.contains_key(&member) {
                        let old_score = new_zset.dict[&member];
                        new_zset
                            .sorted
                            .remove(&(OrderedFloat(old_score), member.clone()));
                    }
                    new_zset.dict.insert(member.clone(), score);
                    new_zset.sorted.insert((OrderedFloat(score), member), ());
                    added += 1;
                }
                shard.data_mut().insert(key, RedisValue::ZSet(new_zset));
                return added;
            }
            None => {
                // Create new zset
                let mut new_zset = ZSetData::new();
                let mut added = 0;
                for (score, member) in members.iter() {
                    new_zset.dict.insert(member.clone(), *score);
                    new_zset
                        .sorted
                        .insert((OrderedFloat(*score), member.clone()), ());
                    added += 1;
                }
                shard.data_mut().insert(key, RedisValue::ZSet(new_zset));
                return added;
            }
        };

        let mut added = 0;
        for (score, member) in members {
            // Remove old entry if exists
            if let Some(old_score) = zset.dict.remove(&member) {
                zset.sorted
                    .remove(&(OrderedFloat(old_score), member.clone()));
            }

            // Add new entry
            zset.dict.insert(member.clone(), score);
            zset.sorted.insert((OrderedFloat(score), member), ());
            added += 1;
        }

        added
    }

    /// ZREM: Remove members
    pub fn zrem(&self, key: &[u8], members: &[Vec<u8>]) -> usize {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let mut shard = shard_data.write();
            if let Some(RedisValue::ZSet(zset)) = shard.data_mut().get_mut(key) {
                let mut removed = 0;
                for member in members {
                    if let Some(score) = zset.dict.remove(member) {
                        zset.sorted.remove(&(OrderedFloat(score), member.clone()));
                        removed += 1;
                    }
                }

                if zset.is_empty() {
                    shard.data_mut().remove(key);
                }

                return removed;
            }
        }
        0
    }

    /// ZSCORE: Get score of member
    pub fn zscore(&self, key: &[u8], member: &[u8]) -> Option<f64> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::ZSet(zset)) = shard.data().get(key) {
                return zset.dict.get(member).copied();
            }
        }
        None
    }

    /// ZCARD: Get cardinality (size)
    pub fn zcard(&self, key: &[u8]) -> usize {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::ZSet(zset)) = shard.data().get(key) {
                return zset.len();
            }
        }
        0
    }

    /// ZRANGE: Get members in range [start, end] (ascending)
    pub fn zrange(
        &self,
        key: &[u8],
        start: i64,
        end: i64,
        with_scores: bool,
    ) -> Vec<(Vec<u8>, Option<f64>)> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::ZSet(zset)) = shard.data().get(key) {
                let len = zset.len() as i64;
                let start_idx = if start < 0 {
                    (len + start).max(0) as usize
                } else {
                    start.min(len - 1) as usize
                };

                let end_idx = if end < 0 {
                    (len + end).max(0) as usize
                } else {
                    end.min(len - 1) as usize
                };

                if start_idx > end_idx {
                    return Vec::new();
                }

                return zset
                    .sorted
                    .iter()
                    .skip(start_idx)
                    .take(end_idx - start_idx + 1)
                    .map(|((score, member), _)| {
                        if with_scores {
                            (member.clone(), Some(score.into_inner()))
                        } else {
                            (member.clone(), None)
                        }
                    })
                    .collect();
            }
        }
        Vec::new()
    }

    /// ZREVRANGE: Get members in range [start, end] (descending)
    pub fn zrevrange(
        &self,
        key: &[u8],
        start: i64,
        end: i64,
        with_scores: bool,
    ) -> Vec<(Vec<u8>, Option<f64>)> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::ZSet(zset)) = shard.data().get(key) {
                let len = zset.len() as i64;
                let start_idx = if start < 0 {
                    (len + start).max(0) as usize
                } else {
                    start.min(len - 1) as usize
                };

                let end_idx = if end < 0 {
                    (len + end).max(0) as usize
                } else {
                    end.min(len - 1) as usize
                };

                if start_idx > end_idx {
                    return Vec::new();
                }

                return zset
                    .sorted
                    .iter()
                    .rev()
                    .skip(start_idx)
                    .take(end_idx - start_idx + 1)
                    .map(|((score, member), _)| {
                        if with_scores {
                            (member.clone(), Some(score.into_inner()))
                        } else {
                            (member.clone(), None)
                        }
                    })
                    .collect();
            }
        }
        Vec::new()
    }

    /// ZRANK: Get rank of member (0-based, ascending)
    pub fn zrank(&self, key: &[u8], member: &[u8]) -> Option<usize> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::ZSet(zset)) = shard.data().get(key) {
                if let Some(score) = zset.dict.get(member) {
                    // Find position in sorted set
                    return Some(
                        zset.sorted
                            .range(..(OrderedFloat(*score), member.to_vec()))
                            .count(),
                    );
                }
            }
        }
        None
    }

    /// ZREVRANK: Get rank of member (0-based, descending)
    pub fn zrevrank(&self, key: &[u8], member: &[u8]) -> Option<usize> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::ZSet(zset)) = shard.data().get(key) {
                if let Some(score) = zset.dict.get(member) {
                    let rank = zset
                        .sorted
                        .range(..(OrderedFloat(*score), member.to_vec()))
                        .count();
                    return Some(zset.len() - 1 - rank);
                }
            }
        }
        None
    }

    /// ZINCRBY: Increment score of member
    pub fn zincrby(&self, key: Vec<u8>, member: Vec<u8>, delta: f64) -> f64 {
        let shard_id = self.get_shard_id(&key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        let zset = match shard.data_mut().get_mut(&key) {
            Some(RedisValue::ZSet(z)) => z,
            Some(_) => {
                // Type mismatch, create new zset
                let mut new_zset = ZSetData::new();
                new_zset.dict.insert(member.clone(), delta);
                new_zset.sorted.insert((OrderedFloat(delta), member), ());
                shard.data_mut().insert(key, RedisValue::ZSet(new_zset));
                return delta;
            }
            None => {
                // Create new zset
                let mut new_zset = ZSetData::new();
                new_zset.dict.insert(member.clone(), delta);
                new_zset.sorted.insert((OrderedFloat(delta), member), ());
                shard.data_mut().insert(key, RedisValue::ZSet(new_zset));
                return delta;
            }
        };

        let new_score = if let Some(old_score) = zset.dict.get(&member).copied() {
            // Remove old entry
            zset.sorted
                .remove(&(OrderedFloat(old_score), member.clone()));
            old_score + delta
        } else {
            delta
        };

        // Add new entry
        zset.dict.insert(member.clone(), new_score);
        zset.sorted.insert((OrderedFloat(new_score), member), ());

        new_score
    }

    /// ZRANGEBYSCORE: Get members with score in range [min, max]
    pub fn zrangebyscore(
        &self,
        key: &[u8],
        min: f64,
        max: f64,
        with_scores: bool,
    ) -> Vec<(Vec<u8>, Option<f64>)> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::ZSet(zset)) = shard.data().get(key) {
                return zset
                    .sorted
                    .range((OrderedFloat(min), vec![])..(OrderedFloat(max + f64::EPSILON), vec![]))
                    .filter(|((score, _), _)| score.into_inner() >= min && score.into_inner() <= max)
                    .map(|((score, member), _)| {
                        if with_scores {
                            (member.clone(), Some(score.into_inner()))
                        } else {
                            (member.clone(), None)
                        }
                    })
                    .collect();
            }
        }
        Vec::new()
    }

    /// ZCOUNT: Count members with score in range [min, max]
    pub fn zcount(&self, key: &[u8], min: f64, max: f64) -> usize {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::ZSet(zset)) = shard.data().get(key) {
                return zset
                    .dict
                    .values()
                    .filter(|score| **score >= min && **score <= max)
                    .count();
            }
        }
        0
    }

    // ========== Extended List Operations ==========

    /// LTRIM: Trim list to range [start, end]
    pub fn ltrim(&self, key: &[u8], start: i64, end: i64) -> bool {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let mut shard = shard_data.write();
            if let Some(RedisValue::List(list)) = shard.data_mut().get_mut(key) {
                let len = list.len() as i64;
                let start_idx = if start < 0 {
                    (len + start).max(0) as usize
                } else {
                    start.min(len) as usize
                };

                let end_idx = if end < 0 {
                    (len + end).max(-1) as usize
                } else {
                    end.min(len - 1) as usize
                };

                if start_idx > end_idx || start_idx >= list.len() {
                    // Empty result
                    shard.data_mut().remove(key);
                    return true;
                }

                // Keep only elements in range
                let new_list: VecDeque<Vec<u8>> = list
                    .drain(..)
                    .skip(start_idx)
                    .take(end_idx - start_idx + 1)
                    .collect();

                if new_list.is_empty() {
                    shard.data_mut().remove(key);
                } else {
                    *list = new_list;
                }
                return true;
            }
        }
        false
    }

    /// LREM: Remove count occurrences of element
    /// count > 0: Remove from head to tail
    /// count < 0: Remove from tail to head
    /// count = 0: Remove all occurrences
    pub fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> usize {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let mut shard = shard_data.write();
            if let Some(RedisValue::List(list)) = shard.data_mut().get_mut(key) {
                let mut removed = 0;
                let target_count = if count == 0 {
                    usize::MAX
                } else {
                    count.unsigned_abs() as usize
                };

                if count >= 0 {
                    // Remove from head to tail
                    let mut i = 0;
                    while i < list.len() && removed < target_count {
                        if list[i] == value {
                            list.remove(i);
                            removed += 1;
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    // Remove from tail to head
                    let mut i = list.len();
                    while i > 0 && removed < target_count {
                        i -= 1;
                        if list[i] == value {
                            list.remove(i);
                            removed += 1;
                        }
                    }
                }

                if list.is_empty() {
                    shard.data_mut().remove(key);
                }

                return removed;
            }
        }
        0
    }

    /// LINSERT: Insert element before/after pivot
    pub fn linsert(&self, key: &[u8], before: bool, pivot: &[u8], value: Vec<u8>) -> i64 {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let mut shard = shard_data.write();
            if let Some(RedisValue::List(list)) = shard.data_mut().get_mut(key) {
                if let Some(pos) = list.iter().position(|x| x == pivot) {
                    let insert_pos = if before { pos } else { pos + 1 };
                    list.insert(insert_pos, value);
                    return list.len() as i64;
                }
                return -1; // Pivot not found
            }
        }
        0 // Key not found
    }

    /// RPOPLPUSH: Pop from source tail, push to destination head
    pub fn rpoplpush(&self, source: &[u8], destination: Vec<u8>) -> Option<Vec<u8>> {
        // First pop from source
        let value = self.rpop(source)?;

        // Then push to destination
        self.lpush(destination, vec![value.clone()]);

        Some(value)
    }

    // ========== Extended Set Operations ==========

    /// SPOP: Remove and return random member(s)
    pub fn spop(&self, key: &[u8], count: usize) -> Vec<Vec<u8>> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let mut shard = shard_data.write();
            if let Some(RedisValue::Set(set)) = shard.data_mut().get_mut(key) {
                let mut result = Vec::with_capacity(count.min(set.len()));
                let members: Vec<_> = set.iter().take(count).cloned().collect();
                for member in members {
                    if set.remove(&member) {
                        result.push(member);
                    }
                }

                if set.is_empty() {
                    shard.data_mut().remove(key);
                }

                return result;
            }
        }
        Vec::new()
    }

    /// SRANDMEMBER: Get random member(s) without removing
    pub fn srandmember(&self, key: &[u8], count: i64) -> Vec<Vec<u8>> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Set(set)) = shard.data().get(key) {
                let abs_count = count.unsigned_abs() as usize;
                if count >= 0 {
                    // Return unique elements
                    return set.iter().take(abs_count).cloned().collect();
                } else {
                    // Allow duplicates (with replacement)
                    let members: Vec<_> = set.iter().cloned().collect();
                    if members.is_empty() {
                        return Vec::new();
                    }
                    let mut result = Vec::with_capacity(abs_count);
                    for i in 0..abs_count {
                        result.push(members[i % members.len()].clone());
                    }
                    return result;
                }
            }
        }
        Vec::new()
    }

    /// SINTER: Return intersection of multiple sets
    pub fn sinter(&self, keys: &[&[u8]]) -> Vec<Vec<u8>> {
        if keys.is_empty() {
            return Vec::new();
        }

        // Get first set
        let first_key = keys[0];
        let shard_id = self.get_shard_id(first_key);
        let first_set = if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Set(set)) = shard.data().get(first_key) {
                set.clone()
            } else {
                return Vec::new();
            }
        } else {
            return Vec::new();
        };

        // Intersect with remaining sets
        let mut result = first_set;
        for key in &keys[1..] {
            let shard_id = self.get_shard_id(key);
            if let Some(shard_data) = self.get_shard(shard_id) {
                let shard = shard_data.read();
                if let Some(RedisValue::Set(set)) = shard.data().get(*key) {
                    result = result.intersection(set).cloned().collect();
                } else {
                    return Vec::new();
                }
            } else {
                return Vec::new();
            }
        }

        result.into_iter().collect()
    }

    /// SUNION: Return union of multiple sets
    pub fn sunion(&self, keys: &[&[u8]]) -> Vec<Vec<u8>> {
        let mut result = HashSet::new();

        for key in keys {
            let shard_id = self.get_shard_id(key);
            if let Some(shard_data) = self.get_shard(shard_id) {
                let shard = shard_data.read();
                if let Some(RedisValue::Set(set)) = shard.data().get(*key) {
                    result.extend(set.iter().cloned());
                }
            }
        }

        result.into_iter().collect()
    }

    /// SDIFF: Return difference of first set with all other sets
    pub fn sdiff(&self, keys: &[&[u8]]) -> Vec<Vec<u8>> {
        if keys.is_empty() {
            return Vec::new();
        }

        // Get first set
        let first_key = keys[0];
        let shard_id = self.get_shard_id(first_key);
        let first_set = if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Set(set)) = shard.data().get(first_key) {
                set.clone()
            } else {
                return Vec::new();
            }
        } else {
            return Vec::new();
        };

        // Subtract remaining sets
        let mut result = first_set;
        for key in &keys[1..] {
            let shard_id = self.get_shard_id(key);
            if let Some(shard_data) = self.get_shard(shard_id) {
                let shard = shard_data.read();
                if let Some(RedisValue::Set(set)) = shard.data().get(*key) {
                    result = result.difference(set).cloned().collect();
                }
            }
        }

        result.into_iter().collect()
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new(16) // Default to 16 shards
    }
}
