//! Trait implementations for MemoryStore
//!
//! Implements all data structure traits:
//! - StringStore
//! - ListStore
//! - HashStore
//! - SetStore
//! - KeyStore
//! - SnapshotStore
//! - RedisStore (combined)

use super::store::MemoryStore;
use super::RedisValue;
use crate::traits::{
    HashStore, KeyStore, ListStore, RedisStore, SetStore, SnapshotStore, StoreError, StoreResult,
    StringStore,
};

// ============================================================================
// StringStore Implementation
// ============================================================================

impl StringStore for MemoryStore {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::String(value)) = shard.data().get(key) {
                return Some(value.clone());
            }
        }
        None
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let shard_id = self.get_shard_for_key(&key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();
        shard.data_mut().insert(key, RedisValue::String(value));
    }

    fn setnx(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
        let shard_id = self.get_shard_for_key(&key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        if shard.data().contains_key(&key) {
            false
        } else {
            shard.data_mut().insert(key, RedisValue::String(value));
            true
        }
    }

    fn setex(&self, key: Vec<u8>, value: Vec<u8>, ttl_secs: u64) {
        self.set(key.clone(), value);
        KeyStore::expire(self, &key, ttl_secs);
    }

    fn incrby(&self, key: &[u8], delta: i64) -> StoreResult<i64> {
        let shard_id = self.get_shard_for_key(key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        let current = match shard.data_mut().get_mut(key) {
            Some(RedisValue::String(s)) => {
                let s_str = String::from_utf8_lossy(s);
                s_str.parse::<i64>().unwrap_or(0)
            }
            Some(_) => return Err(StoreError::WrongType),
            None => 0,
        };

        let new_value = current
            .checked_add(delta)
            .ok_or_else(|| StoreError::InvalidArgument("integer overflow".to_string()))?;

        let new_str = new_value.to_string().into_bytes();
        shard
            .data_mut()
            .insert(key.to_vec(), RedisValue::String(new_str));

        Ok(new_value)
    }

    fn append(&self, key: &[u8], value: &[u8]) -> usize {
        let shard_id = self.get_shard_for_key(key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        match shard.data_mut().get_mut(key) {
            Some(RedisValue::String(s)) => {
                s.extend_from_slice(value);
                s.len()
            }
            Some(_) => 0, // Wrong type
            None => {
                let new_value = value.to_vec();
                let len = new_value.len();
                shard
                    .data_mut()
                    .insert(key.to_vec(), RedisValue::String(new_value));
                len
            }
        }
    }

    fn strlen(&self, key: &[u8]) -> usize {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::String(value)) = shard.data().get(key) {
                return value.len();
            }
        }
        0
    }
}

// ============================================================================
// ListStore Implementation
// ============================================================================

impl ListStore for MemoryStore {
    fn lpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> usize {
        MemoryStore::lpush(self, key.to_vec(), values)
    }

    fn rpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> usize {
        MemoryStore::rpush(self, key.to_vec(), values)
    }

    fn lpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        MemoryStore::lpop(self, key)
    }

    fn rpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        MemoryStore::rpop(self, key)
    }

    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Vec<Vec<u8>> {
        MemoryStore::lrange(self, key, start, stop)
    }

    fn llen(&self, key: &[u8]) -> usize {
        MemoryStore::llen(self, key)
    }

    fn lindex(&self, key: &[u8], index: i64) -> Option<Vec<u8>> {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::List(list)) = shard.data().get(key) {
                let len = list.len() as i64;
                let actual_index = if index < 0 { len + index } else { index };
                if actual_index >= 0 && actual_index < len {
                    return list.get(actual_index as usize).cloned();
                }
            }
        }
        None
    }

    fn lset(&self, key: &[u8], index: i64, value: Vec<u8>) -> StoreResult<()> {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let mut shard = shard_data.write();
            if let Some(RedisValue::List(list)) = shard.data_mut().get_mut(key) {
                let len = list.len() as i64;
                let actual_index = if index < 0 { len + index } else { index };
                if actual_index >= 0 && actual_index < len {
                    list[actual_index as usize] = value;
                    return Ok(());
                }
                return Err(StoreError::IndexOutOfRange);
            }
        }
        Err(StoreError::KeyNotFound)
    }
}

// ============================================================================
// HashStore Implementation
// ============================================================================

impl HashStore for MemoryStore {
    fn hget(&self, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Hash(hash)) = shard.data().get(key) {
                return hash.get(field).cloned();
            }
        }
        None
    }

    fn hset(&self, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> bool {
        let shard_id = self.get_shard_for_key(key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        let hash = match shard.data_mut().get_mut(key) {
            Some(RedisValue::Hash(h)) => h,
            Some(_) => return false, // Wrong type
            None => {
                shard
                    .data_mut()
                    .insert(key.to_vec(), RedisValue::Hash(Default::default()));
                if let Some(RedisValue::Hash(h)) = shard.data_mut().get_mut(key) {
                    h
                } else {
                    return false;
                }
            }
        };

        let is_new = !hash.contains_key(&field);
        hash.insert(field, value);
        is_new
    }

    fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> usize {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let mut shard = shard_data.write();
            if let Some(RedisValue::Hash(hash)) = shard.data_mut().get_mut(key) {
                let mut count = 0;
                for field in fields {
                    if hash.remove(*field).is_some() {
                        count += 1;
                    }
                }
                return count;
            }
        }
        0
    }

    fn hgetall(&self, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Hash(hash)) = shard.data().get(key) {
                return hash.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            }
        }
        Vec::new()
    }

    fn hkeys(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Hash(hash)) = shard.data().get(key) {
                return hash.keys().cloned().collect();
            }
        }
        Vec::new()
    }

    fn hvals(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Hash(hash)) = shard.data().get(key) {
                return hash.values().cloned().collect();
            }
        }
        Vec::new()
    }

    fn hlen(&self, key: &[u8]) -> usize {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::Hash(hash)) = shard.data().get(key) {
                return hash.len();
            }
        }
        0
    }

    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> StoreResult<i64> {
        let shard_id = self.get_shard_for_key(key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        let hash = match shard.data_mut().get_mut(key) {
            Some(RedisValue::Hash(h)) => h,
            Some(_) => return Err(StoreError::WrongType),
            None => {
                shard
                    .data_mut()
                    .insert(key.to_vec(), RedisValue::Hash(Default::default()));
                if let Some(RedisValue::Hash(h)) = shard.data_mut().get_mut(key) {
                    h
                } else {
                    return Err(StoreError::Internal("Failed to create hash".to_string()));
                }
            }
        };

        let current = match hash.get(field) {
            Some(v) => {
                let s = String::from_utf8_lossy(v);
                s.parse::<i64>().unwrap_or(0)
            }
            None => 0,
        };

        let new_value = current
            .checked_add(delta)
            .ok_or_else(|| StoreError::InvalidArgument("integer overflow".to_string()))?;

        hash.insert(field.to_vec(), new_value.to_string().into_bytes());
        Ok(new_value)
    }
}

// ============================================================================
// SetStore Implementation
// ============================================================================

impl SetStore for MemoryStore {
    fn sadd(&self, key: &[u8], members: Vec<Vec<u8>>) -> usize {
        MemoryStore::sadd(self, key.to_vec(), members)
    }

    fn srem(&self, key: &[u8], members: &[&[u8]]) -> usize {
        // Convert &[&[u8]] to Vec<Vec<u8>>
        let members_owned: Vec<Vec<u8>> = members.iter().map(|m| m.to_vec()).collect();
        MemoryStore::srem(self, key, &members_owned)
    }

    fn smembers(&self, key: &[u8]) -> Vec<Vec<u8>> {
        MemoryStore::smembers(self, key)
    }

    fn sismember(&self, key: &[u8], member: &[u8]) -> bool {
        MemoryStore::sismember(self, key, member)
    }

    fn scard(&self, key: &[u8]) -> usize {
        MemoryStore::scard(self, key)
    }
}

// ============================================================================
// KeyStore Implementation
// ============================================================================

impl KeyStore for MemoryStore {
    fn del(&self, keys: &[&[u8]]) -> usize {
        let mut count = 0;
        for key in keys {
            let shard_id = self.get_shard_for_key(key);
            if let Some(shard_data) = self.get_shard_data(shard_id) {
                let mut shard = shard_data.write();
                if shard.data_mut().remove(*key).is_some() {
                    count += 1;
                }
            }
        }
        count
    }

    fn exists(&self, keys: &[&[u8]]) -> usize {
        keys.iter()
            .filter(|key| {
                let key_slice: &[u8] = key;
                let shard_id = self.get_shard_for_key(key_slice);
                if let Some(shard_data) = self.get_shard_data(shard_id) {
                    let shard = shard_data.read();
                    return shard.data().get(key_slice).is_some();
                }
                false
            })
            .count()
    }

    fn keys(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        let pattern_str = String::from_utf8_lossy(pattern);
        let is_wildcard = pattern_str == "*";

        let mut result = Vec::new();
        for shard_id in 0..self.shard_count() {
            if let Some(shard_data) = self.get_shard_data(shard_id) {
                let shard = shard_data.read();
                for key in shard.data().keys() {
                    if is_wildcard {
                        result.push(key.clone());
                    } else {
                        let key_str = String::from_utf8_lossy(key);
                        if key_str.starts_with(pattern_str.trim_end_matches('*')) {
                            result.push(key.clone());
                        }
                    }
                }
            }
        }
        result
    }

    fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        let shard_id = self.get_shard_for_key(key);
        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let shard = shard_data.read();
            if let Some(value) = shard.data().get(key) {
                return Some(match value {
                    RedisValue::String(_) => "string",
                    RedisValue::List(_) => "list",
                    RedisValue::Set(_) => "set",
                    RedisValue::ZSet(_) => "zset",
                    RedisValue::Hash(_) => "hash",
                });
            }
        }
        None
    }

    fn ttl(&self, _key: &[u8]) -> i64 {
        // TODO: Implement TTL
        -1
    }

    fn expire(&self, _key: &[u8], _ttl_secs: u64) -> bool {
        // TODO: Implement TTL
        false
    }

    fn persist(&self, _key: &[u8]) -> bool {
        // TODO: Implement TTL
        false
    }

    fn dbsize(&self) -> usize {
        let mut count = 0;
        for shard_id in 0..self.shard_count() {
            if let Some(shard_data) = self.get_shard_data(shard_id) {
                let shard = shard_data.read();
                count += shard.data().len();
            }
        }
        count
    }

    fn flushdb(&self) {
        for shard_id in 0..self.shard_count() {
            if let Some(shard_data) = self.get_shard_data(shard_id) {
                let mut shard = shard_data.write();
                shard.data_mut().clear();
            }
        }
    }

    fn rename(&self, key: &[u8], new_key: Vec<u8>) -> StoreResult<()> {
        let old_shard_id = self.get_shard_for_key(key);
        let new_shard_id = self.get_shard_for_key(&new_key);

        // Get the value from old location
        let value = {
            let shard_data = self
                .get_shard_data(old_shard_id)
                .ok_or(StoreError::KeyNotFound)?;
            let mut shard = shard_data.write();
            shard
                .data_mut()
                .remove(key)
                .ok_or(StoreError::KeyNotFound)?
        };

        // Insert at new location
        let new_shard_data = self.get_or_create_shard(new_shard_id);
        let mut new_shard = new_shard_data.write();
        new_shard.data_mut().insert(new_key, value);

        Ok(())
    }
}

// ============================================================================
// SnapshotStore Implementation
// ============================================================================

impl SnapshotStore for MemoryStore {
    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        self.create_snapshot_impl()
    }

    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String> {
        self.restore_from_snapshot_impl(snapshot)
    }

    fn create_split_snapshot(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        self.create_split_snapshot_impl(slot_start, slot_end, total_slots)
    }

    fn merge_from_snapshot(&self, snapshot: &[u8]) -> Result<usize, String> {
        self.merge_from_snapshot_impl(snapshot)
    }

    fn delete_keys_in_slot_range(&self, slot_start: u32, slot_end: u32, total_slots: u32) -> usize {
        self.delete_keys_in_slot_range_impl(slot_start, slot_end, total_slots)
    }
}

// ============================================================================
// RedisStore Implementation (Combines All)
// ============================================================================

impl RedisStore for MemoryStore {}
