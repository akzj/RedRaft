//! In-memory storage implementation
//! 
//! HashMap-based in-memory storage that supports multiple Redis data types
//! Can be replaced with persistent storage like RocksDB later

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

use crate::traits::{RedisStore, StoreError, StoreResult};

/// Redis value type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedisValue {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
}

impl RedisValue {
    fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Hash(_) => "hash",
            RedisValue::Set(_) => "set",
        }
    }
}

/// Value with expiration time
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    value: RedisValue,
    #[serde(skip)]
    expire_at: Option<Instant>,
    /// TTL for serialization (milliseconds)
    ttl_ms: Option<u64>,
}

impl Entry {
    fn new(value: RedisValue) -> Self {
        Self {
            value,
            expire_at: None,
            ttl_ms: None,
        }
    }

    fn with_ttl(value: RedisValue, ttl: Duration) -> Self {
        Self {
            value,
            expire_at: Some(Instant::now() + ttl),
            ttl_ms: Some(ttl.as_millis() as u64),
        }
    }

    fn is_expired(&self) -> bool {
        self.expire_at.map_or(false, |t| Instant::now() >= t)
    }

    fn ttl_secs(&self) -> i64 {
        match self.expire_at {
            Some(t) => {
                let now = Instant::now();
                if now >= t {
                    -2 // Expired
                } else {
                    (t - now).as_secs() as i64
                }
            }
            None => -1, // Never expires
        }
    }
}

/// In-memory storage implementation
#[derive(Clone)]
pub struct MemoryStore {
    data: Arc<RwLock<HashMap<Vec<u8>, Entry>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Clean up expired keys
    fn cleanup_expired(&self, data: &mut HashMap<Vec<u8>, Entry>) {
        data.retain(|_, entry| !entry.is_expired());
    }

    /// Get value, automatically skip expired ones
    fn get_entry(&self, data: &HashMap<Vec<u8>, Entry>, key: &[u8]) -> Option<Entry> {
        data.get(key).filter(|e| !e.is_expired()).cloned()
    }

    /// Get mutable value
    fn get_entry_mut<'a>(
        &self,
        data: &'a mut HashMap<Vec<u8>, Entry>,
        key: &[u8],
    ) -> Option<&'a mut Entry> {
        // Check if expired first
        if let Some(entry) = data.get(key) {
            if entry.is_expired() {
                data.remove(key);
                return None;
            }
        }
        data.get_mut(key)
    }

    /// Parse integer
    fn parse_int(value: &[u8]) -> StoreResult<i64> {
        std::str::from_utf8(value)
            .map_err(|_| StoreError::InvalidArgument("value is not a valid string".to_string()))?
            .parse::<i64>()
            .map_err(|_| StoreError::InvalidArgument("value is not an integer".to_string()))
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisStore for MemoryStore {
    // ==================== String Operations ====================

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let data = self.data.read();
        match self.get_entry(&data, key) {
            Some(Entry {
                value: RedisValue::String(v),
                ..
            }) => Some(v),
            _ => None, // Return None if type mismatch
        }
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut data = self.data.write();
        data.insert(key, Entry::new(RedisValue::String(value)));
    }

    fn setnx(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
        let data = self.data.read();
        if self.get_entry(&data, &key).is_some() {
            return false;
        }
        drop(data); // Release read lock before acquiring write lock
        let mut data = self.data.write();
        data.insert(key, Entry::new(RedisValue::String(value)));
        true
    }

    fn setex(&self, key: Vec<u8>, value: Vec<u8>, ttl_secs: u64) {
        let mut data = self.data.write();
        data.insert(
            key,
            Entry::with_ttl(RedisValue::String(value), Duration::from_secs(ttl_secs)),
        );
    }

    fn mget(&self, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        let data = self.data.read();
        keys.iter()
            .map(|key| match self.get_entry(&data, key) {
                Some(Entry {
                    value: RedisValue::String(v),
                    ..
                }) => Some(v),
                _ => None,
            })
            .collect()
    }

    fn mset(&self, kvs: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut data = self.data.write();
        for (k, v) in kvs {
            data.insert(k, Entry::new(RedisValue::String(v)));
        }
    }

    fn incr(&self, key: &[u8]) -> StoreResult<i64> {
        self.incrby(key, 1)
    }

    fn incrby(&self, key: &[u8], delta: i64) -> StoreResult<i64> {
        let mut data = self.data.write();

        let current = match self.get_entry_mut(&mut data, key) {
            Some(Entry {
                value: RedisValue::String(v),
                ..
            }) => Self::parse_int(v)?,
            Some(_) => return Err(StoreError::WrongType),
            None => 0,
        };

        let new_value = current + delta;
        data.insert(
            key.to_vec(),
            Entry::new(RedisValue::String(new_value.to_string().into_bytes())),
        );
        Ok(new_value)
    }

    fn decr(&self, key: &[u8]) -> StoreResult<i64> {
        self.incrby(key, -1)
    }

    fn decrby(&self, key: &[u8], delta: i64) -> StoreResult<i64> {
        self.incrby(key, -delta)
    }

    fn append(&self, key: &[u8], value: &[u8]) -> usize {
        let mut data = self.data.write();

        match self.get_entry_mut(&mut data, key) {
            Some(Entry {
                value: RedisValue::String(v),
                ..
            }) => {
                v.extend_from_slice(value);
                v.len()
            }
            Some(_) => 0, // Type mismatch
            None => {
                let v = value.to_vec();
                let len = v.len();
                data.insert(key.to_vec(), Entry::new(RedisValue::String(v)));
                len
            }
        }
    }

    fn strlen(&self, key: &[u8]) -> usize {
        let data = self.data.read();
        match self.get_entry(&data, key) {
            Some(Entry {
                value: RedisValue::String(v),
                ..
            }) => v.len(),
            _ => 0,
        }
    }

    fn getset(&self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        let data = self.data.read();
        let old = match self.get_entry(&data, &key) {
            Some(Entry {
                value: RedisValue::String(v),
                ..
            }) => Some(v),
            _ => None,
        };
        drop(data); // Release read lock before acquiring write lock
        let mut data = self.data.write();
        data.insert(key, Entry::new(RedisValue::String(value)));
        old
    }

    // ==================== List Operations ====================

    fn lpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> usize {
        let mut data = self.data.write();

        let entry = data
            .entry(key.to_vec())
            .or_insert_with(|| Entry::new(RedisValue::List(VecDeque::new())));

        if let RedisValue::List(list) = &mut entry.value {
            for v in values.into_iter().rev() {
                list.push_front(v);
            }
            list.len()
        } else {
            0 // Type mismatch
        }
    }

    fn rpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> usize {
        let mut data = self.data.write();

        let entry = data
            .entry(key.to_vec())
            .or_insert_with(|| Entry::new(RedisValue::List(VecDeque::new())));

        if let RedisValue::List(list) = &mut entry.value {
            for v in values {
                list.push_back(v);
            }
            list.len()
        } else {
            0
        }
    }

    fn lpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut data = self.data.write();

        if let Some(Entry {
            value: RedisValue::List(list),
            ..
        }) = self.get_entry_mut(&mut data, key)
        {
            list.pop_front()
        } else {
            None
        }
    }

    fn rpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut data = self.data.write();

        if let Some(Entry {
            value: RedisValue::List(list),
            ..
        }) = self.get_entry_mut(&mut data, key)
        {
            list.pop_back()
        } else {
            None
        }
    }

    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Vec<Vec<u8>> {
        let data = self.data.read();

        if let Some(Entry {
            value: RedisValue::List(list),
            ..
        }) = self.get_entry(&data, key)
        {
            let len = list.len() as i64;
            let start = if start < 0 {
                (len + start).max(0) as usize
            } else {
                start.min(len) as usize
            };
            let stop = if stop < 0 {
                (len + stop + 1).max(0) as usize
            } else {
                (stop + 1).min(len) as usize
            };

            if start >= stop {
                return vec![];
            }

            list.iter()
                .skip(start)
                .take(stop - start)
                .cloned()
                .collect()
        } else {
            vec![]
        }
    }

    fn llen(&self, key: &[u8]) -> usize {
        let data = self.data.read();
        match self.get_entry(&data, key) {
            Some(Entry {
                value: RedisValue::List(list),
                ..
            }) => list.len(),
            _ => 0,
        }
    }

    fn lindex(&self, key: &[u8], index: i64) -> Option<Vec<u8>> {
        let data = self.data.read();

        if let Some(Entry {
            value: RedisValue::List(list),
            ..
        }) = self.get_entry(&data, key)
        {
            let len = list.len() as i64;
            let idx = if index < 0 { len + index } else { index };
            if idx >= 0 && idx < len {
                list.get(idx as usize).cloned()
            } else {
                None
            }
        } else {
            None
        }
    }

    fn lset(&self, key: &[u8], index: i64, value: Vec<u8>) -> StoreResult<()> {
        let mut data = self.data.write();

        if let Some(Entry {
            value: RedisValue::List(list),
            ..
        }) = self.get_entry_mut(&mut data, key)
        {
            let len = list.len() as i64;
            let idx = if index < 0 { len + index } else { index };
            if idx >= 0 && idx < len {
                list[idx as usize] = value;
                Ok(())
            } else {
                Err(StoreError::IndexOutOfRange)
            }
        } else {
            Err(StoreError::KeyNotFound)
        }
    }

    // ==================== Hash Operations ====================

    fn hget(&self, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
        let data = self.data.read();
        if let Some(Entry {
            value: RedisValue::Hash(hash),
            ..
        }) = self.get_entry(&data, key)
        {
            hash.get(field).cloned()
        } else {
            None
        }
    }

    fn hset(&self, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> bool {
        let mut data = self.data.write();

        let entry = data
            .entry(key.to_vec())
            .or_insert_with(|| Entry::new(RedisValue::Hash(HashMap::new())));

        if let RedisValue::Hash(hash) = &mut entry.value {
            let is_new = !hash.contains_key(&field);
            hash.insert(field, value);
            is_new
        } else {
            false
        }
    }

    fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        let data = self.data.read();
        if let Some(Entry {
            value: RedisValue::Hash(hash),
            ..
        }) = self.get_entry(&data, key)
        {
            fields.iter().map(|f| hash.get(*f).cloned()).collect()
        } else {
            vec![None; fields.len()]
        }
    }

    fn hmset(&self, key: &[u8], fvs: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut data = self.data.write();

        let entry = data
            .entry(key.to_vec())
            .or_insert_with(|| Entry::new(RedisValue::Hash(HashMap::new())));

        if let RedisValue::Hash(hash) = &mut entry.value {
            for (f, v) in fvs {
                hash.insert(f, v);
            }
        }
    }

    fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> usize {
        let mut data = self.data.write();

        if let Some(Entry {
            value: RedisValue::Hash(hash),
            ..
        }) = self.get_entry_mut(&mut data, key)
        {
            fields
                .iter()
                .filter(|f| hash.remove(&f.to_vec()).is_some())
                .count()
        } else {
            0
        }
    }

    fn hexists(&self, key: &[u8], field: &[u8]) -> bool {
        let data = self.data.read();
        if let Some(Entry {
            value: RedisValue::Hash(hash),
            ..
        }) = self.get_entry(&data, key)
        {
            hash.contains_key(field)
        } else {
            false
        }
    }

    fn hgetall(&self, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let data = self.data.read();
        if let Some(Entry {
            value: RedisValue::Hash(hash),
            ..
        }) = self.get_entry(&data, key)
        {
            hash.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        } else {
            vec![]
        }
    }

    fn hkeys(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let data = self.data.read();
        if let Some(Entry {
            value: RedisValue::Hash(hash),
            ..
        }) = self.get_entry(&data, key)
        {
            hash.keys().cloned().collect()
        } else {
            vec![]
        }
    }

    fn hvals(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let data = self.data.read();
        if let Some(Entry {
            value: RedisValue::Hash(hash),
            ..
        }) = self.get_entry(&data, key)
        {
            hash.values().cloned().collect()
        } else {
            vec![]
        }
    }

    fn hlen(&self, key: &[u8]) -> usize {
        let data = self.data.read();
        match self.get_entry(&data, key) {
            Some(Entry {
                value: RedisValue::Hash(hash),
                ..
            }) => hash.len(),
            _ => 0,
        }
    }

    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> StoreResult<i64> {
        let mut data = self.data.write();

        let entry = data
            .entry(key.to_vec())
            .or_insert_with(|| Entry::new(RedisValue::Hash(HashMap::new())));

        if let RedisValue::Hash(hash) = &mut entry.value {
            let current = match hash.get(field) {
                Some(v) => Self::parse_int(v)?,
                None => 0,
            };
            let new_value = current + delta;
            hash.insert(field.to_vec(), new_value.to_string().into_bytes());
            Ok(new_value)
        } else {
            Err(StoreError::WrongType)
        }
    }

    // ==================== Set Operations ====================

    fn sadd(&self, key: &[u8], members: Vec<Vec<u8>>) -> usize {
        let mut data = self.data.write();

        let entry = data
            .entry(key.to_vec())
            .or_insert_with(|| Entry::new(RedisValue::Set(HashSet::new())));

        if let RedisValue::Set(set) = &mut entry.value {
            let before = set.len();
            for m in members {
                set.insert(m);
            }
            set.len() - before
        } else {
            0
        }
    }

    fn srem(&self, key: &[u8], members: &[&[u8]]) -> usize {
        let mut data = self.data.write();

        if let Some(Entry {
            value: RedisValue::Set(set),
            ..
        }) = self.get_entry_mut(&mut data, key)
        {
            members
                .iter()
                .filter(|m| set.remove(m.to_vec().as_slice()))
                .count()
        } else {
            0
        }
    }

    fn smembers(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let data = self.data.read();
        if let Some(Entry {
            value: RedisValue::Set(set),
            ..
        }) = self.get_entry(&data, key)
        {
            set.iter().cloned().collect()
        } else {
            vec![]
        }
    }

    fn sismember(&self, key: &[u8], member: &[u8]) -> bool {
        let data = self.data.read();
        if let Some(Entry {
            value: RedisValue::Set(set),
            ..
        }) = self.get_entry(&data, key)
        {
            set.contains(member)
        } else {
            false
        }
    }

    fn scard(&self, key: &[u8]) -> usize {
        let data = self.data.read();
        match self.get_entry(&data, key) {
            Some(Entry {
                value: RedisValue::Set(set),
                ..
            }) => set.len(),
            _ => 0,
        }
    }

    // ==================== Generic Operations ====================

    fn del(&self, keys: &[&[u8]]) -> usize {
        let mut data = self.data.write();
        keys.iter()
            .filter(|k| data.remove(&k.to_vec()).is_some())
            .count()
    }

    fn exists(&self, keys: &[&[u8]]) -> usize {
        let data = self.data.read();
        keys.iter()
            .filter(|k| self.get_entry(&data, k).is_some())
            .count()
    }

    fn keys(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        let data = self.data.read();
        let pattern_str = String::from_utf8_lossy(pattern);

        data.iter()
            .filter(|(_, e)| !e.is_expired())
            .filter(|(k, _)| {
                let key_str = String::from_utf8_lossy(k);
                if pattern_str == "*" {
                    true
                } else if pattern_str.starts_with('*') && pattern_str.ends_with('*') {
                    let middle = &pattern_str[1..pattern_str.len() - 1];
                    key_str.contains(middle)
                } else if pattern_str.starts_with('*') {
                    key_str.ends_with(&pattern_str[1..])
                } else if pattern_str.ends_with('*') {
                    key_str.starts_with(&pattern_str[..pattern_str.len() - 1])
                } else {
                    key_str == pattern_str
                }
            })
            .map(|(k, _)| k.clone())
            .collect()
    }

    fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        let data = self.data.read();
        self.get_entry(&data, key).map(|e| e.value.type_name())
    }

    fn ttl(&self, key: &[u8]) -> i64 {
        let data = self.data.read();
        match data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    -2
                } else {
                    entry.ttl_secs()
                }
            }
            None => -2,
        }
    }

    fn expire(&self, key: &[u8], ttl_secs: u64) -> bool {
        let mut data = self.data.write();

        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                data.remove(key);
                return false;
            }
            entry.expire_at = Some(Instant::now() + Duration::from_secs(ttl_secs));
            entry.ttl_ms = Some(ttl_secs * 1000);
            true
        } else {
            false
        }
    }

    fn persist(&self, key: &[u8]) -> bool {
        let mut data = self.data.write();

        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                data.remove(key);
                return false;
            }
            let had_ttl = entry.expire_at.is_some();
            entry.expire_at = None;
            entry.ttl_ms = None;
            had_ttl
        } else {
            false
        }
    }

    fn dbsize(&self) -> usize {
        let data = self.data.read();
        data.iter().filter(|(_, e)| !e.is_expired()).count()
    }

    fn flushdb(&self) {
        self.data.write().clear();
    }

    fn rename(&self, key: &[u8], new_key: Vec<u8>) -> StoreResult<()> {
        let mut data = self.data.write();

        if let Some(entry) = data.remove(key) {
            if entry.is_expired() {
                return Err(StoreError::KeyNotFound);
            }
            data.insert(new_key, entry);
            Ok(())
        } else {
            Err(StoreError::KeyNotFound)
        }
    }

    // ==================== Snapshot Operations ====================

    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String> {
        #[derive(Deserialize)]
        struct SnapshotData {
            entries: HashMap<Vec<u8>, (RedisValue, Option<u64>)>,
        }

        let (snap, _): (SnapshotData, _) =
            bincode::serde::decode_from_slice(snapshot, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

        let mut data = self.data.write();
        data.clear();

        for (key, (value, ttl_ms)) in snap.entries {
            let entry = if let Some(ms) = ttl_ms {
                Entry::with_ttl(value, Duration::from_millis(ms))
            } else {
                Entry::new(value)
            };
            data.insert(key, entry);
        }

        info!("Restored memory store from snapshot, {} keys", data.len());
        Ok(())
    }

    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        #[derive(Serialize)]
        struct SnapshotData {
            entries: HashMap<Vec<u8>, (RedisValue, Option<u64>)>,
        }

        let data = self.data.read();
        let entries: HashMap<Vec<u8>, (RedisValue, Option<u64>)> = data
            .iter()
            .filter(|(_, e)| !e.is_expired())
            .map(|(k, e)| (k.clone(), (e.value.clone(), e.ttl_ms)))
            .collect();

        let snap = SnapshotData { entries };
        bincode::serde::encode_to_vec(&snap, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize snapshot: {}", e))
    }

    fn create_split_snapshot(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        #[derive(Serialize)]
        struct SnapshotData {
            entries: HashMap<Vec<u8>, (RedisValue, Option<u64>)>,
        }

        let data = self.data.read();
        let entries: HashMap<Vec<u8>, (RedisValue, Option<u64>)> = data
            .iter()
            .filter(|(_, e)| !e.is_expired())
            .filter(|(k, _)| {
                let slot = slot_for_key(k, total_slots);
                slot >= slot_start && slot < slot_end
            })
            .map(|(k, e)| (k.clone(), (e.value.clone(), e.ttl_ms)))
            .collect();

        info!(
            "Created split snapshot: slots [{}, {}), {} keys",
            slot_start,
            slot_end,
            entries.len()
        );

        let snap = SnapshotData { entries };
        bincode::serde::encode_to_vec(&snap, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize split snapshot: {}", e))
    }

    fn merge_from_snapshot(&self, snapshot: &[u8]) -> Result<usize, String> {
        #[derive(Deserialize)]
        struct SnapshotData {
            entries: HashMap<Vec<u8>, (RedisValue, Option<u64>)>,
        }

        let (snap, _): (SnapshotData, _) =
            bincode::serde::decode_from_slice(snapshot, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

        let mut data = self.data.write();
        let count = snap.entries.len();

        for (key, (value, ttl_ms)) in snap.entries {
            let entry = if let Some(ms) = ttl_ms {
                Entry::with_ttl(value, Duration::from_millis(ms))
            } else {
                Entry::new(value)
            };
            data.insert(key, entry);
        }

        info!("Merged {} keys from snapshot", count);
        Ok(count)
    }

    fn delete_keys_in_slot_range(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> usize {
        let mut data = self.data.write();
        let before = data.len();

        data.retain(|k, _| {
            let slot = slot_for_key(k, total_slots);
            slot < slot_start || slot >= slot_end
        });

        let deleted = before - data.len();
        info!(
            "Deleted {} keys in slot range [{}, {})",
            deleted, slot_start, slot_end
        );
        deleted
    }
}

/// Calculate key slot (CRC16 XMODEM)
fn slot_for_key(key: &[u8], total_slots: u32) -> u32 {
    crc16(key) as u32 % total_slots
}

/// CRC16 implementation (XMODEM variant, compatible with Redis Cluster)
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
    fn test_string_operations() {
        let store = MemoryStore::new();

        store.set(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(store.get(b"key1"), Some(b"value1".to_vec()));

        assert!(store.setnx(b"key2".to_vec(), b"value2".to_vec()));
        assert!(!store.setnx(b"key2".to_vec(), b"value3".to_vec()));
        assert_eq!(store.get(b"key2"), Some(b"value2".to_vec()));

        assert_eq!(store.incr(b"counter").unwrap(), 1);
        assert_eq!(store.incr(b"counter").unwrap(), 2);
        assert_eq!(store.incrby(b"counter", 10).unwrap(), 12);
        assert_eq!(store.decr(b"counter").unwrap(), 11);
    }

    #[test]
    fn test_list_operations() {
        let store = MemoryStore::new();

        assert_eq!(store.rpush(b"list", vec![b"a".to_vec(), b"b".to_vec()]), 2);
        assert_eq!(store.lpush(b"list", vec![b"c".to_vec()]), 3);

        assert_eq!(
            store.lrange(b"list", 0, -1),
            vec![b"c".to_vec(), b"a".to_vec(), b"b".to_vec()]
        );

        assert_eq!(store.lpop(b"list"), Some(b"c".to_vec()));
        assert_eq!(store.rpop(b"list"), Some(b"b".to_vec()));
        assert_eq!(store.llen(b"list"), 1);
    }

    #[test]
    fn test_hash_operations() {
        let store = MemoryStore::new();

        assert!(store.hset(b"hash", b"f1".to_vec(), b"v1".to_vec()));
        assert!(!store.hset(b"hash", b"f1".to_vec(), b"v2".to_vec()));

        assert_eq!(store.hget(b"hash", b"f1"), Some(b"v2".to_vec()));
        assert_eq!(store.hlen(b"hash"), 1);

        store.hmset(
            b"hash",
            vec![
                (b"f2".to_vec(), b"v2".to_vec()),
                (b"f3".to_vec(), b"v3".to_vec()),
            ],
        );
        assert_eq!(store.hlen(b"hash"), 3);
    }

    #[test]
    fn test_set_operations() {
        let store = MemoryStore::new();

        assert_eq!(
            store.sadd(b"set", vec![b"a".to_vec(), b"b".to_vec(), b"a".to_vec()]),
            2
        );
        assert_eq!(store.scard(b"set"), 2);
        assert!(store.sismember(b"set", b"a"));
        assert!(!store.sismember(b"set", b"c"));
    }

    #[test]
    fn test_generic_operations() {
        let store = MemoryStore::new();

        store.set(b"key1".to_vec(), b"value1".to_vec());
        store.set(b"key2".to_vec(), b"value2".to_vec());

        assert_eq!(store.exists(&[b"key1", b"key2", b"key3"]), 2);
        assert_eq!(store.del(&[b"key1"]), 1);
        assert_eq!(store.exists(&[b"key1"]), 0);
        assert_eq!(store.dbsize(), 1);
    }

    #[test]
    fn test_snapshot() {
        let store = MemoryStore::new();

        store.set(b"str".to_vec(), b"value".to_vec());
        store.rpush(b"list", vec![b"a".to_vec(), b"b".to_vec()]);
        store.hset(b"hash", b"f".to_vec(), b"v".to_vec());
        store.sadd(b"set", vec![b"m".to_vec()]);

        let snapshot = store.create_snapshot().unwrap();

        let new_store = MemoryStore::new();
        new_store.restore_from_snapshot(&snapshot).unwrap();

        assert_eq!(new_store.get(b"str"), Some(b"value".to_vec()));
        assert_eq!(
            new_store.lrange(b"list", 0, -1),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(new_store.hget(b"hash", b"f"), Some(b"v".to_vec()));
        assert!(new_store.sismember(b"set", b"m"));
    }

    #[test]
    fn test_split_snapshot() {
        let store = MemoryStore::new();
        let total_slots = 16384u32;

        // Add multiple keys, they will be distributed across different slots
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            store.set(key.into_bytes(), value.into_bytes());
        }

        // Create a snapshot containing only the first half of slots
        let split_snapshot = store
            .create_split_snapshot(0, 8192, total_slots)
            .unwrap();

        // New store restores from split snapshot
        let new_store = MemoryStore::new();
        let merged = new_store.merge_from_snapshot(&split_snapshot).unwrap();

        // Verify merged count is less than original (since it only contains partial slots)
        assert!(merged < 100);
        assert!(merged > 0);

        // Verify keys in new store are within specified slot range
        let all_keys = new_store.keys(b"*");
        for key in &all_keys {
            let slot = slot_for_key(key, total_slots);
            assert!(slot < 8192, "Key {:?} has slot {} which is >= 8192", key, slot);
        }
    }

    #[test]
    fn test_delete_keys_in_slot_range() {
        let store = MemoryStore::new();
        let total_slots = 16384u32;

        // Add multiple keys
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            store.set(key.into_bytes(), value.into_bytes());
        }

        let before = store.dbsize();

        // Delete keys in first half slots
        let deleted = store.delete_keys_in_slot_range(0, 8192, total_slots);

        let after = store.dbsize();

        // Verify delete count
        assert!(deleted > 0);
        assert_eq!(before - deleted, after);

        // Verify remaining keys are not in delete range
        let remaining_keys = store.keys(b"*");
        for key in &remaining_keys {
            let slot = slot_for_key(key, total_slots);
            assert!(slot >= 8192, "Key {:?} has slot {} which is < 8192", key, slot);
        }
    }

    #[test]
    fn test_merge_from_snapshot() {
        let store1 = MemoryStore::new();
        store1.set(b"key1".to_vec(), b"value1".to_vec());
        store1.set(b"key2".to_vec(), b"value2".to_vec());

        let snapshot = store1.create_snapshot().unwrap();

        // Create another store and set some different keys
        let store2 = MemoryStore::new();
        store2.set(b"key3".to_vec(), b"value3".to_vec());

        // Merge snapshot
        let merged = store2.merge_from_snapshot(&snapshot).unwrap();
        assert_eq!(merged, 2);

        // Verify keys from both sides exist
        assert_eq!(store2.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(store2.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(store2.get(b"key3"), Some(b"value3".to_vec()));
        assert_eq!(store2.dbsize(), 3);
    }
}
