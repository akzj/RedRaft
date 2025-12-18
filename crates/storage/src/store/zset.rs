//! ZSet Store implementation for HybridStore

use crate::memory::DataCow;
use crate::store::HybridStore;
use crate::traits::{StoreError, StoreResult, ZSetStore};
use bytes::Bytes;

impl ZSetStore for HybridStore {
    fn zadd(&self, key: &[u8], members: Vec<(f64, Bytes)>, apply_index: u64) -> StoreResult<usize> {
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        
        // Get or create ZSet
        let zset = match shard_guard.memory_mut().get_mut(key) {
            Some(DataCow::ZSet(zset)) => zset,
            Some(_) => return Err(StoreError::WrongType),
            None => {
                // Create new ZSet
                let new_zset = crate::memory::ZSetDataCow::new();
                shard_guard.memory_mut().insert(key.to_vec(), DataCow::ZSet(new_zset));
                // Get the newly inserted ZSet
                match shard_guard.memory_mut().get_mut(key) {
                    Some(DataCow::ZSet(zset)) => zset,
                    _ => return Err(StoreError::Internal("Failed to create ZSet".to_string())),
                }
            }
        };
        
        let mut count = 0;
        for (score, member) in members {
            let existed = zset.contains(member.as_ref());
            zset.add(member, score);
            if !existed {
                count += 1;
            }
        }
        Ok(count)
    }

    fn zrem(&self, key: &[u8], members: &[&[u8]], apply_index: u64) -> StoreResult<usize> {
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        
        let zset = match shard_guard.memory_mut().get_mut(key) {
            Some(DataCow::ZSet(zset)) => zset,
            Some(_) => return Err(StoreError::WrongType),
            None => return Ok(0),
        };
        
        let mut count = 0;
        for member in members {
            if zset.remove(member) {
                count += 1;
            }
        }
        Ok(count)
    }

    fn zscore(&self, key: &[u8], member: &[u8], read_index: u64) -> StoreResult<Option<f64>> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        
        match shard_guard.memory().get(key) {
            Some(DataCow::ZSet(zset)) => Ok(zset.get_score(member)),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(None),
        }
    }

    fn zrank(&self, key: &[u8], member: &[u8], read_index: u64) -> StoreResult<Option<usize>> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        
        match shard_guard.memory().get(key) {
            Some(DataCow::ZSet(zset)) => Ok(zset.rank(member)),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(None),
        }
    }

    fn zrevrank(&self, key: &[u8], member: &[u8], read_index: u64) -> StoreResult<Option<usize>> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        
        match shard_guard.memory().get(key) {
            Some(DataCow::ZSet(zset)) => Ok(zset.rev_rank(member)),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(None),
        }
    }

    fn zrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        with_scores: bool,
        read_index: u64,
    ) -> StoreResult<Vec<(Bytes, f64)>> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        
        let zset = match shard_guard.memory().get(key) {
            Some(DataCow::ZSet(zset)) => zset,
            Some(_) => return Err(StoreError::WrongType),
            None => return Ok(Vec::new()),
        };
        
        let len = zset.len();
        if len == 0 {
            return Ok(Vec::new());
        }
        
        // Convert negative indices
        let start = if start < 0 {
            (len as i64 + start).max(0) as usize
        } else {
            start as usize
        };
        let stop = if stop < 0 {
            (len as i64 + stop).max(0) as usize
        } else {
            stop as usize
        };
        
        if start >= len || start > stop {
            return Ok(Vec::new());
        }
        
        let end = stop.min(len - 1);
        let result = zset.range_by_rank(start, end);
        
        if with_scores {
            Ok(result)
        } else {
            // Return only members without scores (but trait requires (Bytes, f64))
            Ok(result)
        }
    }

    fn zrevrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        with_scores: bool,
        read_index: u64,
    ) -> StoreResult<Vec<(Bytes, f64)>> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        
        let zset = match shard_guard.memory().get(key) {
            Some(DataCow::ZSet(zset)) => zset,
            Some(_) => return Err(StoreError::WrongType),
            None => return Ok(Vec::new()),
        };
        
        let len = zset.len();
        if len == 0 {
            return Ok(Vec::new());
        }
        
        // Convert negative indices (reverse order)
        let start = if start < 0 {
            (len as i64 + start).max(0) as usize
        } else {
            start as usize
        };
        let stop = if stop < 0 {
            (len as i64 + stop).max(0) as usize
        } else {
            stop as usize
        };
        
        if start >= len || start > stop {
            return Ok(Vec::new());
        }
        
        // Get all members sorted by score descending
        let all = zset.range_by_score(f64::NEG_INFINITY, f64::INFINITY);
        let mut sorted = all;
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        let end = stop.min(len - 1);
        if start > end {
            return Ok(Vec::new());
        }
        
        let result: Vec<(Bytes, f64)> = sorted[start..=end].to_vec();
        
        if with_scores {
            Ok(result)
        } else {
            // Return only members without scores (but trait requires (Bytes, f64))
            Ok(result)
        }
    }

    fn zrangebyscore(
        &self,
        key: &[u8],
        min: f64,
        max: f64,
        with_scores: bool,
        offset: Option<usize>,
        count: Option<usize>,
        read_index: u64,
    ) -> StoreResult<Vec<(Bytes, f64)>> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        
        let zset = match shard_guard.memory().get(key) {
            Some(DataCow::ZSet(zset)) => zset,
            Some(_) => return Err(StoreError::WrongType),
            None => return Ok(Vec::new()),
        };
        
        let mut result = zset.range_by_score(min, max);
        
        // Apply offset and count
        if let Some(offset) = offset {
            if offset >= result.len() {
                return Ok(Vec::new());
            }
            result = result[offset..].to_vec();
        }
        
        if let Some(count) = count {
            if count < result.len() {
                result.truncate(count);
            }
        }
        
        if with_scores {
            Ok(result)
        } else {
            // Return only members without scores (but trait requires (Bytes, f64))
            Ok(result)
        }
    }

    fn zcard(&self, key: &[u8], read_index: u64) -> StoreResult<usize> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        
        match shard_guard.memory().get(key) {
            Some(DataCow::ZSet(zset)) => Ok(zset.len()),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(0),
        }
    }

    fn zcount(&self, key: &[u8], min: f64, max: f64, read_index: u64) -> StoreResult<usize> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        
        let zset = match shard_guard.memory().get(key) {
            Some(DataCow::ZSet(zset)) => zset,
            Some(_) => return Err(StoreError::WrongType),
            None => return Ok(0),
        };
        
        let result = zset.range_by_score(min, max);
        Ok(result.len())
    }

    fn zincrby(&self, key: &[u8], delta: f64, member: &[u8], apply_index: u64) -> StoreResult<f64> {
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        
        // Get or create ZSet
        let zset = match shard_guard.memory_mut().get_mut(key) {
            Some(DataCow::ZSet(zset)) => zset,
            Some(_) => return Err(StoreError::WrongType),
            None => {
                // Create new ZSet
                let new_zset = crate::memory::ZSetDataCow::new();
                shard_guard.memory_mut().insert(key.to_vec(), DataCow::ZSet(new_zset));
                // Get the newly inserted ZSet
                match shard_guard.memory_mut().get_mut(key) {
                    Some(DataCow::ZSet(zset)) => zset,
                    _ => return Err(StoreError::Internal("Failed to create ZSet".to_string())),
                }
            }
        };
        
        let old_score = zset.get_score(member).unwrap_or(0.0);
        let new_score = old_score + delta;
        zset.add(Bytes::copy_from_slice(member), new_score);
        Ok(new_score)
    }

    fn zinterstore(
        &self,
        destination: &[u8],
        keys: &[&[u8]],
        weights: Option<&[f64]>,
        apply_index: u64,
    ) -> StoreResult<usize> {
        // Default implementation: not supported
        let _ = (destination, keys, weights, apply_index);
        Ok(0)
    }

    fn zunionstore(
        &self,
        destination: &[u8],
        keys: &[&[u8]],
        weights: Option<&[f64]>,
        apply_index: u64,
    ) -> StoreResult<usize> {
        // Default implementation: not supported
        let _ = (destination, keys, weights, apply_index);
        Ok(0)
    }
}

