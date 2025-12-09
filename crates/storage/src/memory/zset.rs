//! Sorted Set (ZSet) data structure implementation
//!
//! Uses HashMap for member->score mapping and BTreeMap for score->member mapping
//! This provides O(1) score lookup and O(log N) range queries

use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// ZSet data structure
/// - dict: member -> score (for O(1) score lookup)
/// - sorted: (score, member) -> () (for range queries)
#[derive(Clone, Debug)]
pub struct ZSetData {
    /// member -> score mapping
    dict: HashMap<Vec<u8>, f64>,
    /// (score, member) pairs sorted by score
    sorted: BTreeMap<(OrderedFloat<f64>, Vec<u8>), ()>,
}

impl ZSetData {
    fn new() -> Self {
        Self {
            dict: HashMap::new(),
            sorted: BTreeMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.dict.len()
    }

    fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }
}

/// ZSet storage implementation
#[derive(Clone)]
pub struct ZSetStore {
    data: Arc<RwLock<HashMap<Vec<u8>, ZSetData>>>,
}

impl ZSetStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// ZADD: Add members with scores
    pub fn zadd(&self, key: Vec<u8>, members: Vec<(f64, Vec<u8>)>) -> usize {
        let mut data = self.data.write();
        let zset = data.entry(key).or_insert_with(|| ZSetData::new());
        
        let mut added = 0;
        for (score, member) in members {
            // Remove old entry if exists
            if let Some(old_score) = zset.dict.remove(&member) {
                zset.sorted.remove(&(OrderedFloat(old_score), member.clone()));
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
        let mut data = self.data.write();
        if let Some(zset) = data.get_mut(key) {
            let mut removed = 0;
            for member in members {
                if let Some(score) = zset.dict.remove(member) {
                    zset.sorted.remove(&(OrderedFloat(score), member.clone()));
                    removed += 1;
                }
            }
            
            if zset.is_empty() {
                data.remove(key);
            }
            
            removed
        } else {
            0
        }
    }

    /// ZSCORE: Get score of member
    pub fn zscore(&self, key: &[u8], member: &[u8]) -> Option<f64> {
        let data = self.data.read();
        data.get(key)
            .and_then(|zset| zset.dict.get(member).copied())
    }

    /// ZCARD: Get cardinality (size)
    pub fn zcard(&self, key: &[u8]) -> usize {
        let data = self.data.read();
        data.get(key).map(|zset| zset.len()).unwrap_or(0)
    }

    /// ZRANK: Get rank of member (0-based, ascending)
    pub fn zrank(&self, key: &[u8], member: &[u8]) -> Option<usize> {
        let data = self.data.read();
        if let Some(zset) = data.get(key) {
            if let Some(&score) = zset.dict.get(member) {
                let score_key = OrderedFloat(score);
                // Count members with score < this score, or score == this score but member < this member
                let rank = zset.sorted
                    .iter()
                    .take_while(|((s, m), _)| *s < score_key || (*s == score_key && m.as_slice() < member))
                    .count();
                Some(rank)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// ZREVRANK: Get reverse rank (0-based, descending)
    pub fn zrevrank(&self, key: &[u8], member: &[u8]) -> Option<usize> {
        if let Some(rank) = self.zrank(key, member) {
            let card = self.zcard(key);
            Some(card - rank - 1)
        } else {
            None
        }
    }

    /// ZRANGE: Get members in range [start, end] (ascending)
    pub fn zrange(&self, key: &[u8], start: i64, end: i64, with_scores: bool) -> Vec<(Vec<u8>, Option<f64>)> {
        let data = self.data.read();
        if let Some(zset) = data.get(key) {
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
            
            zset.sorted
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
                .collect()
        } else {
            Vec::new()
        }
    }

    /// ZREVRANGE: Get members in range [start, end] (descending)
    pub fn zrevrange(&self, key: &[u8], start: i64, end: i64, with_scores: bool) -> Vec<(Vec<u8>, Option<f64>)> {
        let data = self.data.read();
        if let Some(zset) = data.get(key) {
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
            
            // Iterate in reverse
            let all: Vec<_> = zset.sorted.iter().rev().collect();
            all.iter()
                .skip(start_idx)
                .take(end_idx - start_idx + 1)
                .map(|((score, member), _)| {
                    if with_scores {
                        (member.clone(), Some(score.into_inner()))
                    } else {
                        (member.clone(), None)
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// ZRANGEBYSCORE: Get members with score in range [min, max]
    pub fn zrangebyscore(&self, key: &[u8], min: f64, max: f64, with_scores: bool) -> Vec<(Vec<u8>, Option<f64>)> {
        let data = self.data.read();
        if let Some(zset) = data.get(key) {
            let min_key = OrderedFloat(min);
            let max_key = OrderedFloat(max);
            zset.sorted
                .iter()
                .filter(|((score, _), _)| *score >= min_key && *score <= max_key)
                .map(|((score, member), _)| {
                    if with_scores {
                        (member.clone(), Some(score.into_inner()))
                    } else {
                        (member.clone(), None)
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// ZINCRBY: Increment score of member
    pub fn zincrby(&self, key: &[u8], member: Vec<u8>, increment: f64) -> f64 {
        let mut data = self.data.write();
        let zset = data.entry(key.to_vec()).or_insert_with(|| ZSetData::new());
        
        let old_score = zset.dict.get(&member).copied().unwrap_or(0.0);
        let new_score = old_score + increment;
        
        // Remove old entry
        if zset.dict.contains_key(&member) {
            zset.sorted.remove(&(OrderedFloat(old_score), member.clone()));
        }
        
        // Add new entry
        zset.dict.insert(member.clone(), new_score);
        zset.sorted.insert((OrderedFloat(new_score), member), ());
        
        new_score
    }

    /// ZCOUNT: Count members with score in range [min, max]
    pub fn zcount(&self, key: &[u8], min: f64, max: f64) -> usize {
        let data = self.data.read();
        if let Some(zset) = data.get(key) {
            let min_key = OrderedFloat(min);
            let max_key = OrderedFloat(max);
            zset.sorted
                .iter()
                .filter(|((score, _), _)| *score >= min_key && *score <= max_key)
                .count()
        } else {
            0
        }
    }

    /// ZPOPMAX: Pop member with highest score
    pub fn zpopmax(&self, key: &[u8], count: usize) -> Vec<(Vec<u8>, f64)> {
        let mut data = self.data.write();
        if let Some(zset) = data.get_mut(key) {
            let mut popped = Vec::new();
            
            // Get highest score members (from end of sorted map)
            for _ in 0..count {
                if let Some(((score, member), _)) = zset.sorted.iter().rev().next() {
                    let member = member.clone();
                    let score = score.into_inner();
                    
                    zset.dict.remove(&member);
                    zset.sorted.remove(&(OrderedFloat(score), member.clone()));
                    popped.push((member, score));
                } else {
                    break;
                }
            }
            
            if zset.is_empty() {
                data.remove(key);
            }
            
            popped
        } else {
            Vec::new()
        }
    }

    /// ZPOPMIN: Pop member with lowest score
    pub fn zpopmin(&self, key: &[u8], count: usize) -> Vec<(Vec<u8>, f64)> {
        let mut data = self.data.write();
        if let Some(zset) = data.get_mut(key) {
            let mut popped = Vec::new();
            
            // Get lowest score members (from start of sorted map)
            for _ in 0..count {
                if let Some(((score, member), _)) = zset.sorted.iter().next() {
                    let member = member.clone();
                    let score = score.into_inner();
                    
                    zset.dict.remove(&member);
                    zset.sorted.remove(&(OrderedFloat(score), member.clone()));
                    popped.push((member, score));
                } else {
                    break;
                }
            }
            
            if zset.is_empty() {
                data.remove(key);
            }
            
            popped
        } else {
            Vec::new()
        }
    }
}

impl Default for ZSetStore {
    fn default() -> Self {
        Self::new()
    }
}

