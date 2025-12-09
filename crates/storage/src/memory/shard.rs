//! Shard utilities for memory storage
//!
//! Provides slot calculation and shard routing functionality

/// Total number of slots (consistent with Redis Cluster)
pub const TOTAL_SLOTS: u32 = 16384;

/// Shard ID type
pub type ShardId = u32;

/// Calculate slot for key using CRC16 algorithm (compatible with Redis Cluster)
pub fn slot_for_key(key: &[u8]) -> u32 {
    crc16(key) as u32 % TOTAL_SLOTS
}

/// Calculate shard ID from key
/// 
/// # Arguments
/// * `key` - The key to route
/// * `shard_count` - Total number of shards
/// 
/// # Returns
/// Shard ID (0-based)
pub fn shard_for_key(key: &[u8], shard_count: u32) -> ShardId {
    let slot = slot_for_key(key);
    slot / (TOTAL_SLOTS / shard_count)
}

/// Calculate slot range for shard
/// 
/// # Arguments
/// * `shard_id` - Shard ID
/// * `shard_count` - Total number of shards
/// 
/// # Returns
/// (start_slot, end_slot) - Slot range [start, end)
pub fn shard_slot_range(shard_id: ShardId, shard_count: u32) -> (u32, u32) {
    let slots_per_shard = TOTAL_SLOTS / shard_count;
    let start = shard_id * slots_per_shard;
    let end = if shard_id == shard_count - 1 {
        TOTAL_SLOTS
    } else {
        (shard_id + 1) * slots_per_shard
    };
    (start, end)
}

/// CRC16 implementation (compatible with Redis Cluster)
/// 
/// This is a simplified CRC16-CCITT implementation
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
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
    fn test_slot_calculation() {
        let key1 = b"test_key_1";
        let key2 = b"test_key_2";
        
        let slot1 = slot_for_key(key1);
        let slot2 = slot_for_key(key2);
        
        assert!(slot1 < TOTAL_SLOTS);
        assert!(slot2 < TOTAL_SLOTS);
        // Different keys should (likely) have different slots
        // (not guaranteed, but very likely)
    }

    #[test]
    fn test_shard_routing() {
        let key = b"test_key";
        let shard_count = 16;
        
        let shard_id = shard_for_key(key, shard_count);
        assert!(shard_id < shard_count);
        
        let (start, end) = shard_slot_range(shard_id, shard_count);
        let slot = slot_for_key(key);
        assert!(slot >= start && slot < end);
    }

    #[test]
    fn test_shard_slot_range() {
        let shard_count = 4;
        
        // Shard 0: [0, 4096)
        let (start, end) = shard_slot_range(0, shard_count);
        assert_eq!(start, 0);
        assert_eq!(end, 4096);
        
        // Shard 3: [12288, 16384)
        let (start, end) = shard_slot_range(3, shard_count);
        assert_eq!(start, 12288);
        assert_eq!(end, 16384);
    }
}

