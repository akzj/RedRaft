//! Shard utilities for memory storage
//!
//! Provides slot calculation and shard routing functionality

/// Total number of slots (consistent with Redis Cluster)
pub const TOTAL_SLOTS: u32 = 16384;

/// Shard ID type
pub type ShardId = String;

/// Calculate slot for key using CRC16 algorithm (compatible with Redis Cluster)
pub fn slot_for_key(key: &[u8]) -> u32 {
    crc16(key) as u32 % TOTAL_SLOTS
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
