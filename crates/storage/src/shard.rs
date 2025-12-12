//! Shard utilities for memory storage
//!
//! Provides slot calculation and shard routing functionality

use crc::{Crc, CRC_16_XMODEM};

/// Total number of slots (consistent with Redis Cluster)
pub const TOTAL_SLOTS: u32 = 16384;

/// Shard ID type
pub type ShardId = String;

/// CRC16 calculator for Redis Cluster (XMODEM variant)
static CRC16: Crc<u16> = Crc::<u16>::new(&CRC_16_XMODEM);

/// Calculate slot for key using CRC16 algorithm (compatible with Redis Cluster)
///
/// Uses CRC16-CCITT (XMODEM) variant, which is what Redis Cluster uses.
pub fn slot_for_key(key: &[u8]) -> u32 {
    CRC16.checksum(key) as u32 % TOTAL_SLOTS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_for_key() {
        // Test that slot calculation is consistent
        let key1 = b"test_key";
        let slot1 = slot_for_key(key1);
        let slot2 = slot_for_key(key1);
        assert_eq!(slot1, slot2, "Slot calculation should be deterministic");

        // Test that slot is within valid range
        assert!(slot1 < TOTAL_SLOTS, "Slot should be less than TOTAL_SLOTS");

        // Test different keys produce different slots (likely, but not guaranteed)
        let key2 = b"different_key";
        let slot3 = slot_for_key(key2);
        // At least verify it's a valid slot
        assert!(slot3 < TOTAL_SLOTS, "Slot should be less than TOTAL_SLOTS");
    }

    #[test]
    fn test_crc16_compatibility() {
        // Test some known CRC16 values to ensure compatibility
        // These are basic sanity checks
        let empty: &[u8] = &[];
        let crc_empty = CRC16.checksum(empty);
        assert_eq!(crc_empty, 0, "CRC16 of empty data should be 0");

        let test_data = b"test";
        let crc1 = CRC16.checksum(test_data);
        let crc2 = CRC16.checksum(test_data);
        assert_eq!(crc1, crc2, "CRC16 should be deterministic");
    }
}
